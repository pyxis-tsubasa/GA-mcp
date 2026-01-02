import express from "express";
import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { BetaAnalyticsDataClient } from "@google-analytics/data";

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "1mb" }));

// ===== 環境変数 =====
const GA4_PROPERTY_ID = process.env.GA4_PROPERTY_ID; // 例: "423169216"
const MCP_PATH_TOKEN = process.env.MCP_PATH_TOKEN;   // Secret値（推奨）
const MCP_API_KEY = process.env.MCP_API_KEY;         // 任意（curl用）

function configErrors() {
  const errs = [];
  if (!GA4_PROPERTY_ID) errs.push("Missing env: GA4_PROPERTY_ID");
  if (!MCP_PATH_TOKEN && !MCP_API_KEY) errs.push("Missing auth env: set MCP_PATH_TOKEN or MCP_API_KEY");
  return errs;
}

function isAuthorized(req) {
  // 認証が未設定なら常に拒否（/healthで原因を見せる）
  if (!MCP_PATH_TOKEN && !MCP_API_KEY) return false;

  const tokenInPath = req.params?.token;
  if (MCP_PATH_TOKEN && tokenInPath === MCP_PATH_TOKEN) return true;

  const headerKey = req.header("x-api-key");
  if (MCP_API_KEY && headerKey && headerKey === MCP_API_KEY) return true;

  return false;
}

// ===== GAクライアント =====
const gaClient = new BetaAnalyticsDataClient();

function toNumber(v) {
  if (v == null) return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function yyyymmddToIso(s) {
  if (!s || s.length !== 8) return s;
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
}

function safePctChange(curr, prev) {
  if (!prev) return null;
  return (curr - prev) / prev;
}

// "7daysAgo" + "yesterday" 形式に強い前期間推定
function computePreviousPeriod(startDate, endDate) {
  const m = String(startDate).match(/^(\d+)daysAgo$/);
  if (!m) return null;
  const n = Number(m[1]);

  if (endDate === "yesterday") {
    return { startDate: `${2 * n}daysAgo`, endDate: `${n + 1}daysAgo` };
  }
  if (endDate === "today") {
    return { startDate: `${2 * n - 1}daysAgo`, endDate: `${n}daysAgo` };
  }
  return null;
}

function jsonText(obj) {
  return { content: [{ type: "text", text: JSON.stringify(obj, null, 2) }] };
}

function formatErr(e) {
  const msg = e?.message || String(e);
  return msg.length > 800 ? msg.slice(0, 800) + "..." : msg;
}

async function runReportKV({
  dimensions = [],
  metrics = [],
  startDate,
  endDate,
  limit,
  orderByMetric,
  desc = true,
  dimensionFilter,
}) {
  const [resp] = await gaClient.runReport({
    property: `properties/${GA4_PROPERTY_ID}`,
    dateRanges: [{ startDate, endDate }],
    dimensions: dimensions.map((name) => ({ name })),
    metrics: metrics.map((name) => ({ name })),
    limit: limit != null ? String(limit) : undefined,
    orderBys: orderByMetric
      ? [{ metric: { metricName: orderByMetric }, desc }]
      : undefined,
    dimensionFilter: dimensionFilter || undefined,
  });

  const dimHeaders = (resp.dimensionHeaders ?? []).map((h) => h.name || "");
  const metHeaders = (resp.metricHeaders ?? []).map((h) => h.name || "");

  const rows = (resp.rows ?? []).map((r) => {
    const out = {};
    dimHeaders.forEach((name, i) => (out[name] = r.dimensionValues?.[i]?.value ?? ""));
    metHeaders.forEach((name, i) => (out[name] = toNumber(r.metricValues?.[i]?.value)));
    return out;
  });

  return { rows, dimHeaders, metHeaders };
}

// ===== Funnel（v1alpha）は REST で叩く：alpha client 依存で起動が落ちるのを避ける =====
async function runFunnelReportViaRest(body) {
  const url = `https://analyticsdata.googleapis.com/v1alpha/properties/${GA4_PROPERTY_ID}:runFunnelReport`;

  // gaClient.auth は GoogleAuth。ここから署名済みヘッダーを作る
  const authClient = await gaClient.auth.getClient();
  const headers = await authClient.getRequestHeaders(url);

  const resp = await fetch(url, {
    method: "POST",
    headers: {
      ...headers,
      "content-type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Funnel API error ${resp.status}: ${text}`);
  }
  return JSON.parse(text);
}

// ===== MCPサーバー =====
const mcp = new McpServer({ name: "ga-mcp", version: "2.1.0" });

// 0) Metadata検索（使えるディメンション/メトリクス）
mcp.tool(
  "ga_metadata_search",
  "GA4 Data APIで使えるディメンション/メトリクスを検索",
  {
    kind: z.enum(["all", "dimension", "metric"]).default("all"),
    query: z.string().default(""),
    limit: z.number().min(1).max(200).default(50),
  },
  async ({ kind, query, limit }) => {
    try {
      const [meta] = await gaClient.getMetadata({
        name: `properties/${GA4_PROPERTY_ID}/metadata`,
      });

      const q = String(query || "").toLowerCase().trim();

      const dims = (meta.dimensions ?? []).map((d) => ({
        apiName: d.apiName,
        uiName: d.uiName,
        description: d.description,
      }));
      const mets = (meta.metrics ?? []).map((m) => ({
        apiName: m.apiName,
        uiName: m.uiName,
        description: m.description,
      }));

      const match = (x) =>
        !q ||
        String(x.apiName || "").toLowerCase().includes(q) ||
        String(x.uiName || "").toLowerCase().includes(q) ||
        String(x.description || "").toLowerCase().includes(q);

      return jsonText({
        report: "metadata_search",
        propertyId: GA4_PROPERTY_ID,
        kind,
        query,
        dimensions: kind === "metric" ? [] : dims.filter(match).slice(0, limit),
        metrics: kind === "dimension" ? [] : mets.filter(match).slice(0, limit),
      });
    } catch (e) {
      return jsonText({ report: "metadata_search", error: formatErr(e) });
    }
  }
);

// 共通メトリクス（CVっぽい）
function baseMetricsPlus(keyEventName) {
  const m = ["sessions", "activeUsers", "keyEvents", "sessionKeyEventRate"];
  if (keyEventName) m.push(`sessionKeyEventRate:${keyEventName}`);
  return m;
}

// 1) KPIサマリー
mcp.tool(
  "ga_kpi_overview",
  "KPIサマリー（sessions/users/views/keyEvents/CVR等）",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    keyEventName: z.string().optional(),
  },
  async ({ startDate, endDate, keyEventName }) => {
    try {
      const metrics = [
        "sessions",
        "activeUsers",
        "newUsers",
        "screenPageViews",
        "keyEvents",
        "sessionKeyEventRate",
        "userKeyEventRate",
      ];
      if (keyEventName) {
        metrics.push(`sessionKeyEventRate:${keyEventName}`);
        metrics.push(`userKeyEventRate:${keyEventName}`);
      }

      const { rows } = await runReportKV({
        dimensions: [],
        metrics,
        startDate,
        endDate,
        limit: 1,
      });

      return jsonText({
        report: "kpi_overview",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        keyEventName: keyEventName || null,
        kpis: rows[0] || {},
      });
    } catch (e) {
      return jsonText({ report: "kpi_overview", error: formatErr(e) });
    }
  }
);

// 2) チャネル別（CV含む）
mcp.tool(
  "ga_channel_summary_plus",
  "集客チャネル別サマリー（CV含む）",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    keyEventName: z.string().optional(),
    limit: z.number().min(1).max(200).default(50),
  },
  async ({ startDate, endDate, keyEventName, limit }) => {
    try {
      const { rows } = await runReportKV({
        dimensions: ["sessionDefaultChannelGroup"],
        metrics: baseMetricsPlus(keyEventName),
        startDate,
        endDate,
        orderByMetric: "sessions",
        desc: true,
        limit,
      });

      return jsonText({
        report: "channel_summary_plus",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        keyEventName: keyEventName || null,
        rows: rows.map((r) => ({
          channel: r.sessionDefaultChannelGroup || "(not set)",
          sessions: r.sessions || 0,
          activeUsers: r.activeUsers || 0,
          keyEvents: r.keyEvents || 0,
          sessionKeyEventRate: r.sessionKeyEventRate ?? null,
          ...(keyEventName
            ? { [`sessionKeyEventRate:${keyEventName}`]: r[`sessionKeyEventRate:${keyEventName}`] ?? null }
            : {}),
        })),
      });
    } ca
