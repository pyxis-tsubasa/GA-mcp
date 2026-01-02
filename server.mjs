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
    } catch (e) {
      return jsonText({ report: "channel_summary_plus", error: formatErr(e) });
    }
  }
);

// 3) チャネル別：前期間比較
mcp.tool(
  "ga_channel_summary_compare",
  "チャネル別を前期間比較（増減・増減率）",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    keyEventName: z.string().optional(),
    limit: z.number().min(1).max(200).default(50),
  },
  async ({ startDate, endDate, keyEventName, limit }) => {
    try {
      const prevRange = computePreviousPeriod(startDate, endDate);
      if (!prevRange) {
        return jsonText({
          report: "channel_summary_compare",
          error: "前期間を自動推定できません。startDateを '7daysAgo' の形にして下さい。",
        });
      }

      const metrics = baseMetricsPlus(keyEventName);

      const curr = await runReportKV({
        dimensions: ["sessionDefaultChannelGroup"],
        metrics,
        startDate,
        endDate,
        limit: 200,
      });

      const prev = await runReportKV({
        dimensions: ["sessionDefaultChannelGroup"],
        metrics,
        startDate: prevRange.startDate,
        endDate: prevRange.endDate,
        limit: 200,
      });

      const prevMap = new Map(
        prev.rows.map((r) => [r.sessionDefaultChannelGroup || "(not set)", r])
      );

      const merged = curr.rows.map((r) => {
        const ch = r.sessionDefaultChannelGroup || "(not set)";
        const p = prevMap.get(ch) || {};
        const cs = r.sessions || 0;
        const ps = p.sessions || 0;

        return {
          channel: ch,
          current: {
            sessions: cs,
            activeUsers: r.activeUsers || 0,
            keyEvents: r.keyEvents || 0,
            sessionKeyEventRate: r.sessionKeyEventRate ?? null,
          },
          previous: {
            sessions: ps,
            activeUsers: p.activeUsers || 0,
            keyEvents: p.keyEvents || 0,
            sessionKeyEventRate: p.sessionKeyEventRate ?? null,
          },
          delta: {
            sessions: cs - ps,
            sessionsPct: safePctChange(cs, ps),
          },
        };
      });

      merged.sort((a, b) => (b.current.sessions || 0) - (a.current.sessions || 0));

      return jsonText({
        report: "channel_summary_compare",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        compareRange: prevRange,
        rows: merged.slice(0, limit),
      });
    } catch (e) {
      return jsonText({ report: "channel_summary_compare", error: formatErr(e) });
    }
  }
);

// 4) LP別（CV含む）
mcp.tool(
  "ga_landing_page_performance",
  "LP別ランキング（CV含む）",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    keyEventName: z.string().optional(),
    limit: z.number().min(1).max(200).default(20),
  },
  async ({ startDate, endDate, keyEventName, limit }) => {
    try {
      const metrics = baseMetricsPlus(keyEventName);
      const { rows } = await runReportKV({
        dimensions: ["landingPagePlusQueryString"],
        metrics,
        startDate,
        endDate,
        orderByMetric: "sessions",
        desc: true,
        limit,
      });

      return jsonText({
        report: "landing_page_performance",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        rows,
      });
    } catch (e) {
      return jsonText({ report: "landing_page_performance", error: formatErr(e) });
    }
  }
);

// 5) 日次推移
mcp.tool(
  "ga_daily_trend",
  "日次推移（sessions/activeUsers/keyEvents）",
  {
    startDate: z.string().default("30daysAgo"),
    endDate: z.string().default("yesterday"),
  },
  async ({ startDate, endDate }) => {
    try {
      const { rows } = await runReportKV({
        dimensions: ["date"],
        metrics: ["sessions", "activeUsers", "keyEvents"],
        startDate,
        endDate,
        limit: 2000,
      });

      const out = rows
        .map((r) => ({
          date: yyyymmddToIso(r.date),
          sessions: r.sessions || 0,
          activeUsers: r.activeUsers || 0,
          keyEvents: r.keyEvents || 0,
        }))
        .sort((a, b) => (a.date > b.date ? 1 : -1));

      return jsonText({
        report: "daily_trend",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        rows: out,
      });
    } catch (e) {
      return jsonText({ report: "daily_trend", error: formatErr(e) });
    }
  }
);

// 6) 異常検知
mcp.tool(
  "ga_daily_anomalies",
  "日次推移の異常検知（移動平均との差）",
  {
    startDate: z.string().default("60daysAgo"),
    endDate: z.string().default("yesterday"),
    metric: z.enum(["sessions", "activeUsers", "keyEvents"]).default("sessions"),
    windowDays: z.number().min(3).max(30).default(7),
    zThreshold: z.number().min(1).max(10).default(2.5),
  },
  async ({ startDate, endDate, metric, windowDays, zThreshold }) => {
    try {
      const { rows } = await runReportKV({
        dimensions: ["date"],
        metrics: ["sessions", "activeUsers", "keyEvents"],
        startDate,
        endDate,
        limit: 2000,
      });

      const series = rows
        .map((r) => ({ date: yyyymmddToIso(r.date), value: r[metric] || 0 }))
        .sort((a, b) => (a.date > b.date ? 1 : -1));

      const mean = (arr) => (arr.length ? arr.reduce((s, x) => s + x, 0) / arr.length : 0);
      const std = (arr, mu) => {
        if (arr.length < 2) return 0;
        const v = arr.reduce((s, x) => s + (x - mu) ** 2, 0) / (arr.length - 1);
        return Math.sqrt(v);
      };

      const anomalies = [];
      for (let i = windowDays; i < series.length; i++) {
        const window = series.slice(i - windowDays, i).map((x) => x.value);
        const mu = mean(window);
        const sd = std(window, mu);
        const v = series[i].value;
        const zScore = sd === 0 ? null : (v - mu) / sd;
        if (zScore != null && Math.abs(zScore) >= zThreshold) {
          anomalies.push({
            date: series[i].date,
            metric,
            value: v,
            baselineMean: mu,
            baselineStd: sd,
            zScore,
            direction: zScore > 0 ? "spike" : "drop",
          });
        }
      }

      return jsonText({
        report: "daily_anomalies",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        metric,
        windowDays,
        zThreshold,
        anomalies,
      });
    } catch (e) {
      return jsonText({ report: "daily_anomalies", error: formatErr(e) });
    }
  }
);

// 7) キャンペーン
mcp.tool(
  "ga_campaign_performance",
  "キャンペーン（source/medium/campaign）別",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    limit: z.number().min(1).max(200).default(50),
  },
  async ({ startDate, endDate, limit }) => {
    try {
      const { rows } = await runReportKV({
        dimensions: ["sessionSourceMedium", "sessionCampaignName"],
        metrics: ["sessions", "activeUsers", "keyEvents", "sessionKeyEventRate"],
        startDate,
        endDate,
        orderByMetric: "sessions",
        desc: true,
        limit,
      });

      return jsonText({
        report: "campaign_performance",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        rows,
      });
    } catch (e) {
      return jsonText({ report: "campaign_performance", error: formatErr(e) });
    }
  }
);

// 8) デバイス
mcp.tool(
  "ga_device_breakdown",
  "デバイス別（deviceCategory）",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
  },
  async ({ startDate, endDate }) => {
    try {
      const { rows } = await runReportKV({
        dimensions: ["deviceCategory"],
        metrics: ["sessions", "activeUsers", "keyEvents", "sessionKeyEventRate"],
        startDate,
        endDate,
        orderByMetric: "sessions",
        desc: true,
        limit: 50,
      });

      return jsonText({
        report: "device_breakdown",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        rows,
      });
    } catch (e) {
      return jsonText({ report: "device_breakdown", error: formatErr(e) });
    }
  }
);

// 9) 国別
mcp.tool(
  "ga_country_breakdown",
  "国別（country）",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    limit: z.number().min(1).max(200).default(50),
  },
  async ({ startDate, endDate, limit }) => {
    try {
      const { rows } = await runReportKV({
        dimensions: ["country"],
        metrics: ["sessions", "activeUsers", "keyEvents", "sessionKeyEventRate"],
        startDate,
        endDate,
        orderByMetric: "sessions",
        desc: true,
        limit,
      });

      return jsonText({
        report: "country_breakdown",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        rows,
      });
    } catch (e) {
      return jsonText({ report: "country_breakdown", error: formatErr(e) });
    }
  }
);

// 10) 新規/リピーター
mcp.tool(
  "ga_new_vs_returning",
  "新規/リピーター（newVsReturning）",
  {
    startDate: z.string().default("30daysAgo"),
    endDate: z.string().default("yesterday"),
  },
  async ({ startDate, endDate }) => {
    try {
      const { rows } = await runReportKV({
        dimensions: ["newVsReturning"],
        metrics: ["sessions", "activeUsers", "keyEvents", "sessionKeyEventRate"],
        startDate,
        endDate,
        limit: 10,
      });

      return jsonText({
        report: "new_vs_returning",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        rows,
      });
    } catch (e) {
      return jsonText({ report: "new_vs_returning", error: formatErr(e) });
    }
  }
);

// 11) 人気ページ
mcp.tool(
  "ga_top_pages",
  "人気ページ（pagePathPlusQueryString）",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    limit: z.number().min(1).max(200).default(20),
  },
  async ({ startDate, endDate, limit }) => {
    try {
      const { rows } = await runReportKV({
        dimensions: ["pagePathPlusQueryString"],
        metrics: ["screenPageViews", "activeUsers", "userEngagementDuration"],
        startDate,
        endDate,
        orderByMetric: "screenPageViews",
        desc: true,
        limit,
      });

      return jsonText({
        report: "top_pages",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        rows,
      });
    } catch (e) {
      return jsonText({ report: "top_pages", error: formatErr(e) });
    }
  }
);

// 12) イベントTOP
mcp.tool(
  "ga_top_events",
  "イベントTOP（eventName）",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    limit: z.number().min(1).max(200).default(30),
  },
  async ({ startDate, endDate, limit }) => {
    try {
      const { rows } = await runReportKV({
        dimensions: ["eventName"],
        metrics: ["eventCount", "keyEvents"],
        startDate,
        endDate,
        orderByMetric: "eventCount",
        desc: true,
        limit,
      });

      return jsonText({
        report: "top_events",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        rows,
      });
    } catch (e) {
      return jsonText({ report: "top_events", error: formatErr(e) });
    }
  }
);

// 13) ファネル（v1alpha REST）
mcp.tool(
  "ga_funnel_basic",
  "ファネル（v1alpha runFunnelReport）",
  {
    startDate: z.string().default("30daysAgo"),
    endDate: z.string().default("yesterday"),
    isOpenFunnel: z.boolean().default(false),
    steps: z
      .array(
        z.object({
          name: z.string().min(1),
          eventName: z.string().min(1),
          pageLocationContains: z.string().optional(),
          isDirectlyFollowedBy: z.boolean().optional(),
          withinDurationFromPriorStep: z.string().optional(),
        })
      )
      .min(2)
      .max(10),
  },
  async ({ startDate, endDate, isOpenFunnel, steps }) => {
    try {
      const funnelSteps = steps.map((s) => {
        const filterExpression = {
          funnelEventFilter: {
            eventName: s.eventName,
          },
        };

        if (s.pageLocationContains) {
          filterExpression.funnelEventFilter.funnelParameterFilterExpression = {
            funnelParameterFilter: {
              eventParameterName: "page_location",
              stringFilter: {
                matchType: "CONTAINS",
                value: s.pageLocationContains,
                caseSensitive: false,
              },
            },
          };
        }

        return {
          name: s.name,
          isDirectlyFollowedBy: s.isDirectlyFollowedBy ?? false,
          withinDurationFromPriorStep: s.withinDurationFromPriorStep,
          filterExpression,
        };
      });

      const body = {
        dateRanges: [{ startDate, endDate }],
        funnel: {
          isOpenFunnel,
          steps: funnelSteps,
        },
      };

      const resp = await runFunnelReportViaRest(body);

      return jsonText({
        report: "funnel_basic",
        propertyId: GA4_PROPERTY_ID,
        dateRange: { startDate, endDate },
        request: { isOpenFunnel, steps },
        response: resp,
        note: "ファネルは v1alpha のため将来互換が変わる可能性があります。",
      });
    } catch (e) {
      return jsonText({ report: "funnel_basic", error: formatErr(e) });
    }
  }
);

// ===== Streamable HTTP transport（ステートレス） =====
const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });

async function main() {
  await mcp.connect(transport);

  app.get("/health", (_req, res) => {
    const errs = configErrors();
    if (errs.length) return res.status(500).send(errs.join(" | "));
    return res.status(200).send("ok");
  });

  app.post("/mcp/:token?", async (req, res) => {
    const errs = configErrors();
    if (errs.length) return res.status(500).json({ error: errs.join(" | ") });

    if (!isAuthorized(req)) return res.status(401).json({ error: "unauthorized" });

    try {
      await transport.handleRequest(req, res, req.body);
    } catch (err) {
      console.error(err);
      if (!res.headersSent) {
        res.status(500).json({
          jsonrpc: "2.0",
          error: { code: -32603, message: "Internal error" },
          id: null,
        });
      }
    }
  });

  app.get("/mcp/:token?", (_req, res) => res.status(405).send("Method not allowed"));
  app.delete("/mcp/:token?", (_req, res) => res.status(405).send("Method not allowed"));

  const port = Number(process.env.PORT || 8080);
  app.listen(port, () => console.log(`MCP server listening on :${port}`));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
