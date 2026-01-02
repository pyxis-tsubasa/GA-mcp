import express from "express";
import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { BetaAnalyticsDataClient } from "@google-analytics/data";

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "1mb" }));

// ===== 必須環境変数 =====
const GA4_PROPERTY_ID = process.env.GA4_PROPERTY_ID;
if (!GA4_PROPERTY_ID) {
  console.error("Missing env: GA4_PROPERTY_ID");
  process.exit(1);
}

// ===== 認証 =====
// 推奨：URLにトークンを埋める方式 → /mcp/<token>
const MCP_PATH_TOKEN = process.env.MCP_PATH_TOKEN;
// 互換用：curl等で x-api-key ヘッダー方式も残す
const MCP_API_KEY = process.env.MCP_API_KEY;

function isAuthorized(req) {
  const tokenInPath = req.params?.token;
  if (MCP_PATH_TOKEN && tokenInPath === MCP_PATH_TOKEN) return true;

  const headerKey = req.header("x-api-key");
  if (MCP_API_KEY && headerKey && headerKey === MCP_API_KEY) return true;

  // どちらかが設定されているのに一致しないなら拒否
  if (MCP_PATH_TOKEN || MCP_API_KEY) return false;

  // 両方未設定なら「無認証」扱い（本番では非推奨）
  return true;
}

// ===== GAクライアント（Cloud RunならADCでOK） =====
const gaClient = new BetaAnalyticsDataClient();

function toNumber(v) {
  if (v == null) return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function yyyymmddToIso(s) {
  // "20260102" -> "2026-01-02"
  if (!s || s.length !== 8) return s;
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
}

async function runReport({
  dimensions,
  metrics,
  startDate,
  endDate,
  limit,
  orderByMetric,
  desc = true,
}) {
  const [resp] = await gaClient.runReport({
    property: `properties/${GA4_PROPERTY_ID}`,
    dateRanges: [{ startDate, endDate }],
    dimensions: dimensions.map((name) => ({ name })),
    metrics: metrics.map((name) => ({ name })),
    limit: limit ? String(limit) : undefined,
    orderBys: orderByMetric
      ? [
          {
            metric: { metricName: orderByMetric },
            desc,
          },
        ]
      : undefined,
  });

  const rows = resp.rows ?? [];
  return rows.map((r) => {
    const dim = r.dimensionValues?.map((d) => d.value ?? "") ?? [];
    const met = r.metricValues?.map((m) => toNumber(m.value)) ?? [];
    return { dim, met };
  });
}

// ===== MCPサーバー（ツール定義） =====
const mcp = new McpServer({ name: "ga-mcp", version: "1.0.0" });

// 1) チャネル別サマリー
mcp.tool(
  "ga_channel_summary",
  "GA4の集客チャネル別（Default Channel Group）サマリーを返す",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
  },
  async ({ startDate, endDate }) => {
    const rows = await runReport({
      dimensions: ["sessionDefaultChannelGroup"],
      metrics: ["sessions", "activeUsers"],
      startDate,
      endDate,
      orderByMetric: "sessions",
      desc: true,
      limit: 100,
    });

    const outRows = rows.map((r) => ({
      channel: r.dim[0] || "(not set)",
      sessions: r.met[0] ?? 0,
      activeUsers: r.met[1] ?? 0,
    }));

    const totals = outRows.reduce(
      (acc, x) => {
        acc.sessions += x.sessions;
        acc.activeUsers += x.activeUsers;
        return acc;
      },
      { sessions: 0, activeUsers: 0 }
    );

    const result = {
      report: "channel_summary",
      propertyId: GA4_PROPERTY_ID,
      dateRange: { startDate, endDate },
      totals,
      rows: outRows,
    };

    return {
      content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
    };
  }
);

// 2) LP別ランキング TOP20
mcp.tool(
  "ga_landing_page_ranking",
  "GA4のLP別（landingPagePlusQueryString）ランキングTOPを返す",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    limit: z.number().min(1).max(200).default(20),
  },
  async ({ startDate, endDate, limit }) => {
    const rows = await runReport({
      dimensions: ["landingPagePlusQueryString"],
      metrics: ["sessions", "activeUsers"],
      startDate,
      endDate,
      orderByMetric: "sessions",
      desc: true,
      limit,
    });

    const outRows = rows.map((r) => ({
      landingPage: r.dim[0] || "(not set)",
      sessions: r.met[0] ?? 0,
      activeUsers: r.met[1] ?? 0,
    }));

    const result = {
      report: "landing_page_ranking",
      propertyId: GA4_PROPERTY_ID,
      dateRange: { startDate, endDate },
      limit,
      rows: outRows,
    };

    return {
      content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
    };
  }
);

// 3) 日次推移（直近30日など）
mcp.tool(
  "ga_daily_trend",
  "GA4の日次推移（date）を返す。週次レポ用に便利",
  {
    startDate: z.string().default("30daysAgo"),
    endDate: z.string().default("yesterday"),
  },
  async ({ startDate, endDate }) => {
    const rows = await runReport({
      dimensions: ["date"],
      metrics: ["sessions", "activeUsers"],
      startDate,
      endDate,
      limit: 1000,
    });

    const outRows = rows
      .map((r) => ({
        date: yyyymmddToIso(r.dim[0]),
        sessions: r.met[0] ?? 0,
        activeUsers: r.met[1] ?? 0,
      }))
      .sort((a, b) => (a.date > b.date ? 1 : -1));

    const result = {
      report: "daily_trend",
      propertyId: GA4_PROPERTY_ID,
      dateRange: { startDate, endDate },
      rows: outRows,
    };

    return {
      content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
    };
  }
);

// ===== Streamable HTTP transport（ステートレス） =====
const transport = new StreamableHTTPServerTransport({
  sessionIdGenerator: undefined,
});

async function main() {
  await mcp.connect(transport);

  app.get("/health", (_req, res) => res.status(200).send("ok"));

  // MCP endpoint（/mcp でも /mcp/<token> でもOK）
  app.post("/mcp/:token?", async (req, res) => {
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

  // 互換用：GET/DELETE は 405
  app.get("/mcp/:token?", (_req, res) => res.status(405).send("Method not allowed"));
  app.delete("/mcp/:token?", (_req, res) => res.status(405).send("Method not allowed"));

  const port = Number(process.env.PORT || 8080);
  app.listen(port, () => console.log(`MCP server listening on :${port}`));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
