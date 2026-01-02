const express = require("express");
const { BetaAnalyticsDataClient } = require("@google-analytics/data");

/**
 * ---- 環境変数（Cloud Runで設定）----
 * GA4_PROPERTY_ID: 例 "423169216"
 * MCP_API_KEY: APIキー（Secretから参照推奨）
 *
 * 互換のため、あなたが現在入れている可能性がある
 * mcp_api_key も読めるようにしてあります（移行後は削除推奨）
 */
const GA4_PROPERTY_ID =
  process.env.GA4_PROPERTY_ID ||
  process.env.GA_PROPERTY_ID ||
  process.env.GA4_PROPERTY ||
  "";

const MCP_API_KEY =
  process.env.MCP_API_KEY ||
  process.env.mcp_api_key || // 互換（いまの設定を動かす用）
  process.env.MCP_KEY ||
  "";

/**
 * GA4 Data API client
 * Cloud Run では「このサービスが動くサービスアカウント」の権限で呼ばれます。
 */
const gaClient = new BetaAnalyticsDataClient();

const app = express();
app.use(express.json({ limit: "1mb" }));

// ヘルスチェック（認証なし）
app.get("/health", (_, res) => res.status(200).send("ok"));

// APIキーでロック（/health以外）
app.use((req, res, next) => {
  if (req.path === "/health") return next();

  if (!MCP_API_KEY) {
    return res.status(500).json({
      error: "MCP_API_KEY が未設定です（Cloud Runで設定してください）",
    });
  }

  const key = req.header("x-api-key") || req.query.key;
  if (key !== MCP_API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  next();
});

function assertEnv() {
  if (!GA4_PROPERTY_ID) {
    const err = new Error(
      "GA4_PROPERTY_ID が未設定です（Cloud Runで設定してください）"
    );
    err.code = "MISSING_GA4_PROPERTY_ID";
    throw err;
  }
}

function yyyymmddToIso(yyyymmdd) {
  if (!yyyymmdd || yyyymmdd.length !== 8) return yyyymmdd;
  const y = yyyymmdd.slice(0, 4);
  const m = yyyymmdd.slice(4, 6);
  const d = yyyymmdd.slice(6, 8);
  return `${y}-${m}-${d}`;
}

async function runReport({
  dimensions,
  metrics,
  startDate,
  endDate,
  limit,
  orderByMetric,
  orderDesc,
}) {
  assertEnv();

  const request = {
    property: `properties/${GA4_PROPERTY_ID}`,
    dateRanges: [{ startDate, endDate }],
    dimensions: dimensions.map((name) => ({ name })),
    metrics: metrics.map((name) => ({ name })),
    limit: String(limit ?? 20),
  };

  if (orderByMetric) {
    request.orderBys = [
      {
        metric: { metricName: orderByMetric },
        desc: !!orderDesc,
      },
    ];
  }

  const [response] = await gaClient.runReport(request);
  return response;
}

// ---- Tools 実装 ----

async function tool_channel_summary(args = {}) {
  // チャネル別サマリー（Traffic acquisitionの “Session primary channel group” 相当） :contentReference[oaicite:4]{index=4}
  const startDate = args.startDate || "7daysAgo";
  const endDate = args.endDate || "yesterday";
  const limit = args.limit ?? 50;

  const dimension = "sessionPrimaryChannelGroup"; // 代表的な“集客チャネル” :contentReference[oaicite:5]{index=5}
  const metrics = ["sessions", "activeUsers"];

  const res = await runReport({
    dimensions: [dimension],
    metrics,
    startDate,
    endDate,
    limit,
    orderByMetric: "sessions",
    orderDesc: true,
  });

  const rows = (res.rows || []).map((r) => ({
    channel: r.dimensionValues?.[0]?.value || "(not set)",
    sessions: Number(r.metricValues?.[0]?.value || 0),
    activeUsers: Number(r.metricValues?.[1]?.value || 0),
  }));

  const totals = rows.reduce(
    (acc, cur) => {
      acc.sessions += cur.sessions;
      acc.activeUsers += cur.activeUsers;
      return acc;
    },
    { sessions: 0, activeUsers: 0 }
  );

  return {
    report: "channel_summary",
    propertyId: GA4_PROPERTY_ID,
    dateRange: { startDate, endDate },
    totals,
    rows,
  };
}

async function tool_landing_pages_top(args = {}) {
  // LP別ランキング：landingPagePlusQueryString を使用 :contentReference[oaicite:6]{index=6}
  const startDate = args.startDate || "30daysAgo";
  const endDate = args.endDate || "yesterday";
  const limit = args.limit ?? 20;

  const dimension = "landingPagePlusQueryString";
  const metrics = ["sessions", "activeUsers"];

  const res = await runReport({
    dimensions: [dimension],
    metrics,
    startDate,
    endDate,
    limit,
    orderByMetric: "sessions",
    orderDesc: true,
  });

  const rows = (res.rows || []).map((r) => ({
    landingPage: r.dimensionValues?.[0]?.value || "(not set)",
    sessions: Number(r.metricValues?.[0]?.value || 0),
    activeUsers: Number(r.metricValues?.[1]?.value || 0),
  }));

  return {
    report: "landing_pages_top",
    propertyId: GA4_PROPERTY_ID,
    dateRange: { startDate, endDate },
    rows,
  };
}

async function tool_daily_trend(args = {}) {
  // 日次推移：date は YYYYMMDD 形式 :contentReference[oaicite:7]{index=7}
  const days = args.days ?? 30;
  const endDate = args.endDate || "yesterday";
  const startDate = args.startDate || `${days}daysAgo`;

  const dimension = "date";
  const metrics = ["sessions", "activeUsers"];

  const res = await runReport({
    dimensions: [dimension],
    metrics,
    startDate,
    endDate,
    limit: 1000,
    orderByMetric: null,
    orderDesc: false,
  });

  const rows = (res.rows || [])
    .map((r) => ({
      date: yyyymmddToIso(r.dimensionValues?.[0]?.value),
      sessions: Number(r.metricValues?.[0]?.value || 0),
      activeUsers: Number(r.metricValues?.[1]?.value || 0),
    }))
    .sort((a, b) => (a.date > b.date ? 1 : -1));

  return {
    report: "daily_trend",
    propertyId: GA4_PROPERTY_ID,
    dateRange: { startDate, endDate },
    rows,
  };
}

const TOOLS = [
  {
    name: "ga_channel_summary",
    description:
      "GA4の集客チャネル別サマリー（Organic/Paid/Social/Referral…）を返します",
    inputSchema: {
      type: "object",
      properties: {
        startDate: { type: "string", description: "例: 7daysAgo / 2025-12-01" },
        endDate: { type: "string", description: "例: yesterday / 2025-12-31" },
        limit: { type: "number", description: "最大行数（省略時50）" },
      },
    },
    handler: tool_channel_summary,
  },
  {
    name: "ga_landing_pages_top",
    description: "流入が多いLP（landing page）ランキング TOP20 を返します",
    inputSchema: {
      type: "object",
      properties: {
        startDate: {
          type: "string",
          description: "例: 30daysAgo / 2025-12-01",
        },
        endDate: { type: "string", description: "例: yesterday / 2025-12-31" },
        limit: { type: "number", description: "最大行数（省略時20）" },
      },
    },
    handler: tool_landing_pages_top,
  },
  {
    name: "ga_daily_trend",
    description: "直近N日の日次推移（sessions/activeUsers）を返します",
    inputSchema: {
      type: "object",
      properties: {
        days: { type: "number", description: "直近何日（省略時30）" },
        startDate: {
          type: "string",
          description: "例: 30daysAgo / 2025-12-01（省略可）",
        },
        endDate: {
          type: "string",
          description: "例: yesterday / 2025-12-31（省略可）",
        },
      },
    },
    handler: tool_daily_trend,
  },
];

function listTools() {
  return TOOLS.map(({ name, description, inputSchema }) => ({
    name,
    description,
    inputSchema,
  }));
}

// ---- MCP(JSON-RPC風) エンドポイント ----
// 互換のため2パターン対応：
// 1) JSON-RPC: {jsonrpc:"2.0", id, method:"tools/list"|"tools/call", params:{}}
// 2) 簡易: {tool:"ga_channel_summary", arguments:{}}
app.post("/mcp", async (req, res) => {
  try {
    const body = req.body || {};

    // 簡易形式
    if (body.tool) {
      const tool = TOOLS.find((t) => t.name === body.tool);
      if (!tool) return res.status(404).json({ error: "tool not found" });
      const out = await tool.handler(body.arguments || {});
      return res.json(out);
    }

    // JSON-RPC形式
    const { jsonrpc, id, method, params } = body;
    if (jsonrpc !== "2.0") {
      return res.status(400).json({ error: "invalid jsonrpc" });
    }

    if (method === "initialize") {
      return res.json({
        jsonrpc: "2.0",
        id,
        result: {
          serverInfo: { name: "ga4-mcp-cloudrun", version: "1.0.0" },
          capabilities: { tools: {} },
        },
      });
    }

    if (method === "tools/list") {
      return res.json({
        jsonrpc: "2.0",
        id,
        result: { tools: listTools() },
      });
    }

    if (method === "tools/call") {
      const name = params?.name;
      const args = params?.arguments || {};
      const tool = TOOLS.find((t) => t.name === name);
      if (!tool) {
        return res.json({
          jsonrpc: "2.0",
          id,
          error: { code: -32601, message: "tool not found" },
        });
      }
      const out = await tool.handler(args);
      return res.json({
        jsonrpc: "2.0",
        id,
        result: {
          content: [{ type: "text", text: JSON.stringify(out, null, 2) }],
        },
      });
    }

    return res.json({
      jsonrpc: "2.0",
      id,
      error: { code: -32601, message: "method not found" },
    });
  } catch (e) {
    const msg = e?.message || String(e);
    return res.status(500).json({ error: msg });
  }
});

// ついでに tools をブラウザで見たい用
app.get("/tools", (_, res) => res.json({ tools: listTools() }));

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`listening on ${port}`);
});
