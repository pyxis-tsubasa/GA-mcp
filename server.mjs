import express from "express";
import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import {
  BetaAnalyticsDataClient,
  AlphaAnalyticsDataClient,
} from "@google-analytics/data";

const app = express();
app.use(express.json({ limit: "1mb" }));

// ===== 必須環境変数 =====
const GA4_PROPERTY_ID = process.env.GA4_PROPERTY_ID;
if (!GA4_PROPERTY_ID) {
  console.error("Missing env: GA4_PROPERTY_ID");
  process.exit(1);
}

// ===== 認証（おすすめ：URLにトークンを埋める方式） =====
// /mcp/<token> でアクセスした時だけ通す（ChatGPT側でヘッダー付与しなくて良い）
const MCP_PATH_TOKEN = process.env.MCP_PATH_TOKEN; // Secret Manager から渡す想定
// 互換用：curl等で使いたい場合のヘッダー方式（任意）
const MCP_API_KEY = process.env.MCP_API_KEY;

// セキュリティ：どちらも無いなら起動しない（事故防止）
if (!MCP_PATH_TOKEN && !MCP_API_KEY) {
  console.error("Missing auth env: set MCP_PATH_TOKEN (recommended) or MCP_API_KEY");
  process.exit(1);
}

function isAuthorized(req) {
  const tokenInPath = req.params?.token;
  if (MCP_PATH_TOKEN && tokenInPath === MCP_PATH_TOKEN) return true;

  const headerKey = req.header("x-api-key");
  if (MCP_API_KEY && headerKey && headerKey === MCP_API_KEY) return true;

  return false;
}

// ===== GAクライアント =====
const gaClient = new BetaAnalyticsDataClient();
// Funnelは v1alpha
const gaAlphaClient = new AlphaAnalyticsDataClient();

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

// ISO日付の -1年
function shiftIsoYear(iso, deltaYears) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  d.setFullYear(d.getFullYear() + deltaYears);
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

// ===== runReport（ヘッダ名→オブジェクト化） =====
async function runReportKV({
  dimensions = [],
  metrics = [],
  startDate,
  endDate,
  limit,
  orderByMetric,
  desc = true,
  dimensionFilter, // FilterExpression (任意)
}) {
  const [resp] = await gaClient.runReport({
    property: `properties/${GA4_PROPERTY_ID}`,
    dateRanges: [{ startDate, endDate }],
    dimensions: dimensions.map((name) => ({ name })),
    metrics: metrics.map((name) => ({ name })),
    limit: limit != null ? String(limit) : undefined,
    orderBys: orderByMetric
      ? [
          {
            metric: { metricName: orderByMetric },
            desc,
          },
        ]
      : undefined,
    dimensionFilter: dimensionFilter || undefined,
  });

  const dimHeaders = (resp.dimensionHeaders ?? []).map((h) => h.name || "");
  const metHeaders = (resp.metricHeaders ?? []).map((h) => h.name || "");

  const rows = (resp.rows ?? []).map((r) => {
    const out = {};
    dimHeaders.forEach((name, i) => {
      out[name] = r.dimensionValues?.[i]?.value ?? "";
    });
    metHeaders.forEach((name, i) => {
      out[name] = toNumber(r.metricValues?.[i]?.value);
    });
    return out;
  });

  return { rows, dimHeaders, metHeaders };
}

// ===== Metadata（使える指標/項目の検索用） =====
async function getMetadata() {
  const [meta] = await gaClient.getMetadata({
    name: `properties/${GA4_PROPERTY_ID}/metadata`,
  });
  return meta;
}

function jsonText(obj) {
  return { content: [{ type: "text", text: JSON.stringify(obj, null, 2) }] };
}

// ===== MCPサーバー（ツール定義） =====
const mcp = new McpServer({ name: "ga-mcp", version: "2.0.0" });

// 0) 使えるディメンション/メトリクス検索（最強の補助ツール）
mcp.tool(
  "ga_metadata_search",
  "GA4 Data APIで使えるディメンション/メトリクスを検索（そのプロパティ固有のカスタム定義も含む）",
  {
    kind: z.enum(["all", "dimension", "metric"]).default("all"),
    query: z.string().default(""),
    limit: z.number().min(1).max(200).default(50),
  },
  async ({ kind, query, limit }) => {
    const meta = await getMetadata();
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

    function match(x) {
      if (!q) return true;
      return (
        String(x.apiName || "").toLowerCase().includes(q) ||
        String(x.uiName || "").toLowerCase().includes(q) ||
        String(x.description || "").toLowerCase().includes(q)
      );
    }

    const out = {
      propertyId: GA4_PROPERTY_ID,
      kind,
      query,
      dimensions:
        kind === "metric" ? [] : dims.filter(match).slice(0, limit),
      metrics:
        kind === "dimension" ? [] : mets.filter(match).slice(0, limit),
      note:
        "欲しい項目が見つからない時は query を短く（例: source, medium, campaign, pagePath, keyEvent）して試してください。",
    };

    return jsonText(out);
  }
);

// 1) KPIサマリー
mcp.tool(
  "ga_kpi_overview",
  "KPIサマリー（sessions/users/views/keyEvents/CVR/revenue等）を返す",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    keyEventName: z.string().optional(), // 特定キーイベント（例: "purchase"）のレートを見たい時
  },
  async ({ startDate, endDate, keyEventName }) => {
    const metrics = [
      "sessions",
      "activeUsers",
      "newUsers",
      "screenPageViews",
      "keyEvents",
      "sessionKeyEventRate",
      "userKeyEventRate",
      "totalRevenue",
      "purchaseRevenue",
      "transactions",
    ];

    // 特定キーイベントのレート（例: sessionKeyEventRate:purchase）
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

    const row = rows[0] || {};
    return jsonText({
      report: "kpi_overview",
      propertyId: GA4_PROPERTY_ID,
      dateRange: { startDate, endDate },
      keyEventName: keyEventName || null,
      kpis: row,
      note:
        "keyEvents / sessionKeyEventRate はGA4でKey event（旧コンバージョン）設定されているイベントが前提です。",
    });
  }
);

// 共通で使う「CV込みの基本メトリクス」
function baseMetricsPlus(keyEventName) {
  const m = ["sessions", "activeUsers", "keyEvents", "sessionKeyEventRate"];
  if (keyEventName) m.push(`sessionKeyEventRate:${keyEventName}`);
  return m;
}

// 2) チャネル別（CV含む）
mcp.tool(
  "ga_channel_summary_plus",
  "集客チャネル別（Default Channel Group）サマリー（CV含む）を返す",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    keyEventName: z.string().optional(),
    limit: z.number().min(1).max(200).default(50),
  },
  async ({ startDate, endDate, keyEventName, limit }) => {
    const { rows } = await runReportKV({
      dimensions: ["sessionDefaultChannelGroup"],
      metrics: baseMetricsPlus(keyEventName),
      startDate,
      endDate,
      orderByMetric: "sessions",
      desc: true,
      limit,
    });

    const totals = rows.reduce(
      (acc, r) => {
        acc.sessions += r.sessions || 0;
        acc.activeUsers += r.activeUsers || 0;
        acc.keyEvents += r.keyEvents || 0;
        return acc;
      },
      { sessions: 0, activeUsers: 0, keyEvents: 0 }
    );

    return jsonText({
      report: "channel_summary_plus",
      propertyId: GA4_PROPERTY_ID,
      dateRange: { startDate, endDate },
      keyEventName: keyEventName || null,
      totals,
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
  }
);

// 3) チャネル別：前期間比較
mcp.tool(
  "ga_channel_summary_compare",
  "チャネル別サマリーを前期間/前年比で比較（増減・増減率）",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    compareMode: z.enum(["previous_period", "previous_year"]).default("previous_period"),
    keyEventName: z.string().optional(),
    limit: z.number().min(1).max(200).default(50),
  },
  async ({ startDate, endDate, compareMode, keyEventName, limit }) => {
    // 比較期間を推定
    let compareRange = null;

    if (compareMode === "previous_period") {
      compareRange = computePreviousPeriod(startDate, endDate);
    } else {
      // previous_year は ISO日付が来た時だけ確実にできる
      if (/^\d{4}-\d{2}-\d{2}$/.test(startDate) && /^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
        compareRange = {
          startDate: shiftIsoYear(startDate, -1),
          endDate: shiftIsoYear(endDate, -1),
        };
      }
    }

    if (!compareRange?.startDate || !compareRange?.endDate) {
      return jsonText({
        report: "channel_summary_compare",
        error:
          "比較期間を自動推定できませんでした。startDate/endDate を '7daysAgo〜yesterday' 形式にするか、ISO日付(YYYY-MM-DD)で指定してください。",
        input: { startDate, endDate, compareMode },
      });
    }

    const metrics = baseMetricsPlus(keyEventName);

    const curr = await runReportKV({
      dimensions: ["sessionDefaultChannelGroup"],
      metrics,
      startDate,
      endDate,
      orderByMetric: "sessions",
      desc: true,
      limit: 200,
    });
    const prev = await runReportKV({
      dimensions: ["sessionDefaultChannelGroup"],
      metrics,
      startDate: compareRange.startDate,
      endDate: compareRange.endDate,
      orderByMetric: "sessions",
      desc: true,
      limit: 200,
    });

    const prevMap = new Map(
      prev.rows.map((r) => [r.sessionDefaultChannelGroup || "(not set)", r])
    );

    const merged = curr.rows.map((r) => {
      const ch = r.sessionDefaultChannelGroup || "(not set)";
      const p = prevMap.get(ch) || {};
      const out = {
        channel: ch,
        current: {
          sessions: r.sessions || 0,
          activeUsers: r.activeUsers || 0,
          keyEvents: r.keyEvents || 0,
          sessionKeyEventRate: r.sessionKeyEventRate ?? null,
        },
        previous: {
          sessions: p.sessions || 0,
          activeUsers: p.activeUsers || 0,
          keyEvents: p.keyEvents || 0,
          sessionKeyEventRate: p.sessionKeyEventRate ?? null,
        },
      };

      out.delta = {
        sessions: out.current.sessions - out.previous.sessions,
        activeUsers: out.current.activeUsers - out.previous.activeUsers,
        keyEvents: out.current.keyEvents - out.previous.keyEvents,
        sessionKeyEventRate:
          out.current.sessionKeyEventRate != null && out.previous.sessionKeyEventRate != null
            ? out.current.sessionKeyEventRate - out.previous.sessionKeyEventRate
            : null,
      };

      out.pctChange = {
        sessions: safePctChange(out.current.sessions, out.previous.sessions),
        activeUsers: safePctChange(out.current.activeUsers, out.previous.activeUsers),
        keyEvents: safePctChange(out.current.keyEvents, out.previous.keyEvents),
        sessionKeyEventRate:
          out.current.sessionKeyEventRate != null && out.previous.sessionKeyEventRate != null && out.previous.sessionKeyEventRate !== 0
            ? (out.current.sessionKeyEventRate - out.previous.sessionKeyEventRate) / out.previous.sessionKeyEventRate
            : null,
      };

      if (keyEventName) {
        out.current[`sessionKeyEventRate:${keyEventName}`] = r[`sessionKeyEventRate:${keyEventName}`] ?? null;
        out.previous[`sessionKeyEventRate:${keyEventName}`] = p[`sessionKeyEventRate:${keyEventName}`] ?? null;
      }
      return out;
    });

    merged.sort((a, b) => (b.current.sessions || 0) - (a.current.sessions || 0));

    return jsonText({
      report: "channel_summary_compare",
      propertyId: GA4_PROPERTY_ID,
      dateRange: { startDate, endDate },
      compareRange,
      compareMode,
      keyEventName: keyEventName || null,
      rows: merged.slice(0, limit),
    });
  }
);

// 4) LP別ランキング（CV含む）
mcp.tool(
  "ga_landing_page_performance",
  "LP別（landingPagePlusQueryString）ランキング（CV含む）を返す",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    keyEventName: z.string().optional(),
    limit: z.number().min(1).max(200).default(20),
  },
  async ({ startDate, endDate, keyEventName, limit }) => {
    const metrics = ["sessions", "activeUsers", "keyEvents", "sessionKeyEventRate"];
    if (keyEventName) metrics.push(`sessionKeyEventRate:${keyEventName}`);

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
      keyEventName: keyEventName || null,
      rows: rows.map((r) => ({
        landingPage: r.landingPagePlusQueryString || "(not set)",
        sessions: r.sessions || 0,
        activeUsers: r.activeUsers || 0,
        keyEvents: r.keyEvents || 0,
        sessionKeyEventRate: r.sessionKeyEventRate ?? null,
        ...(keyEventName
          ? { [`sessionKeyEventRate:${keyEventName}`]: r[`sessionKeyEventRate:${keyEventName}`] ?? null }
          : {}),
      })),
    });
  }
);

// 5) 日次推移
mcp.tool(
  "ga_daily_trend",
  "日次推移（date）を返す（sessions/users/keyEvents）",
  {
    startDate: z.string().default("30daysAgo"),
    endDate: z.string().default("yesterday"),
  },
  async ({ startDate, endDate }) => {
    const { rows } = await runReportKV({
      dimensions: ["date"],
      metrics: ["sessions", "activeUsers", "keyEvents"],
      startDate,
      endDate,
      limit: 1000,
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
  }
);

// 6) 日次推移：異常検知
mcp.tool(
  "ga_daily_anomalies",
  "日次推移の異常検知（急増/急減）を返す（移動平均＋標準偏差）",
  {
    startDate: z.string().default("60daysAgo"),
    endDate: z.string().default("yesterday"),
    metric: z.enum(["sessions", "activeUsers", "keyEvents"]).default("sessions"),
    windowDays: z.number().min(3).max(30).default(7),
    zThreshold: z.number().min(1).max(10).default(2.5),
  },
  async ({ startDate, endDate, metric, windowDays, zThreshold }) => {
    const { rows } = await runReportKV({
      dimensions: ["date"],
      metrics: ["sessions", "activeUsers", "keyEvents"],
      startDate,
      endDate,
      limit: 2000,
    });

    const series = rows
      .map((r) => ({
        date: yyyymmddToIso(r.date),
        value: r[metric] || 0,
      }))
      .sort((a, b) => (a.date > b.date ? 1 : -1));

    function mean(arr) {
      if (!arr.length) return 0;
      return arr.reduce((s, x) => s + x, 0) / arr.length;
    }
    function std(arr, mu) {
      if (arr.length < 2) return 0;
      const v = arr.reduce((s, x) => s + (x - mu) ** 2, 0) / (arr.length - 1);
      return Math.sqrt(v);
    }

    const anomalies = [];
    for (let i = windowDays; i < series.length; i++) {
      const window = series.slice(i - windowDays, i).map((x) => x.value);
      const mu = mean(window);
      const sd = std(window, mu);
      const v = series[i].value;

      const z = sd === 0 ? null : (v - mu) / sd;
      const isAnomaly = z != null && Math.abs(z) >= zThreshold;

      if (isAnomaly) {
        anomalies.push({
          date: series[i].date,
          metric,
          value: v,
          baselineMean: mu,
          baselineStd: sd,
          zScore: z,
          direction: z > 0 ? "spike" : "drop",
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
      note:
        "異常が多すぎる場合は zThreshold を上げる（例: 3.0）か、windowDays を増やしてください。",
    });
  }
);

// 7) キャンペーン/UTM成績
mcp.tool(
  "ga_campaign_performance",
  "キャンペーン（source/medium/campaign）別の成績を返す",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    keyEventName: z.string().optional(),
    limit: z.number().min(1).max(200).default(50),
  },
  async ({ startDate, endDate, keyEventName, limit }) => {
    const dims = ["sessionSourceMedium", "sessionCampaignName"];
    const metrics = baseMetricsPlus(keyEventName);

    const { rows } = await runReportKV({
      dimensions: dims,
      metrics,
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
      keyEventName: keyEventName || null,
      rows: rows.map((r) => ({
        sourceMedium: r.sessionSourceMedium || "(not set)",
        campaign: r.sessionCampaignName || "(not set)",
        sessions: r.sessions || 0,
        activeUsers: r.activeUsers || 0,
        keyEvents: r.keyEvents || 0,
        sessionKeyEventRate: r.sessionKeyEventRate ?? null,
      })),
    });
  }
);

// 8) デバイス別
mcp.tool(
  "ga_device_breakdown",
  "デバイス別（deviceCategory）の成績を返す",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    keyEventName: z.string().optional(),
  },
  async ({ startDate, endDate, keyEventName }) => {
    const { rows } = await runReportKV({
      dimensions: ["deviceCategory"],
      metrics: baseMetricsPlus(keyEventName),
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
  }
);

// 9) 国別
mcp.tool(
  "ga_country_breakdown",
  "国別（country）の成績を返す",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    keyEventName: z.string().optional(),
    limit: z.number().min(1).max(200).default(50),
  },
  async ({ startDate, endDate, keyEventName, limit }) => {
    const { rows } = await runReportKV({
      dimensions: ["country"],
      metrics: baseMetricsPlus(keyEventName),
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
  }
);

// 10) 新規/リピーター
mcp.tool(
  "ga_new_vs_returning",
  "新規/リピーター（newVsReturning）の成績を返す",
  {
    startDate: z.string().default("30daysAgo"),
    endDate: z.string().default("yesterday"),
  },
  async ({ startDate, endDate }) => {
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
  }
);

// 11) 人気ページ
mcp.tool(
  "ga_top_pages",
  "人気ページ（pagePathPlusQueryString）のランキング（閲覧・エンゲージメント）",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    limit: z.number().min(1).max(200).default(20),
  },
  async ({ startDate, endDate, limit }) => {
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
      rows: rows.map((r) => ({
        page: r.pagePathPlusQueryString || "(not set)",
        views: r.screenPageViews || 0,
        activeUsers: r.activeUsers || 0,
        engagementSeconds: r.userEngagementDuration || 0,
      })),
    });
  }
);

// 12) イベントTOP
mcp.tool(
  "ga_top_events",
  "イベントTOP（eventName）を返す（eventCount / keyEvents）",
  {
    startDate: z.string().default("7daysAgo"),
    endDate: z.string().default("yesterday"),
    limit: z.number().min(1).max(200).default(30),
  },
  async ({ startDate, endDate, limit }) => {
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
      rows: rows.map((r) => ({
        eventName: r.eventName || "(not set)",
        eventCount: r.eventCount || 0,
        keyEvents: r.keyEvents || 0,
      })),
    });
  }
);

// 13) ファネル（v1alpha runFunnelReport）
mcp.tool(
  "ga_funnel_basic",
  "ファネル（例: page_view → sign_up → purchase）を返す（v1alpha runFunnelReport）",
  {
    startDate: z.string().default("30daysAgo"),
    endDate: z.string().default("yesterday"),
    isOpenFunnel: z.boolean().default(false),
    breakdownDimension: z.string().optional(), // 例: deviceCategory
    breakdownLimit: z.number().min(1).max(50).default(5),
    steps: z
      .array(
        z.object({
          name: z.string().min(1),
          eventName: z.string().min(1),
          // page_view に絞りたい場合など（page_location の contains）
          pageLocationContains: z.string().optional(),
          // 直後に続く制約（厳しめ）
          isDirectlyFollowedBy: z.boolean().optional(),
          // 例: "300s" = 5分以内
          withinDurationFromPriorStep: z.string().optional(),
        })
      )
      .min(2)
      .max(10),
  },
  async ({
    startDate,
    endDate,
    isOpenFunnel,
    breakdownDimension,
    breakdownLimit,
    steps,
  }) => {
    function stepToFilterExpression(s) {
      const eventFilter = { eventName: s.eventName };

      if (s.pageLocationContains) {
        eventFilter.funnelParameterFilterExpression = {
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

      return { funnelEventFilter: eventFilter };
    }

    const funnelSteps = steps.map((s) => ({
      name: s.name,
      isDirectlyFollowedBy: s.isDirectlyFollowedBy ?? false,
      withinDurationFromPriorStep: s.withinDurationFromPriorStep,
      filterExpression: stepToFilterExpression(s),
    }));

    const req = {
      property: `properties/${GA4_PROPERTY_ID}`,
      dateRanges: [{ startDate, endDate }],
      funnel: {
        isOpenFunnel,
        steps: funnelSteps,
      },
      funnelBreakdown: breakdownDimension
        ? {
            breakdownDimension: { name: breakdownDimension },
            limit: String(breakdownLimit),
          }
        : undefined,
      limit: "250000",
    };

    const [resp] = await gaAlphaClient.runFunnelReport(req);

    function parseSubReport(sr) {
      const dimH = (sr.dimensionHeaders ?? []).map((h) => h.name || "");
      const metH = (sr.metricHeaders ?? []).map((h) => h.name || "");
      const outRows = (sr.rows ?? []).map((row) => {
        const o = {};
        dimH.forEach((name, i) => {
          o[name] = row.dimensionValues?.[i]?.value ?? "";
        });
        metH.forEach((name, i) => {
          o[name] = toNumber(row.metricValues?.[i]?.value);
        });
        return o;
      });
      return { dimensionHeaders: dimH, metricHeaders: metH, rows: outRows };
    }

    return jsonText({
      report: "funnel_basic",
      propertyId: GA4_PROPERTY_ID,
      dateRange: { startDate, endDate },
      request: {
        isOpenFunnel,
        breakdownDimension: breakdownDimension || null,
        steps,
      },
      funnelTable: parseSubReport(resp.funnelTable || {}),
      funnelVisualization: parseSubReport(resp.funnelVisualization || {}),
      note:
        "ファネルは v1alpha のため将来互換が変わる可能性があります。",
    });
  }
);

// ===== Streamable HTTP transport（ステートレス） =====
const transport = new StreamableHTTPServerTransport({
  sessionIdGenerator: undefined,
});

async function main() {
  await mcp.connect(transport);

  app.get("/health", (_req, res) => res.status(200).send("ok"));

  // MCP endpoint（/mcp でも /mcp/<token> でもOKにする）
  app.post("/mcp/:token?", async (req, res) => {
    if (!isAuthorized(req))
      return res.status(401).json({ error: "unauthorized" });

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

  // 互換用：GET/DELETE は 405 を返す
  app.get("/mcp/:token?", (_req, res) =>
    res.status(405).send("Method not allowed")
  );
  app.delete("/mcp/:token?", (_req, res) =>
    res.status(405).send("Method not allowed")
  );

  const port = Number(process.env.PORT || 8080);
  app.listen(port, () => console.log(`MCP server listening on :${port}`));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
