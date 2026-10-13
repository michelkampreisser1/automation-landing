// fadc-spend — pulls daily Meta ad spend server-side and stores it in fadc.ad_spend.
// build: v5 (adds read-only ?level=ad|adset breakdown and ?status=<id> node check; no DB writes in either mode)
//
// Why this exists: the daily ledger used to be read by driving Ads Manager in Chrome on
// Michel's Mac. That silently did nothing whenever the Mac was asleep — the ledger sat
// stale on Oct 9 and Oct 10 and nobody was told. Supabase reaches graph.facebook.com
// directly, so the pull happens here on a schedule and the 8:57am task just reads the table.
//
// Auth: x-fadc-cron header matched against fadc.settings['cron_secret'] — the same shared
// secret the followups cron already uses, so the value lives in exactly one place.
// Deployed with verify_jwt=false because pg_cron calls it without a JWT.

import postgres from "npm:postgres@3.4.4";

const GRAPH = "https://graph.facebook.com/v21.0";
const ACCOUNT = Deno.env.get("META_AD_ACCOUNT") ?? "act_921592927669863";
const DB_URL = Deno.env.get("SUPABASE_DB_URL") ?? "";

// Prefer a purpose-made ads token; fall back to the page token in case it ever gains
// ads_read. The response names which one worked, so this stays debuggable.
const TOKENS: Array<[string, string]> = [
  ["META_ADS_TOKEN", Deno.env.get("META_ADS_TOKEN") ?? ""],
  ["META_PAGE_TOKEN", Deno.env.get("META_PAGE_TOKEN") ?? ""],
];

const CONV_ACTIONS = new Set([
  "onsite_conversion.messaging_conversation_started_7d",
  "onsite_conversion.total_messaging_connection",
]);

const todayNY = () =>
  new Date().toLocaleDateString("en-CA", { timeZone: "America/New_York" });

const daysAgoNY = (n: number) =>
  new Date(Date.now() - n * 86400000)
    .toLocaleDateString("en-CA", { timeZone: "America/New_York" });

type Row = {
  day: string;
  spend: number;
  impressions: number;
  link_clicks: number;
  conversations: number;
  is_final: boolean;
};

function countActions(actions: Array<{ action_type: string; value: string }>) {
  let conversations = 0;
  let linkClicks = 0;
  for (const a of actions) {
    if (CONV_ACTIONS.has(a.action_type)) {
      conversations = Math.max(conversations, Number(a.value) || 0);
    }
    if (a.action_type === "link_click") linkClicks = Number(a.value) || 0;
  }
  return { conversations, linkClicks };
}

async function graphGet(path: string, params: Record<string, string>) {
  const errors: string[] = [];
  for (const [label, token] of TOKENS) {
    if (!token) continue;
    const url = new URL(`${GRAPH}/${path}`);
    for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
    const res = await fetch(url.toString(), {
      headers: { Authorization: `Bearer ${token}` },
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok || body?.error) {
      errors.push(`${label}: ${body?.error?.message ?? `HTTP ${res.status}`}`);
      continue;
    }
    return { body, tokenUsed: label, errors };
  }
  throw new Error(
    `no Meta token with ads_read. Tried: ${errors.join(" | ") || "none configured"}`,
  );
}

async function fetchInsights(from: string, to: string) {
  const { body, tokenUsed, errors } = await graphGet(`${ACCOUNT}/insights`, {
    level: "account",
    time_increment: "1",
    time_range: JSON.stringify({ since: from, until: to }),
    fields: "spend,impressions,actions,date_start",
    limit: "100",
  });

  const today = todayNY();
  const rows: Row[] = (body.data ?? []).map((d: Record<string, unknown>) => {
    const { conversations, linkClicks } = countActions(
      (d.actions ?? []) as Array<{ action_type: string; value: string }>,
    );
    const day = String(d.date_start);
    return {
      day,
      spend: Number(d.spend) || 0,
      impressions: Number(d.impressions) || 0,
      link_clicks: linkClicks,
      conversations,
      // A day is only trustworthy once it has fully closed in the account's own day.
      is_final: day < today,
    };
  });

  return { rows, tokenUsed, errors };
}

// Read-only breakdown by ad or adset for a date range. Nothing is written; this is how
// we see which creative actually produces conversations without opening Ads Manager.
async function fetchBreakdown(level: "ad" | "adset", from: string, to: string) {
  const { body, tokenUsed, errors } = await graphGet(`${ACCOUNT}/insights`, {
    level,
    time_range: JSON.stringify({ since: from, until: to }),
    fields:
      "campaign_name,adset_name,ad_name,ad_id,adset_id,spend,impressions,reach,frequency,actions,cost_per_action_type",
    limit: "200",
  });
  const rows = (body.data ?? []).map((d: Record<string, unknown>) => {
    const { conversations, linkClicks } = countActions(
      (d.actions ?? []) as Array<{ action_type: string; value: string }>,
    );
    const spend = Number(d.spend) || 0;
    return {
      campaign: d.campaign_name,
      adset: d.adset_name,
      adset_id: d.adset_id,
      ad: d.ad_name,
      ad_id: d.ad_id,
      spend,
      impressions: Number(d.impressions) || 0,
      reach: Number(d.reach) || 0,
      frequency: Number(d.frequency) || 0,
      link_clicks: linkClicks,
      conversations,
      cost_per_conversation: conversations ? Math.round((spend / conversations) * 100) / 100 : null,
    };
  });
  return { rows, tokenUsed, errors };
}

Deno.serve(async (req: Request) => {
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body, null, 2), {
      status,
      headers: { "Content-Type": "application/json" },
    });

  const url = new URL(req.url);

  // Permission probe. Deliberately needs no secret: it returns only whether each
  // configured token can read ad insights — never a token, never a spend figure. It is
  // how we find out which token to wire up without anyone pasting a credential to check.
  if (url.searchParams.get("probe") === "1") {
    const results: Record<string, string> = {};
    for (const [label, token] of TOKENS) {
      if (!token) { results[label] = "not configured"; continue; }
      try {
        const u = new URL(`${GRAPH}/${ACCOUNT}/insights`);
        u.searchParams.set("fields", "spend");
        u.searchParams.set("date_preset", "yesterday");
        const r = await fetch(u.toString(), {
          headers: { Authorization: `Bearer ${token}` },
        });
        const b = await r.json().catch(() => ({}));
        results[label] = (r.ok && !b?.error)
          ? "OK — can read ad insights"
          : `DENIED — ${b?.error?.message ?? `HTTP ${r.status}`}`;
      } catch (e) {
        results[label] = `ERROR — ${e instanceof Error ? e.message : String(e)}`;
      }
    }
    return json({ probe: true, account: ACCOUNT, tokens: results });
  }

  // Default window: the last 4 closed days plus today, so a missed run self-heals
  // instead of leaving a permanent hole in the ledger.
  const from = url.searchParams.get("from") ?? daysAgoNY(4);
  const to = url.searchParams.get("to") ?? todayNY();
  const level = url.searchParams.get("level");

  let sql: ReturnType<typeof postgres> | null = null;
  try {
    sql = postgres(DB_URL, { prepare: false, max: 1 });

    const given = req.headers.get("x-fadc-cron") ?? "";
    const [secret] = await sql`
      select value from fadc.settings where key = 'cron_secret'
    `;
    if (!secret?.value || given !== secret.value) {
      return json({ ok: false, error: "unauthorized" }, 401);
    }

    if (level === "ad" || level === "adset") {
      const { rows, tokenUsed, errors } = await fetchBreakdown(level, from, to);
      return json({ ok: true, mode: `breakdown:${level}`, token_used: tokenUsed, token_errors: errors, range: { from, to }, rows });
    }

    // Read-only node status check: ?status=<ad_id> — confirms an ad's actual on/off state
    // in Meta's own system, independent of whatever the Ads Manager UI claims mid-publish.
    const statusId = url.searchParams.get("status");
    if (statusId) {
      const { body: node, tokenUsed, errors } = await graphGet(statusId, {
        fields: "name,status,effective_status,configured_status",
      });
      return json({ ok: true, mode: "status", token_used: tokenUsed, token_errors: errors, node });
    }

    const { rows, tokenUsed, errors } = await fetchInsights(from, to);

    for (const r of rows) {
      await sql`
        insert into fadc.ad_spend
          (day, account_id, spend, impressions, link_clicks, conversations, is_final, fetched_at)
        values
          (${r.day}, ${ACCOUNT}, ${r.spend}, ${r.impressions},
           ${r.link_clicks}, ${r.conversations}, ${r.is_final}, now())
        on conflict (day, account_id) do update set
          spend         = excluded.spend,
          impressions   = excluded.impressions,
          link_clicks   = excluded.link_clicks,
          conversations = excluded.conversations,
          is_final      = excluded.is_final,
          fetched_at    = now()
      `;
    }

    await sql`
      insert into fadc.spend_runs (days_from, days_to, rows_upsert, ok)
      values (${from}, ${to}, ${rows.length}, true)
    `;

    return json({
      ok: true,
      token_used: tokenUsed,
      token_errors: errors,
      range: { from, to },
      rows,
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    // Record the failure so a silently broken cron shows up in fadc.spend_runs
    // rather than just leaving the ledger quietly stale.
    try {
      if (!sql) sql = postgres(DB_URL, { prepare: false, max: 1 });
      await sql`
        insert into fadc.spend_runs (days_from, days_to, rows_upsert, ok, error)
        values (${from}, ${to}, 0, false, ${msg})
      `;
    } catch { /* logging is best effort */ }
    return json({ ok: false, error: msg, range: { from, to } }, 500);
  } finally {
    try { await sql?.end(); } catch { /* ignore */ }
  }
});
