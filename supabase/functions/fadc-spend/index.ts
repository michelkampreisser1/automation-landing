// fadc-spend — pulls daily Meta ad spend server-side and stores it in fadc.ad_spend.
// build: v6 (writes a fadc_ad_spend heartbeat on the scheduled run; read/status modes don't)
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

async function heartbeat(sql: ReturnType<typeof postgres>, jobname: string, ok: boolean, error?: string) {
  try {
    await sql`
      insert into fadc.cron_heartbeats (jobname, last_attempt_at, last_ok_at, last_error)
      values (${jobname}, now(), ${ok ? sql`now()` : null}, ${ok ? null : (error ?? null)})
      on conflict (jobname) do update set
        last_attempt_at = now(),
        last_ok_at = case when ${ok} then now() else fadc.cron_heartbeats.last_ok_at end,
        last_error = case when ${ok} then null else excluded.last_error end
    `;
  } catch { /* heartbeat is best-effort, never block the actual job */ }
}

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
        // This has to be the SAME call shape the real pull uses. The old probe asked for
        // "spend" with date_preset=yesterday and came back OK on 21 oct while the actual
        // job was writing zeros — a narrower request can succeed where the real one is
        // served without its paid fields, so a probe that does not match proves nothing.
        const u = new URL(`${GRAPH}/${ACCOUNT}/insights`);
        u.searchParams.set("level", "account");
        u.searchParams.set("time_increment", "1");
        u.searchParams.set("time_range", JSON.stringify({ since: daysAgoNY(2), until: daysAgoNY(1) }));
        u.searchParams.set("fields", "spend,impressions,actions,date_start");
        const r = await fetch(u.toString(), {
          headers: { Authorization: `Bearer ${token}` },
        });
        const b = await r.json().catch(() => ({}));
        if (!r.ok || b?.error) {
          results[label] = `DENIED — ${b?.error?.message ?? `HTTP ${r.status}`}`;
        } else {
          // Reaching the endpoint is not the same as being served the money. Report
          // whether the paid fields actually came back, which is the thing that broke.
          const d = (b?.data ?? []) as Array<Record<string, unknown>>;
          const hasPaidFields = d.some((x) => x.spend !== undefined && x.impressions !== undefined);
          const hasActivity = d.some((x) => Array.isArray(x.actions) && (x.actions as unknown[]).length > 0);
          results[label] = hasPaidFields
            ? "OK — returns spend and impressions"
            : d.length === 0
            ? "OK — reachable, but no rows in window (cannot confirm paid fields)"
            : `PARTIAL — reachable${hasActivity ? " and returns actions" : ""} but NO spend/impressions. This token cannot see the account's money.`;
        }
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

    // THE LEDGER THAT LIED (found 21 oct). From 17 oct this job wrote spend $0.00 and
    // 0 impressions every four hours while reporting ok:true — and Meta was charging the
    // card the whole time ($79 on the 16th, $133 on the 19th, $89.71 still outstanding).
    // A week of unit economics was computed against a ledger short by roughly $180.
    //
    // The tell was in the data all along: rows with 15 link_clicks and 0 impressions.
    // That combination cannot occur in real Meta data — nobody clicks an ad that was
    // never shown. It means the response came back without the paid fields, which is what
    // happens when graphGet silently falls through to META_PAGE_TOKEN: a Page token still
    // sees messaging actions attributed to the Page, but never the ad account's money.
    //
    // So: a row that reports activity with no impressions is not written at all. Good rows
    // still land, the run is marked failed with the token named, and a notification goes
    // out, because the first failure mode here was being quiet.
    const corrupt = rows.filter((r) =>
      r.impressions === 0 && r.spend === 0 && (r.link_clicks > 0 || r.conversations > 0)
    );
    const sane = rows.filter((r) => !corrupt.includes(r));

    for (const r of sane) {
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

    if (corrupt.length) {
      const days = corrupt.map((r) => r.day).join(", ");
      const detail =
        `Meta returned activity with no impressions for ${days} (token: ${tokenUsed}). ` +
        `Those days were NOT written, the ledger keeps its last good numbers. ` +
        `Spend is understated until this is fixed — check real spend in Ads Manager billing.`;
      await sql`
        insert into fadc.spend_runs (days_from, days_to, rows_upsert, ok, error)
        values (${from}, ${to}, ${sane.length}, false, ${detail})
      `;
      // Once a day at most: this cron runs every 4 hours and the same broken token would
      // otherwise produce six identical alerts before breakfast.
      const [recent] = await sql`
        select 1 from fadc.notifications
         where kind = 'spend_ledger_broken' and created_at > now() - interval '20 hours' limit 1`;
      if (!recent) {
        await sql`
          insert into fadc.notifications (kind, text)
          values ('spend_ledger_broken', ${"\u{1F4B8} Ad spend ledger is not recording. " + detail})`;
      }
      await heartbeat(sql, "fadc_ad_spend", false, detail);
      return json({ ok: false, error: detail, token_used: tokenUsed, token_errors: errors, written: sane.length, skipped: corrupt }, 500);
    }

    await sql`
      insert into fadc.spend_runs (days_from, days_to, rows_upsert, ok)
      values (${from}, ${to}, ${sane.length}, true)
    `;

    await heartbeat(sql, "fadc_ad_spend", true);

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
      await heartbeat(sql, "fadc_ad_spend", false, msg);
    } catch { /* logging is best effort */ }
    return json({ ok: false, error: msg, range: { from, to } }, 500);
  } finally {
    try { await sql?.end(); } catch { /* ignore */ }
  }
});
