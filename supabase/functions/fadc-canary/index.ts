// fadc-canary — watches whether the fadc crons are actually succeeding.
// build: v2 (reads fadc.cron_heartbeats instead of net._http_response — no more false positives
// from ad-hoc SQL diagnostic calls that happen to hit graph.facebook.com)
//
// Each cron job writes its own heartbeat row on success/failure via the heartbeat() helper
// in its edge function. This canary checks that every known job's last_ok_at is recent
// enough for its schedule. If any job is stale or has never reported, we alert.
//
// Auth: same x-fadc-cron header + fadc.settings['cron_secret'] as the other crons.
// Deployed with verify_jwt=false — pg_cron calls it without a JWT.

import postgres from "npm:postgres@3.4.4";

const DB_URL = Deno.env.get("SUPABASE_DB_URL") ?? "";
const TG_TOKEN = Deno.env.get("TELEGRAM_BOT_TOKEN") ?? "";
const TG_CHAT = Deno.env.get("TELEGRAM_CHAT_ID") ?? "";

// Known cron jobs and their maximum allowed staleness (minutes since last_ok_at).
// Generous padding over actual schedule so normal jitter doesn't fire false alarms.
const JOBS: Record<string, number> = {
  fadc_followups:        45,   // runs every 30 min → stale after 45
  fadc_ad_spend:        300,   // runs every 4 h   → stale after 5 h
  fadc_closeout_ask:    180,   // runs at 21:00 & 22:00 → stale after 3 h (checked relative to last run)
  fadc_closeout_remind: 180,   // runs at 00:00 & 01:00 → stale after 3 h
  fadc_weekly_summary: 10200,  // runs Wed 12:00     → stale after 7 d + 2 h (10200 min)
};

const REALERT_MINUTES = 60;

async function sendTelegram(text: string): Promise<boolean> {
  if (!TG_TOKEN || !TG_CHAT) return false;
  try {
    const res = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ chat_id: TG_CHAT, text }),
    });
    return res.ok;
  } catch {
    return false;
  }
}

type HeartbeatRow = {
  jobname: string;
  last_attempt_at: string | null;
  last_ok_at: string | null;
  last_error: string | null;
};

Deno.serve(async (req: Request) => {
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body, null, 2), {
      status,
      headers: { "Content-Type": "application/json" },
    });

  let sql: ReturnType<typeof postgres> | null = null;
  try {
    sql = postgres(DB_URL, { prepare: false, max: 1 });

    const given = req.headers.get("x-fadc-cron") ?? "";
    const [secret] = await sql`select value from fadc.settings where key = 'cron_secret'`;
    if (!secret?.value || given !== secret.value) {
      return json({ ok: false, error: "unauthorized" }, 401);
    }

    // Read all heartbeats
    const rows = await sql<HeartbeatRow[]>`
      select jobname, last_attempt_at, last_ok_at, last_error
      from fadc.cron_heartbeats
    `;
    const byJob = new Map(rows.map((r) => [r.jobname, r]));

    const now = Date.now();
    const stale: Array<{ job: string; reason: string }> = [];

    for (const [job, maxMinutes] of Object.entries(JOBS)) {
      const hb = byJob.get(job);
      if (!hb) {
        // Job has never reported — could be freshly deployed, give it one cycle grace.
        // But if we've been running for a while (canary itself has run before), it's a problem.
        stale.push({ job, reason: "never reported a heartbeat" });
        continue;
      }
      if (!hb.last_ok_at) {
        stale.push({
          job,
          reason: `last attempt failed: ${hb.last_error ?? "unknown error"}`,
        });
        continue;
      }
      const ageMin = (now - new Date(hb.last_ok_at).getTime()) / 60_000;
      if (ageMin > maxMinutes) {
        stale.push({
          job,
          reason: `last_ok ${Math.round(ageMin)} min ago (max ${maxMinutes})${hb.last_error ? ` — last error: ${hb.last_error}` : ""}`,
        });
      }
    }

    // State machine: ok ↔ alerting, same as v1
    const [stateRow] = await sql`select value from fadc.settings where key = 'canary_state'`;
    let state: { status: "ok" | "alerting"; last_alert_at: string | null } = stateRow?.value
      ? JSON.parse(stateRow.value)
      : { status: "ok", last_alert_at: null };

    const nowIso = new Date().toISOString();
    let action = "none";
    let telegramSent = false;

    if (stale.length > 0) {
      const shouldAlert =
        state.status === "ok" ||
        !state.last_alert_at ||
        (now - new Date(state.last_alert_at).getTime()) > REALERT_MINUTES * 60_000;

      if (shouldAlert) {
        const lines = stale.map((s) => `  • ${s.job}: ${s.reason}`);
        const text =
          `🔴 FADC cron alert: ${stale.length} job(s) stale or failing.\n` +
          lines.join("\n") +
          `\nCheck edge-function deploys (verify_jwt must be false).`;
        telegramSent = await sendTelegram(text);
        action = "alerted";
      } else {
        action = "already_alerting";
      }
      state = { status: "alerting", last_alert_at: shouldAlert ? nowIso : state.last_alert_at };
    } else if (state.status === "alerting") {
      telegramSent = await sendTelegram(
        `✅ FADC cron recovered — all ${Object.keys(JOBS).length} jobs reporting OK.`,
      );
      action = "recovered";
      state = { status: "ok", last_alert_at: null };
    }

    await sql`
      insert into fadc.settings (key, value) values ('canary_state', ${JSON.stringify(state)})
      on conflict (key) do update set value = excluded.value
    `;

    // Write canary's own heartbeat so the table reflects canary is running too
    await sql`
      insert into fadc.cron_heartbeats (jobname, last_attempt_at, last_ok_at, last_error)
      values ('fadc_canary', now(), now(), null)
      on conflict (jobname) do update set
        last_attempt_at = now(),
        last_ok_at = now(),
        last_error = null
    `;

    return json({
      ok: true,
      jobs_checked: Object.keys(JOBS).length,
      stale_jobs: stale,
      action,
      telegram_sent: telegramSent,
      state,
      heartbeats: Object.fromEntries(
        Object.keys(JOBS).map((j) => {
          const hb = byJob.get(j);
          return [j, hb ? { last_ok_at: hb.last_ok_at, last_error: hb.last_error } : null];
        }),
      ),
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return json({ ok: false, error: msg }, 500);
  } finally {
    try { await sql?.end(); } catch { /* ignore */ }
  }
});
