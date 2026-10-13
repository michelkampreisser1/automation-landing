// fadc-canary — watches whether the fadc-agent / fadc-spend crons are actually succeeding.
// build: v1
//
// Why this exists: on Oct 12-13 the fadc-agent deploy picked up verify_jwt=true, so every
// cron call for 17.5 hours got a 401. pg_cron's own job-run log showed "succeeded" the whole
// time — it only knows the HTTP POST was sent, not what came back. The real answer lives in
// net._http_response, and nobody was watching it. This function watches it, on its own
// schedule, independent of the crons it's checking (so a cron that stops firing entirely
// is also caught, not just one that fires and gets rejected).
//
// Logic: look at net._http_response rows from the last WINDOW_MINUTES. Any status code
// outside 200-299, or a timed-out request, counts as a failure. State (ok / alerting) is
// kept in fadc.settings so we alert once when things break and once when they recover,
// instead of spamming Telegram every 15 minutes while broken.
//
// Auth: same x-fadc-cron header + fadc.settings['cron_secret'] as the other crons.
// Deployed with verify_jwt=false — pg_cron calls it without a JWT.

import postgres from "npm:postgres@3.4.4";

const DB_URL = Deno.env.get("SUPABASE_DB_URL") ?? "";
const TG_TOKEN = Deno.env.get("TELEGRAM_BOT_TOKEN") ?? "";
const TG_CHAT = Deno.env.get("TELEGRAM_CHAT_ID") ?? "";

const WINDOW_MINUTES = 20; // wider than the 15-min canary schedule, so no gap between runs
const REALERT_MINUTES = 60; // while still broken, nag again at most this often

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

    const failRows = await sql<{ id: number; status_code: number | null; timed_out: boolean; created: string }[]>`
      select id, status_code, timed_out, created
      from net._http_response
      where created > now() - interval '${sql.unsafe(String(WINDOW_MINUTES))} minutes'
        and (status_code is null or status_code < 200 or status_code >= 300 or timed_out = true)
      order by id desc
    `;

    const totalRows = await sql<{ n: string }[]>`
      select count(*)::text as n from net._http_response
      where created > now() - interval '${sql.unsafe(String(WINDOW_MINUTES))} minutes'
    `;
    const total = Number(totalRows[0]?.n ?? 0);

    const [stateRow] = await sql`select value from fadc.settings where key = 'canary_state'`;
    let state: { status: "ok" | "alerting"; last_alert_at: string | null } = stateRow?.value
      ? JSON.parse(stateRow.value)
      : { status: "ok", last_alert_at: null };

    const nowIso = new Date().toISOString();
    let action = "none";
    let telegramSent = false;

    if (failRows.length > 0) {
      // Something's failing. No calls at all in the window (total === failRows.length === 0
      // doesn't apply here since failRows.length>0 implies total>0) — alert now if we weren't
      // already, or re-alert if we've been broken for a while without a fresh nudge.
      const shouldAlert =
        state.status === "ok" ||
        !state.last_alert_at ||
        (Date.now() - new Date(state.last_alert_at).getTime()) > REALERT_MINUTES * 60_000;

      if (shouldAlert) {
        const worst = failRows[0];
        const lines = failRows.slice(0, 5).map((r) =>
          `  #${r.id} → ${r.timed_out ? "timeout" : r.status_code} at ${new Date(r.created).toLocaleTimeString("en-US", { timeZone: "America/New_York" })} ET`
        );
        const text =
          `🔴 FADC cron alert: ${failRows.length}/${total} HTTP calls failed in the last ${WINDOW_MINUTES} min.\n` +
          `Latest: ${worst.timed_out ? "timeout" : worst.status_code} at ${new Date(worst.created).toLocaleTimeString("en-US", { timeZone: "America/New_York" })} ET.\n` +
          lines.join("\n") +
          `\nCheck net._http_response and the fadc-agent deploy (verify_jwt must be false).`;
        telegramSent = await sendTelegram(text);
        action = "alerted";
      } else {
        action = "already_alerting";
      }
      state = { status: "alerting", last_alert_at: shouldAlert ? nowIso : state.last_alert_at };
    } else if (total > 0 && state.status === "alerting") {
      // We were broken and now every call in the window succeeded — recovered.
      telegramSent = await sendTelegram(`✅ FADC cron recovered — all ${total} calls in the last ${WINDOW_MINUTES} min returned 2xx.`);
      action = "recovered";
      state = { status: "ok", last_alert_at: null };
    } else if (total === 0) {
      // No cron activity at all in the window is itself suspicious (fadc_followups runs
      // every 30 min, so a 20-min window should normally have at least one call once every
      // other run). Don't hard-alert on this alone — just surface it in the response — since
      // the window can legitimately be empty right after a fresh deploy.
      action = "no_calls_in_window";
    }

    await sql`
      insert into fadc.settings (key, value) values ('canary_state', ${JSON.stringify(state)})
      on conflict (key) do update set value = excluded.value
    `;

    return json({
      ok: true,
      window_minutes: WINDOW_MINUTES,
      total_calls: total,
      failed_calls: failRows.length,
      action,
      telegram_sent: telegramSent,
      state,
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return json({ ok: false, error: msg }, 500);
  } finally {
    try { await sql?.end(); } catch { /* ignore */ }
  }
});
