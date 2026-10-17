// fadc-canary — watches whether the fadc crons are actually succeeding.
// build: v4 — adds unanswered-lead check: alerts when a lead has waited >30 min with no bot reply.
// v3: reads fadc.cron_heartbeats instead of net._http_response — no more false positives
// from ad-hoc SQL diagnostic calls that happen to hit graph.facebook.com.
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
  fadc_followups:        25,   // runs every 10 min (was 30) → stale after 25, generous padding over schedule
  fadc_ad_spend:        300,   // runs every 4 h   → stale after 5 h
  // v5: these two run ONCE A DAY, so a 3 h threshold marked them stale for ~20 h out of every
  // 24 and the canary sat in "alerting" permanently, re-firing every REALERT_MINUTES. Michel
  // was getting roughly seventeen false alarms a day, which is exactly how a real alert gets
  // ignored. A daily job is only late once it has missed its whole next window: 25 h.
  fadc_closeout_ask:   1500,   // runs daily at 21:00 & 22:00 → stale after 25 h
  fadc_closeout_remind: 1500,  // runs daily at 00:00 & 01:00 → stale after 25 h
  fadc_weekly_summary: 10200,  // runs Wed 12:00     → stale after 7 d + 2 h (10200 min)
};

const REALERT_MINUTES = 60;

// Unanswered-lead alert: if a lead's first message has been waiting this many minutes
// with no bot reply, alert even if all cron jobs are healthy (catches bot downtime silently).
const UNANSWERED_LEAD_MINUTES = 30;

// v5 — DEAD ENGINE. On 17 Oct the Anthropic API ran out of credit at 09:00. Every cron kept
// firing and writing a healthy heartbeat (they ran fine — the model call inside them failed),
// and the unanswered-lead check never tripped because it only looks at brand-new leads that
// never got ANY agent message: the 91 failures were follow-ups to leads that already had
// replies, and the one new lead was answered by hand within four minutes. So the system was
// dead for three hours and reported itself perfectly healthy. The direct signal is the one
// nobody was reading: fadc.agent_runs rows with a non-null error. If that many of the most
// recent runs in the window all failed, the engine is down regardless of what else looks fine.
const ENGINE_WINDOW_MINUTES = 30;
const ENGINE_FAIL_STREAK = 3;

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

    // ── Unanswered-lead check ─────────────────────────────────────────────────
    // Find leads whose first customer message arrived >UNANSWERED_LEAD_MINUTES ago
    // but have never received a bot reply. This catches silent bot failures.
    type UnansweredRow = { lead_id: string; first_msg: string; wait_min: number; snippet: string };
    const unanswered = await sql<UnansweredRow[]>`
      select
        l.id as lead_id,
        min(m.created_at) as first_msg,
        round(extract(epoch from (now() - min(m.created_at))) / 60) as wait_min,
        left(min(m.text), 60) as snippet
      from fadc.leads l
      join fadc.messages m on m.lead_id = l.id and m.role = 'customer'
      where l.created_at > now() - interval '24 hours'
        -- v5: a lead the bot deliberately did not answer is not a failure. Every one of the
        -- three "unanswered leads" this check reported on 17 Oct was noise: an obscene troll
        -- (already closed_lost) and two people saying goodbye. Alerting on those teaches
        -- everyone to ignore the channel.
        and l.stage <> 'closed_lost'
        and not exists (
          select 1 from fadc.messages m2
          where m2.lead_id = l.id and m2.role = 'agent'
        )
      group by l.id
      having extract(epoch from (now() - min(m.created_at))) / 60 > ${UNANSWERED_LEAD_MINUTES}
         and min(m.text) !~* '(unsubscribe|stop messaging|no need|not interested|no thanks|thanks anyway|wrong number|^\\s*bye)'
    `;

    // ── Dead-engine check ─────────────────────────────────────────────────────
    // Look at the most recent agent_runs inside the window. If there are at least
    // ENGINE_FAIL_STREAK of them and every single one carries an error, the model call
    // is failing for everybody — out of credit, bad key, provider outage. The error text
    // goes straight into the alert so the cause is readable without opening the database.
    type EngineRow = { total: number; failed: number; last_error: string | null; oldest_min: number };
    const [engine] = await sql<EngineRow[]>`
      select
        count(*)::int as total,
        count(*) filter (where error is not null)::int as failed,
        (array_agg(error order by created_at desc) filter (where error is not null))[1] as last_error,
        coalesce(round(extract(epoch from (now() - min(created_at))) / 60), 0)::int as oldest_min
      from fadc.agent_runs
      where created_at > now() - make_interval(mins => ${ENGINE_WINDOW_MINUTES})
    `;
    const engineDown =
      !!engine && engine.total >= ENGINE_FAIL_STREAK && engine.failed === engine.total;

    // State machine: ok ↔ alerting, same as v1
    const [stateRow] = await sql`select value from fadc.settings where key = 'canary_state'`;
    let state: { status: "ok" | "alerting"; last_alert_at: string | null } = stateRow?.value
      ? JSON.parse(stateRow.value)
      : { status: "ok", last_alert_at: null };

    const nowIso = new Date().toISOString();
    let action = "none";
    let telegramSent = false;

    if (stale.length > 0 || unanswered.length > 0 || engineDown) {
      const shouldAlert =
        state.status === "ok" ||
        !state.last_alert_at ||
        (now - new Date(state.last_alert_at).getTime()) > REALERT_MINUTES * 60_000;

      if (shouldAlert) {
        const parts: string[] = [];
        if (engineDown) {
          // Trim the provider's JSON down to the human sentence when there is one.
          const raw = engine.last_error ?? "sin detalle";
          const msg = raw.match(/"message"\s*:\s*"([^"]+)"/)?.[1] ?? raw.slice(0, 200);
          parts.push(
            `🚨 EL BOT NO ESTÁ CONTESTANDO A NADIE\n` +
            `Los últimos ${engine.total} intentos fallaron, todos. Lleva ~${engine.oldest_min} min así.\n\n` +
            `Error: ${msg}\n\n` +
            `Si dice que no hay crédito, recarga en console.anthropic.com → Plans & Billing. ` +
            `El bot revive solo, no hay que desplegar nada.`
          );
        }
        if (stale.length > 0) {
          const jobLabels: Record<string, string> = {
            fadc_followups:        "Follow-ups a leads",
            fadc_ad_spend:         "Monitor de gasto en anuncios",
            fadc_closeout_ask:     "Pregunta de cierre al cliente",
            fadc_closeout_remind:  "Recordatorio de cierre",
            fadc_weekly_summary:   "Resumen semanal",
          };
          parts.push(
            `🔴 El bot tiene ${stale.length} tarea(s) caída(s):\n` +
            stale.map((s) => {
              const label = jobLabels[s.job] ?? s.job;
              // Extract minutes from reason like "last_ok 915 min ago (max 180)"
              const m = s.reason.match(/last_ok (\d+) min ago/);
              const since = m ? `hace ${Math.round(Number(m[1]) / 60)}h` : s.reason;
              return `  • ${label} — sin respuesta ${since}`;
            }).join("\n") +
            `\n\nNo necesitas hacer nada — me aviso solo cuando se recupere.`
          );
        }
        if (unanswered.length > 0) {
          parts.push(
            `⚠️ ${unanswered.length} lead(s) esperando respuesta del bot hace más de ${UNANSWERED_LEAD_MINUTES} min:\n` +
            unanswered.map((u) => `  • ${u.wait_min} min esperando — "${u.snippet}"`).join("\n") +
            `\nRevisa si el bot está caído.`
          );
        }
        telegramSent = await sendTelegram(parts.join("\n\n"));
        action = "alerted";
      } else {
        action = "already_alerting";
      }
      state = { status: "alerting", last_alert_at: shouldAlert ? nowIso : state.last_alert_at };
    } else if (state.status === "alerting") {
      telegramSent = await sendTelegram(
        `✅ Todo recuperado — el bot está funcionando normal y no hay leads sin respuesta.`,
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
      unanswered_leads: unanswered.length,
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
