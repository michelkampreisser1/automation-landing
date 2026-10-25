// fadc-twilio-webhook — receives inbound SMS replies to Michel's 617 Twilio number.
// build: v2 (25 Oct 2026) — el aviso de Telegram ahora guarda su message_id, para que
// responderlo le llegue al tecnico como SMS (lo maneja fadc-agent).
// v1 (21 Oct 2026)
//
// Right now the only outbound SMS this number sends is runTechnicianOutreach (fadc-agent),
// cold outreach to technician prospects in fadc.technicians. So inbound replies here are
// either: a STOP/opt-out keyword, or a technician prospect replying to the pitch.
//
// What it does NOT do: reply automatically to a technician. Negotiating a subcontractor
// split is a judgment call every time (zone, insurance, trial job) — this logs the reply
// and pings Telegram so Michel answers it himself, same as he asked.
//
// Auth: Twilio's X-Twilio-Signature header, verified with TWILIO_AUTH_TOKEN per Twilio's
// documented algorithm (HMAC-SHA1 of the exact webhook URL + sorted param key+value pairs).
// Deployed with --no-verify-jwt (Twilio does not send a Supabase JWT).

import postgres from "npm:postgres@3.4.4";

const sql = postgres(Deno.env.get("SUPABASE_DB_URL")!, { prepare: false, max: 2 });

const TWILIO_TOKEN = Deno.env.get("TWILIO_AUTH_TOKEN") ?? "";
const TG_TOKEN = Deno.env.get("TELEGRAM_BOT_TOKEN") ?? "";
const TG_CHAT = Deno.env.get("TELEGRAM_CHAT_ID") ?? "";
// Must match EXACTLY what's configured as the "A Message Comes In" webhook URL in the
// Twilio console for +16178125319 — no query string, no trailing slash difference.
const WEBHOOK_URL = Deno.env.get("FADC_TWILIO_WEBHOOK_URL")
  ?? "https://lvqeucpqgqcxsghqsemn.supabase.co/functions/v1/fadc-twilio-webhook";

const STOP_WORDS = new Set(["stop", "stopall", "stop all", "unsubscribe", "cancel", "quit", "end"]);

function twiml(body?: string): Response {
  const xml = body
    ? `<?xml version="1.0" encoding="UTF-8"?><Response><Message>${body.replace(/&/g, "&amp;").replace(/</g, "&lt;")}</Message></Response>`
    : `<?xml version="1.0" encoding="UTF-8"?><Response></Response>`;
  return new Response(xml, { status: 200, headers: { "Content-Type": "text/xml" } });
}

async function validSignature(params: Record<string, string>, signature: string | null): Promise<boolean> {
  if (!signature || !TWILIO_TOKEN) return false;
  let data = WEBHOOK_URL;
  for (const k of Object.keys(params).sort()) data += k + params[k];
  const key = await crypto.subtle.importKey(
    "raw", new TextEncoder().encode(TWILIO_TOKEN), { name: "HMAC", hash: "SHA-1" }, false, ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(data));
  const b64 = btoa(String.fromCharCode(...new Uint8Array(sig)));
  return b64 === signature;
}

// Devuelve el message_id de Telegram. Hace falta para que una respuesta a ese mensaje
// pueda rastrearse de vuelta al tecnico que escribio (ver tg_reply_msg_id).
async function notifyTeam(text: string): Promise<number | null> {
  if (!TG_TOKEN || !TG_CHAT) return null;
  try {
    const res = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ chat_id: TG_CHAT, text }),
    });
    const j = await res.json().catch(() => null);
    const id = j?.result?.message_id;
    return typeof id === "number" ? id : null;
  } catch (_) {
    return null; /* best-effort */
  }
}

Deno.serve(async (req) => {
  try {
    if (req.method !== "POST") return new Response("ok", { status: 200 });

    const raw = await req.text();
    const form = new URLSearchParams(raw);
    const params: Record<string, string> = {};
    for (const [k, v] of form.entries()) params[k] = v;

    const signature = req.headers.get("X-Twilio-Signature");
    if (!(await validSignature(params, signature))) {
      console.error("fadc-twilio-webhook: bad signature", { signature });
      return new Response("forbidden", { status: 403 });
    }

    const from = (params["From"] ?? "").trim();
    const body = (params["Body"] ?? "").trim();
    const normalized = body.toLowerCase().trim();

    if (!from) return twiml();

    if (STOP_WORDS.has(normalized)) {
      await sql`update fadc.technicians set opted_out = true, last_reply_at = now(), reply_text = ${body} where phone = ${from}`;
      await notifyTeam(`🛑 Technician opt-out: ${from} replied "${body}". Marked opted_out, no more texts to this number.`);
      return twiml("You've been unsubscribed and won't receive more texts from Vonkam Home Services.");
    }

    const [tech] = await sql<{ id: string; name: string }[]>`
      select id, name from fadc.technicians where phone = ${from} limit 1`;

    if (tech) {
      await sql`update fadc.technicians set replied_at = now(), last_reply_at = now(), reply_text = ${body}, stage = case when stage = 'prospect' then 'replied' else stage end where id = ${tech.id}`;
      // El aviso lleva su propio message_id guardado: responder a ESTE mensaje en Telegram
      // le manda un SMS al tecnico desde el 617. Sin esto el aviso era un callejon sin
      // salida — llegaba la respuesta y no habia por donde contestarla.
      const msgId = await notifyTeam(
        `💬 Technician reply — ${tech.name} (${from}): "${body}"\n` +
        `Reply to THIS message in Telegram and he gets it as a text from the 617.`);
      if (msgId) {
        await sql`update fadc.technicians set tg_reply_msg_id = ${msgId} where id = ${tech.id}`;
      }
    } else {
      await notifyTeam(`💬 SMS reply from unknown number ${from}: "${body}"`);
    }

    return twiml();
  } catch (e) {
    console.error("fadc-twilio-webhook error", e);
    return twiml();
  }
});
