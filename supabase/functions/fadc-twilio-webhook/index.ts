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

// 26 oct 2026: antes era un Set y se comparaba con el mensaje COMPLETO, asi que solo
// cachaba un SMS cuyo texto fuera exactamente "stop".
//
// Paula S. de McCarthy Cleaning (+16038861867, Hudson NH) escribio dos veces
// "Paula S. from McCarthy Cleaning Co.: stop". Un Set no la cacha: habria quedado
// archivada como respuesta normal y el opt-out se pierde. Muchos negocios textean desde
// apps o servicios de recados que anteponen el nombre de quien escribe, asi que el
// prefijo es lo normal, no la excepcion.
//
// Ahora se acepta la palabra al final del mensaje, con o sin prefijo. Deliberadamente
// permisivo: marcar de mas a alguien que no queria salirse cuesta un prospecto; marcar de
// menos a alguien que SI pidio salirse cuesta un reporte de spam y el numero. Los limites
// de palabra evitan los falsos amigos obvios ("weekend" y "send" no disparan "end").
const STOP_RE =
  /(?:^|[:\-\u2013\u2014]\s*|\s)(stop\s?all|stop|unsubscribe|cancel|quit|end)\s*[.!]?\s*$/i;

function esOptOut(texto: string): boolean {
  return STOP_RE.test(texto.trim());
}

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
      // 26 oct 2026: ESTE 403 ERA EL UNICO CAMINO MUDO DE TODA LA FUNCION.
      //
      // McCarthy Cleaning Company (+16038861867, Hudson NH) recibio el SMS del outreach a
      // las 10:30:01 ET y contesto DOS VECES a la 1:08 PM. Twilio lo registro como
      // "Received". En la base quedo last_reply_at nulo, reply_text nulo y stage en
      // 'prospect'. Michel no vio nada. De 45 SMS mandados desde el 22 oct, la base decia
      // cero respuestas y cero opt-outs -- un cero perfecto que parecia desinteres y era
      // esto.
      //
      // Todos los demas caminos avisan a Telegram, incluso cuando el numero es desconocido.
      // Solo este se iba en silencio, que es justo el que falla cuando la URL configurada en
      // Twilio no coincide EXACTO con WEBHOOK_URL (y el numero vive en un Messaging Service,
      // asi que el webhook se configura en el servicio, no en el numero).
      //
      // Ahora grita. Limitado a un aviso por hora para que un bot pegandole al endpoint no
      // convierta esto en otra fuente de ruido.
      console.error("fadc-twilio-webhook: bad signature", { signature: !!signature });
      try {
        const [reciente] = await sql<{ hay: boolean }[]>`
          select true as hay from fadc.notifications
           where kind = 'twilio_bad_signature' and created_at > now() - interval '1 hour'
           limit 1`;
        if (!reciente) {
          const motivo = !TWILIO_TOKEN
            ? "falta TWILIO_AUTH_TOKEN en los secretos de Supabase"
            : !signature
            ? "la peticion llego SIN cabecera X-Twilio-Signature"
            : `la firma no cuadra: la URL configurada en Twilio no coincide con ${WEBHOOK_URL}`;
          const texto =
            `\u{1F6A8} SMS ENTRANTE RECHAZADO\n` +
            `Alguien escribio al 617 y lo tiramos sin leerlo.\n` +
            `De: ${params["From"] ?? "desconocido"}\n` +
            `Motivo: ${motivo}\n` +
            `Revisa el webhook del Messaging Service en Twilio (no el del numero).`;
          await notifyTeam(texto);
          await sql`insert into fadc.notifications (kind, text, delivered_telegram)
                    values ('twilio_bad_signature', ${texto}, true)`;
        }
      } catch (_) { /* el aviso nunca bloquea la respuesta a Twilio */ }
      return new Response("forbidden", { status: 403 });
    }

    const from = (params["From"] ?? "").trim();
    const body = (params["Body"] ?? "").trim();
    const normalized = body.toLowerCase().trim();

    if (!from) return twiml();

    // 26 oct: todo entrante queda en el hilo (fadc.sms_log), pase lo que pase despues.
    // Es la materia prima del console de Michel y la razon de que una respuesta ya no
    // pueda evaporarse aunque una rama posterior falle.
    try {
      await sql`insert into fadc.sms_log (phone, tech_id, direction, body, sent_by, twilio_sid)
                select ${from}, t.id, 'in', ${body}, 'webhook', ${params["MessageSid"] ?? null}
                  from (values (1)) as _
                  left join fadc.technicians t on t.phone = ${from}`;
    } catch (e) { console.error("sms_log insert:", e instanceof Error ? e.message : String(e)); }

    if (esOptOut(normalized)) {
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
