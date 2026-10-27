// fadc-sms-console — el lugar donde Michel ve y contesta los SMS de tecnicos.
//
// Pedido el 26 oct 2026: "un lugar donde pueda contestar y ver que ponen, para ver quien si
// esta interesado y a quien podemos calificar". Hasta ese dia las respuestas vivian (cuando
// vivian) en una sola columna con el ultimo mensaje, y las de Paula McCarthy se perdieron
// completas. El hilo real esta ahora en fadc.sms_log; esto es la ventana para leerlo y
// contestarlo desde el telefono.
//
// Seguridad:
// - Llave en la URL (?key=...), comparada contra fadc.settings 'sms_console_key'. Mismo
//   patron que el dashboard. Si la llave se filtra: update fadc.settings y listo.
// - Solo manda SMS a telefonos que YA existen en fadc.technicians o en el hilo. El console
//   no es un relay abierto: con la llave robada no se puede textear a numeros arbitrarios.
// - NUNCA manda a un telefono con opted_out = true. Eso es ley (TCPA), no cortesia.

import postgres from "npm:postgres@3.4.4";

const sql = postgres(Deno.env.get("SUPABASE_DB_URL")!, { prepare: false, max: 2 });
const TWILIO_SID = Deno.env.get("TWILIO_ACCOUNT_SID") ?? "";
const TWILIO_TOKEN = Deno.env.get("TWILIO_AUTH_TOKEN") ?? "";
const TWILIO_SMS_FROM = Deno.env.get("TWILIO_SMS_FROM") ?? "+16178125319";

async function keyOk(key: string | null): Promise<boolean> {
  if (!key || key.length < 20) return false;
  const [row] = await sql<{ value: string }[]>`
    select value from fadc.settings where key = 'sms_console_key'`;
  return !!row?.value && row.value === key;
}

async function sendSMS(to: string, body: string): Promise<{ ok: boolean; sid?: string; error?: string }> {
  if (!TWILIO_SID || !TWILIO_TOKEN) return { ok: false, error: "twilio creds not set" };
  try {
    const params = new URLSearchParams({ From: TWILIO_SMS_FROM, To: to, Body: body });
    const res = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${TWILIO_SID}/Messages.json`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization: `Basic ${btoa(`${TWILIO_SID}:${TWILIO_TOKEN}`)}`,
      },
      body: params.toString(),
    });
    const json = await res.json();
    return res.ok ? { ok: true, sid: json.sid } : { ok: false, error: json.message ?? `HTTP ${res.status}` };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  }
}

const esc = (x: unknown) =>
  String(x ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

const SHELL = (inner: string) => `<!doctype html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SMS Técnicos</title>
<style>
  :root { --ink:#0B2545; --line:#d8dee9; --bg:#f6f8fb; --in:#ffffff; --out:#0B2545; }
  * { box-sizing:border-box }
  body { margin:0; background:var(--bg); color:var(--ink);
         font:15px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif }
  .wrap { max-width:640px; margin:0 auto; padding:16px 12px 90px }
  h1 { font-size:19px; margin:4px 0 14px } h1 a { color:inherit; text-decoration:none }
  .th { display:block; background:#fff; border:1px solid var(--line); border-radius:10px;
        padding:12px 14px; margin-bottom:8px; text-decoration:none; color:inherit }
  .th b { font-size:15px } .th .m { color:#5a6b85; font-size:13px; margin-top:3px;
        white-space:nowrap; overflow:hidden; text-overflow:ellipsis }
  .tags { margin-top:5px } 
  .tag { display:inline-block; font-size:11px; padding:2px 8px; border-radius:999px;
         background:#e8eef7; color:#2a4a77; margin-right:5px }
  .tag.rojo { background:#fdeaea; color:#9c2626 } .tag.verde { background:#e8f5ec; color:#1d6b38 }
  .tag.ambar { background:#fdf3e0; color:#8a5a00 }
  .bub { max-width:84%; padding:9px 13px; border-radius:14px; margin:5px 0; white-space:pre-wrap;
         word-break:break-word }
  .in  { background:var(--in); border:1px solid var(--line); border-bottom-left-radius:4px }
  .out { background:var(--out); color:#fff; margin-left:auto; border-bottom-right-radius:4px }
  .when { font-size:11px; color:#78879e; margin:0 4px 10px } .when.r { text-align:right }
  form.reply { position:fixed; bottom:0; left:0; right:0; background:var(--bg);
         border-top:1px solid var(--line); padding:10px 12px; display:flex; gap:8px }
  form.reply textarea { flex:1; max-width:520px; margin-left:auto; padding:10px 12px;
         border:1px solid var(--line); border-radius:10px; font:inherit; resize:none; height:46px }
  form.reply button { padding:0 18px; border:0; border-radius:10px; background:var(--out);
         color:#fff; font-weight:600; font-size:15px }
  .aviso { background:#fdf3e0; border:1px solid #efd9a7; color:#6b4b00; border-radius:10px;
         padding:10px 13px; margin:10px 0; font-size:14px }
  .ok { background:#e8f5ec; border-color:#bfe2cb; color:#1d6b38 }
  .err { background:#fdeaea; border-color:#f3c4c4; color:#9c2626 }
  .ficha { background:#fff; border:1px solid var(--line); border-radius:10px; padding:10px 14px;
         margin-bottom:12px; font-size:13px; color:#44546b }
  @media (prefers-color-scheme: dark) {
    :root:not([data-theme="light"]) { --ink:#e8edf5; --line:#2b3950; --bg:#0d1420; --in:#141d2d; --out:#2f6df6 }
    :root:not([data-theme="light"]) .th, :root:not([data-theme="light"]) .ficha { background:#141d2d }
    :root:not([data-theme="light"]) form.reply textarea { background:#141d2d; color:#e8edf5 }
  }
</style></head><body><div class="wrap">${inner}</div></body></html>`;

function fechaET(d: Date): string {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", month: "short", day: "numeric",
    hour: "numeric", minute: "2-digit",
  }).format(d) + " ET";
}

Deno.serve(async (req: Request) => {
  const url = new URL(req.url);
  const html = (b: string, status = 200) =>
    new Response(SHELL(b), { status, headers: { "Content-Type": "text/html; charset=utf-8" } });

  // ── Enviar ────────────────────────────────────────────────────────────────
  if (req.method === "POST") {
    const form = await req.formData();
    const key = String(form.get("key") ?? "");
    if (!(await keyOk(key))) return html(`<h1>SMS Técnicos</h1><div class="aviso err">Llave inválida.</div>`, 403);
    const phone = String(form.get("phone") ?? "").trim();
    const body = String(form.get("body") ?? "").trim().slice(0, 1200);
    const back = `?key=${encodeURIComponent(key)}&phone=${encodeURIComponent(phone)}`;
    if (!/^\+1\d{10}$/.test(phone) || !body) {
      return html(`<div class="aviso err">Falta el mensaje o el teléfono no es válido.</div><a href="${back}">Volver</a>`, 400);
    }
    // Solo numeros conocidos, y jamas a un opt-out.
    const [known] = await sql<{ opted: boolean | null }[]>`
      select bool_or(coalesce(t.opted_out, false)) as opted
        from (select ${phone}::text as p) x
        left join fadc.technicians t on t.phone = x.p
       where t.id is not null
          or exists (select 1 from fadc.sms_log l where l.phone = x.p)`;
    if (!known) return html(`<div class="aviso err">Ese número no está en el sistema. El console solo escribe a técnicos registrados.</div><a href="?key=${encodeURIComponent(key)}">Volver</a>`, 400);
    if (known.opted) return html(`<div class="aviso err">Ese número pidió STOP. No se le puede textear — es ley, no preferencia.</div><a href="${back}">Volver al hilo</a>`, 400);

    const send = await sendSMS(phone, body);
    if (send.ok) {
      await sql`insert into fadc.sms_log (phone, tech_id, direction, body, sent_by, twilio_sid)
                select ${phone}, t.id, 'out', ${body}, 'michel_console', ${send.sid ?? null}
                  from (values (1)) as _
                  left join fadc.technicians t on t.phone = ${phone}`;
      return new Response(null, { status: 303, headers: { Location: back } });
    }
    return html(`<div class="aviso err">Twilio no lo aceptó: ${esc(send.error)}</div><a href="${back}">Volver</a>`, 502);
  }

  // ── Leer ──────────────────────────────────────────────────────────────────
  const key = url.searchParams.get("key");
  if (!(await keyOk(key))) {
    return html(`<h1>SMS Técnicos</h1><div class="aviso err">Falta la llave. Pide el link completo.</div>`, 403);
  }
  const phone = url.searchParams.get("phone");

  if (phone) {
    const msgs = await sql<{ direction: string; body: string; sent_by: string | null; created_at: Date }[]>`
      select direction, body, sent_by, created_at from fadc.sms_log
       where phone = ${phone} order by created_at asc limit 200`;
    const [t] = await sql<{ name: string; base_town: string | null; stage: string; zone: string | null;
                            radius_miles: number | null; has_insurance: boolean; opted_out: boolean }[]>`
      select name, base_town, stage, zone, radius_miles, has_insurance, opted_out
        from fadc.technicians where phone = ${phone}`;
    const burbujas = msgs.map((m) =>
      `<div class="bub ${m.direction === "in" ? "in" : "out"}">${esc(m.body)}</div>` +
      `<div class="when ${m.direction === "in" ? "" : "r"}">${fechaET(m.created_at)}${m.sent_by ? ` · ${esc(m.sent_by)}` : ""}</div>`
    ).join("");
    const ficha = t
      ? `<div class="ficha"><b>${esc(t.name)}</b> · ${esc(t.base_town ?? "?")} · etapa ${esc(t.stage)}` +
        `${t.radius_miles ? ` · viaja ${t.radius_miles} mi` : ""}${t.has_insurance ? " · con seguro" : ""}` +
        `${t.opted_out ? ' · <b style="color:#9c2626">OPT-OUT</b>' : ""}</div>`
      : `<div class="ficha">Número sin ficha de técnico.</div>`;
    const reply = t?.opted_out
      ? `<div class="aviso err">Pidió STOP — no se le puede escribir.</div>`
      : `<form class="reply" method="POST" action="">
           <input type="hidden" name="key" value="${esc(key)}">
           <input type="hidden" name="phone" value="${esc(phone)}">
           <textarea name="body" placeholder="Responder como Vonkam (sale del 617)…" required></textarea>
           <button type="submit">Enviar</button></form>`;
    return html(`<h1><a href="?key=${encodeURIComponent(key!)}">← Hilos</a></h1>${ficha}${burbujas || '<div class="aviso">Sin mensajes todavía.</div>'}${reply}`);
  }

  const hilos = await sql<{ phone: string; name: string | null; base_town: string | null;
      stage: string | null; opted_out: boolean | null; reply_intent: string | null;
      last_body: string; last_dir: string; last_at: Date; n: number }[]>`
    select l.phone, t.name, t.base_town, t.stage, t.opted_out, t.reply_intent,
           (array_agg(l.body order by l.created_at desc))[1] as last_body,
           (array_agg(l.direction order by l.created_at desc))[1] as last_dir,
           max(l.created_at) as last_at, count(*)::int as n
      from fadc.sms_log l
      left join fadc.technicians t on t.id = l.tech_id or t.phone = l.phone
     group by l.phone, t.name, t.base_town, t.stage, t.opted_out, t.reply_intent
     order by (max(l.created_at) filter (where l.direction = 'in')) desc nulls last,
              max(l.created_at) desc
     limit 100`;
  const lista = hilos.map((h) => {
    const tags =
      (h.last_dir === "in" && !h.opted_out ? `<span class="tag ambar">sin responder</span>` : "") +
      (h.opted_out ? `<span class="tag rojo">opt-out</span>` : "") +
      (h.reply_intent === "interested" ? `<span class="tag verde">interesado</span>` : "") +
      (h.stage ? `<span class="tag">${esc(h.stage)}</span>` : "") +
      (h.base_town ? `<span class="tag">${esc(h.base_town)}</span>` : "");
    return `<a class="th" href="?key=${encodeURIComponent(key!)}&phone=${encodeURIComponent(h.phone)}">
      <b>${esc(h.name ?? h.phone)}</b>
      <div class="m">${h.last_dir === "in" ? "⬅︎ " : "➡︎ "}${esc(h.last_body.slice(0, 90))}</div>
      <div class="tags">${tags}<span class="tag">${fechaET(h.last_at)}</span></div></a>`;
  }).join("");
  return html(`<h1>SMS Técnicos</h1>${lista || '<div class="aviso">Sin conversaciones todavía.</div>'}`);
});
