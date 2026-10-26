// fadc-tech-signup — alta de tecnicos desde la web.
//
// Por que existe: al 26 oct 2026 el unico canal para reclutar tecnicos era SMS en frio, y
// de 45 mensajes mandados desde el 22 de oct no llego UNA sola respuesta, ni un STOP. Ese
// cero absoluto apunta a que los mensajes ni siquiera se entregan (filtrado de operadora por
// registro A2P 10DLC), asi que el canal entero puede estar muerto sin que nadie lo supiera.
//
// Ademas el tabulador no refleja la realidad: Ilay y "Tech Ivy" trabajan con Vonkam y no
// existen como filas, porque nunca hubo donde darlos de alta.
//
// Esto resuelve las dos cosas con una URL que la gente de Michel en Boston puede mandar por
// WhatsApp, y que recoge de una vez lo que siempre falta: ZIP base, cuantas millas viaja,
// que servicios hace y si tiene seguro.
//
// Seguridad: el formulario NUNCA decide stage, active ni type. Entra siempre como
// stage='prospect', source='web_signup', type='subcontractor'. Si alguien spamea, las filas
// quedan marcadas por source y se borran de un SQL.

import postgres from "npm:postgres@3.4.4";

const DB_URL = Deno.env.get("SUPABASE_DB_URL") ?? "";
const TG_TOKEN = Deno.env.get("TELEGRAM_BOT_TOKEN") ?? "";
const TG_CHAT = Deno.env.get("TELEGRAM_CHAT_ID") ?? "";

const sql = postgres(DB_URL, { prepare: false });

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "content-type",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
};

async function notifyTeam(text: string) {
  if (!TG_TOKEN || !TG_CHAT) return;
  try {
    await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: TG_CHAT, text }),
    });
  } catch { /* el alta nunca falla por el aviso */ }
}

// Mismo mapeo de zonas que usa fadc-tech-sourcing, para que el prospecto caiga en la cola
// correcta desde el primer dia en vez de quedarse sin zona.
function zoneFor(state: string, zip: string): string {
  if (state === "RI") return "providence";
  if (state === "CT") return "hartford";
  if (state === "NH") return "other";
  if (state === "NJ" || state === "NY") return "nj";
  if (state === "MA") {
    const n = Number(zip.slice(0, 3));
    if (n >= 15 && n <= 16) return "worcester";   // 015xx-016xx Worcester
    if (n >= 27 && n <= 28) return "providence";  // 027xx-028xx sur, mas cerca de Providence
    return "boston";
  }
  return "other";
}

const PAGE = (msg = "", ok = false) => `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Work With Vonkam</title>
<style>
  :root { --ink:#0B2545; --line:#d8dee9; --bg:#f6f8fb; --accent:#0B2545; }
  * { box-sizing:border-box }
  body { margin:0; background:var(--bg); color:var(--ink);
         font:16px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }
  .wrap { max-width:560px; margin:0 auto; padding:32px 16px 64px }
  h1 { font-size:26px; margin:0 0 6px; letter-spacing:-.01em }
  .sub { color:#5a6b85; margin:0 0 24px }
  .card { background:#fff; border:1px solid var(--line); border-radius:12px; padding:22px 20px }
  label { display:block; font-weight:600; font-size:14px; margin:16px 0 6px }
  .hint { font-weight:400; color:#78879e; font-size:13px }
  input[type=text], input[type=tel], input[type=email], input[type=number], textarea, select {
    width:100%; padding:11px 12px; border:1px solid var(--line); border-radius:8px;
    font-size:16px; font-family:inherit; background:#fff; color:var(--ink) }
  textarea { min-height:76px; resize:vertical }
  .row { display:flex; gap:12px } .row > div { flex:1 }
  .checks { display:flex; flex-wrap:wrap; gap:14px; margin-top:8px }
  .checks label { display:flex; align-items:center; gap:7px; font-weight:400; margin:0; cursor:pointer }
  input[type=checkbox] { width:17px; height:17px; accent-color:var(--accent); margin:0 }
  button { width:100%; margin-top:24px; padding:14px; border:0; border-radius:8px;
           background:var(--accent); color:#fff; font-size:16px; font-weight:600; cursor:pointer }
  button:hover { opacity:.92 }
  .note { margin-top:18px; font-size:13px; color:#78879e }
  .msg { padding:13px 15px; border-radius:8px; margin-bottom:20px; font-size:15px }
  .err { background:#fdeaea; color:#9c2626; border:1px solid #f3c4c4 }
  .ok  { background:#e8f5ec; color:#1d6b38; border:1px solid #bfe2cb }
  .hp { position:absolute; left:-9999px }
  .terms { font-size:13px; color:#78879e; margin-top:14px; line-height:1.45 }
  @media (prefers-color-scheme: dark) {
    :root:not([data-theme="light"]) { --ink:#e8edf5; --line:#2b3950; --bg:#0d1420 }
    :root:not([data-theme="light"]) body { background:#0d1420 }
    :root:not([data-theme="light"]) .card { background:#141d2d }
    :root:not([data-theme="light"]) input, :root:not([data-theme="light"]) textarea,
    :root:not([data-theme="light"]) select { background:#0d1420; color:#e8edf5 }
    :root:not([data-theme="light"]) button { background:#2f6df6 }
  }
</style></head>
<body><div class="wrap">
<h1>Work with Vonkam Home Services</h1>
<p class="sub">We have duct and dryer vent jobs already sold and scheduled across MA, NH, CT and RI, and we need subcontractors to run them. The customer pays you on site the same day and you keep 60%.</p>
${msg ? `<div class="msg ${ok ? "ok" : "err"}">${msg}</div>` : ""}
${ok ? "" : `
<form class="card" method="POST" action="">
  <input class="hp" name="website" tabindex="-1" autocomplete="off" aria-hidden="true">

  <label>Your name <span class="hint">required</span>
    <input type="text" name="name" required maxlength="120" autocomplete="name"></label>

  <label>Company <span class="hint">if you have one</span>
    <input type="text" name="company" maxlength="120" autocomplete="organization"></label>

  <div class="row">
    <div><label>Phone <span class="hint">required</span>
      <input type="tel" name="phone" required maxlength="25" autocomplete="tel" placeholder="(617) 555-0199"></label></div>
    <div><label>Email
      <input type="email" name="email" maxlength="160" autocomplete="email"></label></div>
  </div>

  <div class="row">
    <div><label>ZIP you work out of <span class="hint">required</span>
      <input type="text" name="base_zip" required inputmode="numeric" maxlength="5" pattern="[0-9]{5}" placeholder="02478"></label></div>
    <div><label>Miles you'll travel
      <input type="number" name="radius_miles" min="1" max="200" placeholder="40"></label></div>
  </div>

  <label>What you do</label>
  <div class="checks">
    <label><input type="checkbox" name="services" value="ducts"> Air ducts</label>
    <label><input type="checkbox" name="services" value="dryer_vent"> Dryer vents</label>
    <label><input type="checkbox" name="services" value="chimney"> Chimneys</label>
  </div>

  <label>Equipment and insurance</label>
  <div class="checks">
    <label><input type="checkbox" name="has_equipment" value="1"> I have my own equipment</label>
    <label><input type="checkbox" name="has_insurance" value="1"> I carry liability insurance</label>
  </div>

  <label>Anything else <span class="hint">crew size, years doing this, areas you know well</span>
    <textarea name="notes" maxlength="900"></textarea></label>

  <button type="submit">Send</button>

  <p class="terms">By sending this you agree we may text or call you about work. Reply STOP to any text and we won't message you again.</p>
</form>`}
<p class="note">Vonkam Home Services &middot; michel@vonkam.com &middot; (617) 812-5319</p>
</div></body></html>`;

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });

  const html = (body: string, status = 200) =>
    new Response(body, { status, headers: { ...CORS, "Content-Type": "text/html; charset=utf-8" } });

  if (req.method === "GET") return html(PAGE());
  if (req.method !== "POST") return html(PAGE("Method not allowed."), 405);

  try {
    const form = await req.formData();
    const s = (k: string) => String(form.get(k) ?? "").trim();

    // Trampa para bots: un campo invisible que un humano nunca llena. Se responde 200 con la
    // pagina de exito para no ensenarle al bot que fue detectado.
    if (s("website")) return html(PAGE("Thanks — we'll be in touch.", true));

    const name = s("name").slice(0, 120);
    const company = s("company").slice(0, 120);
    const digits = s("phone").replace(/\D/g, "");
    const zip = s("base_zip").replace(/\D/g, "");

    if (!name) return html(PAGE("Please tell us your name."), 400);
    if (digits.length < 10 || digits.length > 11)
      return html(PAGE("That phone number doesn't look right — 10 digits, please."), 400);
    if (!/^\d{5}$/.test(zip))
      return html(PAGE("We need a 5-digit ZIP code for the area you work out of."), 400);

    const phone = "+1" + digits.slice(-10);

    const [geo] = await sql<{ city: string; state: string }[]>`
      select city, state from fadc.zip_geo where zip = ${zip}`;
    if (!geo) return html(PAGE("We couldn't find that ZIP code. Please check it."), 400);

    const services = form.getAll("services").map(String).filter((x) =>
      ["ducts", "dryer_vent", "chimney"].includes(x));
    const radiusRaw = Number(s("radius_miles"));
    const radius = Number.isFinite(radiusRaw) && radiusRaw > 0 && radiusRaw <= 200
      ? Math.round(radiusRaw) : null;

    // Un alta repetida no crea una fila nueva: actualiza la que ya existe. Pero NUNCA toca
    // stage ni active, para que un tecnico ya aprobado no se degrade a prospecto por volver
    // a llenar el formulario.
    const [row] = await sql<{ id: string; created: boolean }[]>`
      insert into fadc.technicians
        (name, phone, email, base_zip, base_town, radius_miles, services,
         has_equipment, has_insurance, notes, source, zone, type, stage)
      values (
        ${company ? `${name} (${company})` : name},
        ${phone},
        ${s("email").slice(0, 160) || null},
        ${zip},
        ${`${geo.city} ${geo.state}`},
        ${radius},
        ${services},
        ${!!form.get("has_equipment")},
        ${!!form.get("has_insurance")},
        ${s("notes").slice(0, 900) || null},
        'web_signup',
        ${zoneFor(geo.state, zip)},
        'subcontractor',
        'prospect'
      )
      on conflict (phone) do update set
        name            = excluded.name,
        email           = coalesce(excluded.email, fadc.technicians.email),
        base_zip        = excluded.base_zip,
        base_town       = excluded.base_town,
        radius_miles    = coalesce(excluded.radius_miles, fadc.technicians.radius_miles),
        services        = case when cardinality(excluded.services) > 0
                               then excluded.services else fadc.technicians.services end,
        has_equipment   = excluded.has_equipment or fadc.technicians.has_equipment,
        has_insurance   = excluded.has_insurance or fadc.technicians.has_insurance,
        notes           = coalesce(excluded.notes, fadc.technicians.notes),
        updated_at      = now()
      returning id, (xmax = 0) as created`;

    await notifyTeam(
      `\u{1F527} ALTA DE TECNICO desde la web\n` +
      `${company ? `${name} (${company})` : name}\n` +
      `${phone}${s("email") ? ` · ${s("email")}` : ""}\n` +
      `${geo.city} ${geo.state} ${zip}${radius ? ` · viaja ${radius} mi` : ""}\n` +
      `${services.length ? services.join(", ") : "no dijo servicios"}` +
      `${form.get("has_insurance") ? " · dice tener seguro" : ""}` +
      `${form.get("has_equipment") ? " · equipo propio" : ""}\n` +
      (s("notes") ? `"${s("notes").slice(0, 200)}"\n` : "") +
      (row?.created ? "" : "(ya estaba en la base, se actualizo su ficha)"),
    );

    return html(PAGE(
      "Thanks — we got it. We'll reach out about the next job in your area.", true));
  } catch (e) {
    console.error("fadc-tech-signup:", e instanceof Error ? e.message : String(e));
    return html(PAGE("Something went wrong on our end. Try again in a minute."), 500);
  }
});
