// fadc-tech-sourcing — el agente que llena la cola de técnicos solo.
// build: v5 (24 Oct 2026) — descarta las fichas SEO de revendedores de leads antes de que
// entren a la cola (ver looksLikeLeadReseller más abajo).
// v4 (23 Oct 2026) — apunta a las zonas HUECO: donde Afek e Ilay no
// llegan (sur de NH, Merrimack Valley, North Shore, sureste MA/Cape, Worcester,
// Springfield, CT fuera de Hartford). Esas búsquedas van con zone='other' y se
// corren primero. NJ ya no se atiende: fuera de SERVED_STATES. La cola que
// decide si gastar en Apify cuenta sólo prospectos de zona hueco, para que los
// cientos de Hartford/Boston sin textear no apaguen la búsqueda donde sí falta.
// v3 (22 Oct 2026) — {"action":"check"} valida el token y reporta su forma.
//
// POR QUÉ DOS FASES: un crawl de Apify tarda varios minutos y una edge function
// no vive tanto. Así que una invocación ARRANCA el run y una posterior lo COBRA.
// El cron corre cada pocas horas; cada vez la función primero mira si hay un run
// pendiente que ya terminó, y sólo si no hay arranca uno nuevo. Se auto-repara.
//
// POR QUÉ UN TOPE DE COLA: sin esto el agente acumularía miles de prospectos que
// el outreach nunca alcanza a textear (va a ~15/día). Es un grifo, no una manguera.
//
// FUENTES: sólo Google Maps y Yelp publican teléfonos reales.
//
// Auth: mismo header x-fadc-cron + fadc.settings['cron_secret'] que los demás crons.
// Desplegada con verify_jwt=false — pg_cron la llama sin JWT.

import postgres from "npm:postgres@3.4.4";

const DB_URL = Deno.env.get("SUPABASE_DB_URL") ?? "";
const APIFY_TOKEN_RAW = Deno.env.get("APIFY_TOKEN") ?? "";
const APIFY_TOKEN = APIFY_TOKEN_RAW.trim();
const TG_TOKEN = Deno.env.get("TELEGRAM_BOT_TOKEN") ?? "";
const TG_CHAT = Deno.env.get("TELEGRAM_CHAT_ID") ?? "";

const ACTOR = "compass~crawler-google-places";
const APIFY_API = "https://api.apify.com/v2";

// Zona que marca "hueco de cobertura". El check de la tabla sólo admite
// boston/providence/hartford/worcester/nj/other, así que NH y los pueblos
// lejanos de MA/CT van como 'other'.
const GAP_ZONE = "other";

const DEFAULTS = {
  queue_threshold: 120,   // si hay >= esto sin textear EN ZONA HUECO, no gastamos
  queries_per_run: 6,
  results_per_query: 15,
};

const GOOD_CATS = new Set([
  "Air duct cleaning service",
  "HVAC contractor",
  "Dryer vent cleaning service",
  "Air conditioning contractor",
  "Air conditioning repair service",
  "Furnace repair service",
]);

const ZONE_STATES: Record<string, string[]> = {
  boston: ["MA"],
  worcester: ["MA"],
  providence: ["RI", "MA"],
  hartford: ["CT"],
  other: ["MA", "NH", "CT", "RI"],
};

const STATE_NAMES: Record<string, string> = {
  massachusetts: "MA", "rhode island": "RI", connecticut: "CT", "new jersey": "NJ",
  "new hampshire": "NH", "new york": "NY", pennsylvania: "PA", texas: "TX", virginia: "VA",
  maine: "ME", vermont: "VT",
};

// Estados donde se reclutan técnicos. NJ fuera (23 oct). NH dentro: el sur de
// NH es donde Afek e Ilay no llegan y ya entran leads (Hollis, Nashua).
const SERVED_STATES = new Set(["MA", "RI", "CT", "NH"]);
const STATE_ZONE: Record<string, string> = { CT: "hartford", NH: GAP_ZONE };

function resolveZone(queryZone: string, st: string): string | null {
  if (!SERVED_STATES.has(st)) return null;
  if ((ZONE_STATES[queryZone] ?? []).includes(st)) return queryZone;
  return STATE_ZONE[st] ?? GAP_ZONE;
}

function normState(state: string, address: string): string {
  const s = (state ?? "").trim().toLowerCase();
  if (STATE_NAMES[s]) return STATE_NAMES[s];
  if (/^[a-z]{2}$/.test(s)) return s.toUpperCase();
  const m = (address ?? "").match(/\b([A-Z]{2})\s+\d{5}/);
  return m ? m[1] : "";
}

function last10(phone: unknown): string | null {
  const d = String(phone ?? "").replace(/\D/g, "");
  return d.length >= 10 ? d.slice(-10) : null;
}

// Los 8xx son centrales de franquicias: no reciben SMS y no son el técnico.
function isTollFree(p10: string): boolean {
  return /^8(00|33|44|55|66|77|88)/.test(p10);
}

// Fichas SEO de revendedores de leads disfrazadas de negocio local. Verificado a mano el
// 24 oct contra 216 prospectos: 49 coincidieron y las 49 eran basura — ningún negocio real
// quedó atrapado. Dos redes:
//   1. el nombre termina en "<Pueblo>, <ST>" — "VSM Dryer Vent Cleaning Services Nashua, NH"
//   2. una palabra inventada + "Air vent cleaning service" — Hovlor, Peakjax, Pipmax, TopCrew
// Si estos contestan el SMS es para vendernos leads, no para correr un trabajo. Se filtran
// aquí, en la fuente, para no volver a llenar la cola con ellos.
const RESELLER_RE =
  /,\s*(MA|NH|CT|RI)\.?\s*(\d{5})?\s*$|air vent cleaning servic|(media|marketing|seo)\b/i;
function looksLikeLeadReseller(name: string): boolean {
  return RESELLER_RE.test(name);
}

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

async function apify(path: string, init: RequestInit = {}, timeoutMs = 20_000) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    const res = await fetch(`${APIFY_API}${path}`, {
      ...init,
      signal: ctl.signal,
      headers: {
        ...(init.headers ?? {}),
        authorization: `Bearer ${APIFY_TOKEN}`,
        "content-type": "application/json",
      },
    });
    if (!res.ok) throw new Error(`Apify ${path} → ${res.status} ${await res.text()}`);
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

type Row = Record<string, unknown>;

Deno.serve(async (req: Request) => {
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body, null, 2), {
      status,
      headers: { "Content-Type": "application/json" },
    });

  let sql: ReturnType<typeof postgres> | null = null;
  try {
    sql = postgres(DB_URL, { prepare: false, max: 2 });

    const given = req.headers.get("x-fadc-cron") ?? "";
    const [secret] = await sql`select value from fadc.settings where key = 'cron_secret'`;
    if (!secret?.value || given !== secret.value) {
      return json({ ok: false, error: "unauthorized" }, 401);
    }

    const reqBody = await req.json().catch(() => ({}));

    const forma = {
      largo: APIFY_TOKEN.length,
      largo_sin_limpiar: APIFY_TOKEN_RAW.length,
      traia_espacios: APIFY_TOKEN_RAW !== APIFY_TOKEN,
      empieza_con_apify_api: APIFY_TOKEN.startsWith("apify_api_"),
    };

    if (!APIFY_TOKEN) {
      await sql`
        insert into fadc.cron_heartbeats (jobname, last_attempt_at, last_error)
        values ('fadc_tech_sourcing', now(), 'APIFY_TOKEN no configurado')
        on conflict (jobname) do update set
          last_attempt_at = now(), last_error = excluded.last_error
      `;
      return json({ ok: false, error: "APIFY_TOKEN no configurado", forma }, 500);
    }

    if (reqBody?.action === "check") {
      try {
        const me = await apify("/users/me", {}, 10_000);
        return json({
          ok: true,
          token: "válido",
          apify_user: me?.data?.username ?? null,
          plan: me?.data?.plan?.id ?? null,
          forma,
        });
      } catch (e) {
        const detail = e instanceof Error ? e.message : String(e);
        return json({ ok: false, token: "inválido", forma, detail: detail.slice(0, 300) }, 200);
      }
    }

    const cfgRows = await sql<{ key: string; value: string }[]>`
      select key, value from fadc.settings
      where key in ('sourcing_queue_threshold','sourcing_queries_per_run',
                    'sourcing_results_per_query','sourcing_enabled')
    `;
    const cfg = new Map(cfgRows.map((r) => [r.key, r.value]));
    const num = (k: string, d: number) => {
      const v = Number(cfg.get(`sourcing_${k}`));
      return Number.isFinite(v) && v > 0 ? v : d;
    };
    const queueThreshold = num("queue_threshold", DEFAULTS.queue_threshold);
    const queriesPerRun = num("queries_per_run", DEFAULTS.queries_per_run);
    const resultsPerQuery = num("results_per_query", DEFAULTS.results_per_query);
    const enabled = (cfg.get("sourcing_enabled") ?? "true") !== "false";

    const heartbeat = async (error: string | null) => {
      if (error) {
        await sql!`
          insert into fadc.cron_heartbeats (jobname, last_attempt_at, last_error)
          values ('fadc_tech_sourcing', now(), ${error})
          on conflict (jobname) do update set
            last_attempt_at = now(), last_error = excluded.last_error
        `;
      } else {
        await sql!`
          insert into fadc.cron_heartbeats (jobname, last_attempt_at, last_ok_at, last_error)
          values ('fadc_tech_sourcing', now(), now(), null)
          on conflict (jobname) do update set
            last_attempt_at = now(), last_ok_at = now(), last_error = null
        `;
      }
    };

    // Cola que importa: prospectos sin textear en las zonas que HOY se estan buscando.
    //
    // 26 oct: antes contaba solo zone = 'other' (sur de NH). Ese dia se apagaron las
    // consultas de 'other' y 'hartford' y el buscador se reapunto a Boston, Providence y
    // Worcester, que es donde estan los 5 trabajos cobrados. Con el conteo viejo el
    // freno habria quedado midiendo una cola que ya nadie llena: 36 prospectos de NH
    // congelados para siempre, mientras Providence se quedaba sin buscar.
    //
    // Ahora la cola se mide contra las zonas con consultas activas, asi el freno sigue
    // al objetivo sin tener que tocar codigo cada vez que cambia la estrategia.
    const gapQueue = async () => {
      const [{ queue }] = await sql!<{ queue: number }[]>`
        select count(*)::int as queue from fadc.technicians
        where stage = 'prospect' and sms_sent_at is null and not opted_out
          and zone in (select distinct zone from fadc.sourcing_queries where active)
      `;
      return queue;
    };

    // ── FASE 1: ¿hay un run pendiente que cobrar? ────────────────────────────
    const [pending] = await sql<Row[]>`
      select id, apify_run_id, apify_dataset_id, queries, started_at
      from fadc.sourcing_runs
      where status = 'started' and apify_run_id is not null
      order by started_at desc
      limit 1
    `;

    if (pending) {
      const runId = String(pending.apify_run_id);
      const info = await apify(`/actor-runs/${runId}`);
      const status = String(info?.data?.status ?? "");

      if (status === "READY" || status === "RUNNING") {
        const ageMin = (Date.now() - new Date(String(pending.started_at)).getTime()) / 60_000;
        if (ageMin > 120) {
          await sql`
            update fadc.sourcing_runs
            set status = 'aborted', collected_at = now(),
                error = ${`run colgado ${Math.round(ageMin)} min`}
            where id = ${pending.id as number}
          `;
          await heartbeat(null);
          return json({ ok: true, action: "abandoned_stuck_run", run_id: runId });
        }
        await heartbeat(null);
        return json({ ok: true, action: "still_crawling", run_id: runId, age_min: Math.round(ageMin) });
      }

      if (status !== "SUCCEEDED") {
        await sql`
          update fadc.sourcing_runs
          set status = 'failed', collected_at = now(), error = ${`Apify status ${status}`}
          where id = ${pending.id as number}
        `;
        await heartbeat(`Apify run ${status}`);
        return json({ ok: false, action: "run_failed", status }, 200);
      }

      const datasetId = String(pending.apify_dataset_id ?? info?.data?.defaultDatasetId ?? "");
      const items: Row[] = await apify(
        `/datasets/${datasetId}/items?clean=true&format=json&limit=2000`,
        {},
        30_000,
      );

      const zoneByQuery = new Map<string, string>();
      const qRows = await sql<{ query: string; zone: string }[]>`
        select query, zone from fadc.sourcing_queries
        where query = any(${pending.queries as string[]})
      `;
      for (const r of qRows) zoneByQuery.set(r.query, r.zone);

      const existing = await sql<{ p: string }[]>`
        select regexp_replace(phone, '\\D', '', 'g') as p
        from fadc.technicians where phone is not null
      `;
      const known = new Set(
        existing.map((r) => (r.p.length >= 10 ? r.p.slice(-10) : "")).filter(Boolean),
      );

      let skippedNoPhone = 0, skippedDupe = 0, skippedCat = 0, skippedZone = 0, skippedTollFree = 0;
      let skippedReseller = 0;
      const batch = new Map<string, Row>();

      for (const it of items) {
        const cat = String(it.categoryName ?? "");
        if (!GOOD_CATS.has(cat)) { skippedCat++; continue; }

        if (looksLikeLeadReseller(String(it.title ?? ""))) { skippedReseller++; continue; }

        const p10 = last10(it.phone);
        if (!p10) { skippedNoPhone++; continue; }
        if (isTollFree(p10)) { skippedTollFree++; continue; }
        if (known.has(p10) || batch.has(p10)) { skippedDupe++; continue; }

        const st = normState(String(it.state ?? ""), String(it.address ?? ""));
        const zone = resolveZone(zoneByQuery.get(String(it.searchString ?? "")) ?? "", st);
        if (!zone) { skippedZone++; continue; }

        const rating = it.totalScore ? `rating ${it.totalScore}` : "";
        const reviews = it.reviewsCount ? `${it.reviewsCount} reviews` : "";
        const town = [String(it.city ?? ""), st].filter(Boolean).join(" ");
        batch.set(p10, {
          name: String(it.title ?? "").slice(0, 250),
          phone: `+1${p10}`,
          zone,
          base_town: town.slice(0, 120),
          notes: [cat, rating, reviews].filter(Boolean).join(" | "),
          query: String(it.searchString ?? ""),
        });
      }

      const fresh = [...batch.values()];
      if (fresh.length) {
        await sql`
          insert into fadc.technicians ${sql(
            fresh.map((t) => ({
              name: t.name as string,
              phone: t.phone as string,
              type: "subcontractor",
              stage: "prospect",
              zone: t.zone as string,
              source: "google_maps",
              base_town: t.base_town as string,
              notes: t.notes as string,
            })),
            "name", "phone", "type", "stage", "zone", "source", "base_town", "notes",
          )}
        `;
      }

      const perQuery = new Map<string, number>();
      for (const t of fresh) {
        const q = String(t.query);
        perQuery.set(q, (perQuery.get(q) ?? 0) + 1);
      }
      for (const q of pending.queries as string[]) {
        await sql`
          update fadc.sourcing_queries
          set last_run_at = now(), runs = runs + 1, found = found + ${perQuery.get(q) ?? 0}
          where query = ${q}
        `;
      }

      await sql`
        update fadc.sourcing_runs
        set status = 'collected', collected_at = now(),
            raw_count = ${items.length}, inserted = ${fresh.length},
            skipped_dupe = ${skippedDupe}, skipped_no_phone = ${skippedNoPhone}
        where id = ${pending.id as number}
      `;

      const queue = await gapQueue();

      if (fresh.length > 0) {
        const towns = [...new Set(fresh.map((t) => String(t.base_town)).filter(Boolean))].slice(0, 8);
        const text =
          `🔎 ${fresh.length} técnicos nuevos en zonas sin cobertura (${towns.join(", ")}).\n` +
          `Quedan ${queue} sin textear en esas zonas. El outreach los recoge solo.`;
        const delivered = await sendTelegram(text);
        await sql`
          insert into fadc.notifications (kind, text, delivered_telegram)
          values ('tech_sourcing', ${text}, ${delivered})
        `;
      }

      await heartbeat(null);
      return json({
        ok: true, action: "collected", run_id: runId,
        raw: items.length, inserted: fresh.length,
        skipped: { dupe: skippedDupe, no_phone: skippedNoPhone, category: skippedCat, zone: skippedZone, toll_free: skippedTollFree, reseller: skippedReseller },
        gap_queue: queue,
      });
    }

    // ── FASE 2: no hay run pendiente. ¿Hace falta arrancar uno? ──────────────
    const queue = await gapQueue();

    if (!enabled) {
      await heartbeat(null);
      return json({ ok: true, action: "disabled", gap_queue: queue });
    }

    if (queue >= queueThreshold) {
      await heartbeat(null);
      return json({ ok: true, action: "queue_healthy", gap_queue: queue, threshold: queueThreshold });
    }

    // Zona hueco primero; dentro de cada grupo, la menos reciente.
    const next = await sql<{ query: string }[]>`
      select query from fadc.sourcing_queries
      where active and source = 'google_maps'
      order by (zone = ${GAP_ZONE}) desc, last_run_at asc nulls first,
               (query ilike '% NH') desc, id desc
      limit ${queriesPerRun}
    `;
    if (next.length === 0) {
      await heartbeat(null);
      return json({ ok: true, action: "no_queries_left", gap_queue: queue });
    }
    const queries = next.map((r) => r.query);

    if (reqBody?.dry_run === true) {
      return json({ ok: true, action: "would_start", queries, gap_queue: queue, threshold: queueThreshold });
    }

    const started = await apify(`/acts/${ACTOR}/runs`, {
      method: "POST",
      body: JSON.stringify({
        searchStringsArray: queries,
        maxCrawledPlacesPerSearch: resultsPerQuery,
        language: "en",
        skipClosedPlaces: true,
      }),
    });

    const runId = String(started?.data?.id ?? "");
    const datasetId = String(started?.data?.defaultDatasetId ?? "");
    await sql`
      insert into fadc.sourcing_runs (apify_run_id, apify_dataset_id, status, queries)
      values (${runId}, ${datasetId}, 'started', ${queries})
    `;

    await heartbeat(null);
    return json({ ok: true, action: "started", run_id: runId, queries, gap_queue: queue, threshold: queueThreshold });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    try {
      if (sql) {
        await sql`
          insert into fadc.cron_heartbeats (jobname, last_attempt_at, last_error)
          values ('fadc_tech_sourcing', now(), ${msg})
          on conflict (jobname) do update set
            last_attempt_at = now(), last_error = excluded.last_error
        `;
      }
    } catch { /* ignore */ }
    return json({ ok: false, error: msg }, 500);
  } finally {
    try { await sql?.end(); } catch { /* ignore */ }
  }
});
