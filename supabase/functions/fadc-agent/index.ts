// FADC Messenger Agent — Supabase Edge Function
// Three ways in:
//   1. Meta Messenger webhook (GET verify + POST events) → runs agent → replies via Send API.
//   2. Direct JSON POST with x-fadc-secret header (curl tests / ManyChat) → returns {reply}.
//   3. POST {"action":"followups"} (pg_cron every 30 min, or manual) → nudges quiet leads. See runFollowups.
// Flow per message: upsert lead → save inbound (dedup) → load history → Claude → save reply → notify team → respond.

import postgres from "npm:postgres@3.4.4";
import { AsyncLocalStorage } from "node:async_hooks";

// Which lead the current request is about, so every alert can be linked to its lead in the
// CRM without threading a lead id through 29 notifyTeam calls. One store per request.
const leadCtx = new AsyncLocalStorage<{ leadId: string | null }>();
function setCtxLead(id: string | null) { const st = leadCtx.getStore(); if (st) st.leadId = id; }

const sql = postgres(Deno.env.get("SUPABASE_DB_URL")!, { prepare: false, max: 2 });

// Heartbeat: every cron-triggered action records that it actually ran here, independent of
// net._http_response (which logs EVERY http call in the project, including ad-hoc debugging
// calls, and so is too noisy to alert on directly). fadc-canary reads this table instead and
// alerts only when a known job goes quiet longer than its own schedule allows for.
async function heartbeat(jobname: string, ok: boolean, error?: string) {
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

const WEBHOOK_SECRET = Deno.env.get("FADC_WEBHOOK_SECRET") ?? "";
const ANTHROPIC_API_KEY = Deno.env.get("ANTHROPIC_API_KEY") ?? "";
const MODEL = Deno.env.get("ANTHROPIC_MODEL") ?? "claude-haiku-4-5";
const HOURS = Deno.env.get("FADC_HOURS") ?? "Sunday to Thursday 8am-8pm, Friday 8am-6pm, closed Saturdays";
const TG_TOKEN = Deno.env.get("TELEGRAM_BOT_TOKEN") ?? "";
const TG_CHAT = Deno.env.get("TELEGRAM_CHAT_ID") ?? "";

// Twilio WhatsApp — used to notify Afek directly when a job is closed out as "done".
// Number format: whatsapp:+1XXXXXXXXXX
const TWILIO_SID = Deno.env.get("TWILIO_ACCOUNT_SID") ?? "";
const TWILIO_TOKEN = Deno.env.get("TWILIO_AUTH_TOKEN") ?? "";
const TWILIO_FROM = Deno.env.get("TWILIO_WA_FROM") ?? "whatsapp:+13055264093"; // 305 number
const AFEK_WA = Deno.env.get("AFEK_WA_NUMBER") ?? "whatsapp:+16178309808";     // Afek's WhatsApp

// Twilio plain SMS — used for cold outreach to technician prospects (runTechnicianOutreach).
// This is Michel's own recruiting line, already used manually for this exact purpose
// (see claude/plan-30-dias-red-tecnicos-14oct-2026.md), forwarded to his cell.
const TWILIO_SMS_FROM = Deno.env.get("TWILIO_SMS_FROM") ?? "+16178125319"; // 617 number
const TECH_OUTREACH_DAILY_CAP = Number(Deno.env.get("TECH_OUTREACH_DAILY_CAP") ?? "6");

// Safety kill-switch: while false (default), the agent still receives, qualifies and
// logs every lead normally, but NEVER actually sends a Messenger reply to a real person.
// Set the BOT_LIVE_MODE secret to the exact string "true" only once Michel has reviewed
// qualification quality and explicitly wants real leads to get live replies.
const BOT_LIVE_MODE = (Deno.env.get("BOT_LIVE_MODE") ?? "false").trim().toLowerCase() === "true";

// Meta / Messenger
const META_VERIFY_TOKEN = Deno.env.get("META_VERIFY_TOKEN") ?? "fadc_verify_token_2026";
const META_PAGE_TOKEN = Deno.env.get("META_PAGE_TOKEN") ?? "";
const META_APP_SECRET = Deno.env.get("META_APP_SECRET") ?? "";
// Our own page id, so the bot never answers its own comments. Not a secret; override via env if the page changes.
const META_PAGE_ID = (Deno.env.get("META_PAGE_ID") ?? "1368585769661966").trim();
let lastBadSigAlert = 0; // throttle for the bad-signature alarm below
let lastProfileAlert = 0; // throttle for the broken-token alarm in getMessengerProfile
const GRAPH_VERSION = "v21.0";

const SYSTEM_PROMPT = `You are replying to customers on the Facebook Messenger inbox of Vonkam Home Services, a family-owned air duct, dryer vent and chimney cleaning company. You write as a member of the office team, the same way Michel writes. Write naturally and never bring up AI, automation or "the system", but never claim to be a specific human being either, and never deny being an assistant. If someone asks whether they are talking to a bot or a real person, do not argue: say you are on the Vonkam team here in the chat and offer to get them on a quick call with someone right away. That is both honest and the fastest way to close, people who ask that question are usually ready to buy and just want a human voice.

## Your job
Get the customer from "replied to the ad" to a qualified appointment request, then hand off. A qualified request has ALL of:
1. service (air ducts / dryer vent / chimney, or a combination)
2. for air ducts: how many heating/AC systems the home has (price is $99 per system, so this sets the total, e.g. 2 systems = $198). Ask this naturally when they ask about air-duct price or after they say they want air ducts. Skip for dryer-vent-only.
3. ZIP code (or town + state)
4. best phone number
5. preferred day and a 2-hour time window
6. full address (ask for this LAST, only after they've given a day/time)

When you have all of these, reply with a short confirmation that the team will confirm the slot shortly (see Handoff), and stop. A human sends the final confirmation.

## How you write (copy this exactly)
- MATCH THEIR LANGUAGE. If they write in Spanish, reply in natural Spanish (tú, not usted, warm and close, the way the team talks). If they mix Spanish and English, reply in Spanish. If they write in English, reply in English. Never switch languages mid-conversation unless they do. A big share of our customers in Chelsea, Lynn and Everett are Spanish-speaking and this is often why they pick us.
- NEVER use a dash as punctuation. No em dash, no en dash, no " - ". It is the single clearest tell that a message was written by a machine and our customers notice it. Use a comma, a period, or just two short sentences. Write a time range as 9-11 with a plain hyphen.
- Short. 1 to 3 sentences. One question per message, never a list of questions.
- Use their first name in the first reply ("Yes, Chris!"), then not every message.
- Answer whatever they asked FIRST, directly, then ask the next thing you need.
- Warm, plain, a little upbeat. One exclamation mark max. No emojis.
- No bullet points, no headers, no bold, no "Great question!", no "I'd be happy to", no "Please note that". No sign-offs.
- NEVER re-ask for something already in the conversation. Before you ask anything, scan the whole thread INCLUDING their latest message: if they already gave a ZIP, a day, a phone, an address, or the number of systems, treat it as collected and move to the next missing item. Re-asking is the single worst mistake you can make, it reads like you did not read their message and it loses the customer.
- NEVER recap or summarize back what they just told you ("To confirm, you have a 3-bed, 2-bath, 1,200 sq ft home..."). It wastes the message and sounds robotic. Just answer and ask the next thing.
- ONE question per message. Never "What's your ZIP, and what day works?" if you already have the ZIP.
- If they went quiet and come back, just pick up where it left off.
- You are texting a real person: "What's your ZIP code, and what day works best for you?", not "Could you kindly provide your ZIP code so that we may check availability in your area?"
- If they share something personal (a veteran in the family, seniors, a tight budget, a hard situation), acknowledge it warmly and specifically before anything else, a real person would. E.g. "please thank your husband for his service, and we completely understand about the budget." Acknowledge it ONCE, in your own words, and never repeat the same thanks twice in one message ("Thank you for that, and please thank you for..." is broken English and reads like a machine).

## How we sell: soft outside, certain inside
We are the friendly, easy company. The customer is always right, and we are never the ones
pushing. We get the job because talking to us is pleasant and nothing we say ever turns out to
be untrue at the door. The pressure is in how good the offer is, never in how hard you ask.

- NEVER contradict them head-on. If they are wrong or half-right, agree with the true part
  first, in their words, and let the correction ride in behind it. "That's a fair point — the
  tech can focus on the supply vents first and you decide on the main duct after" beats "No,
  the supply vents aren't included."
- EARN the ask. Every message gives something before it takes: the real answer, a detail that
  saves them money, a reason not to worry. A question with nothing in front of it is a form.
- The ask is an OFFER, not a demand. "If you want, drop me your number and the tech can give
  you a ball-park before you commit" lands; "What's your best phone number?" three times in a
  row loses the job. Never end two messages in a row with the same question.
- When they say they'll think about it, let them. Say something that makes it easy to come
  back — no deposit, pay the day of, we're here — and stop. People who are chased go quiet;
  people who are left alone come back.
- NEVER argue about price or scope. What isn't included is framed as protection for them:
  the tech shows you the price before touching anything, nothing happens without your OK.
- PUSH only where pushing is welcome, and then be direct: they ask about availability or
  times, they describe their own house or system, they say yes to anything. That is buying
  intent — offer a specific slot and ask for what you need to lock it in, once, plainly.
  Everywhere else, answer well and let them lead.

Real examples of the team's messages (match this register exactly, short, direct, human):
- "Yes, Maria! The $99 is for one system. What's your ZIP code?"
- "Great! And what day works best for you?"
- "Ok, 9 to 11 can work?"
- "And your best phone number?"
- "Please send me the full address so I can confirm."
- "You're all set for Tuesday 9 to 11! The tech will call you that morning with his ETA."

NEVER write long explanations or list everything the service includes unprompted. One short answer, one question. If they ask what's included, give one sentence max.

## FAQ copy, when they ask these, use language like this (short, direct, then ONE question)
- "What's included?" → "The $99 covers a full clean of your main duct system, plus a free dryer vent & furnace/AC inspection and before-and-after photos. How many heating/AC systems does your home have?"
  ⚠️ NEVER say the $99 "covers the cleaning of all supply vents". Supply vents/registers are NOT included, the tech checks them on-site and quotes them separately, nothing without the customer's OK. Saying vents are included creates a dispute at the door and we have lost money on this before.
- "What could change / what could increase pricing?" → "The tech inspects on-site and only mentions extras if your supply vents or buildup need more work, nothing gets done without your OK first. What's your ZIP and the best day for you?"
- "Is $99 the flat rate?" → "Yes, $99 per heating/AC system. How many does your home have?"
- "Do you cover [ZIP/city]?" → "Yes, we cover [ZIP]! What day works best for you?"
- "Where are you located?" / "Are you in the area?" / "What areas do you serve?" / "Too far" / "Do you come to [place]?" → ANSWER IT like a person, in one sentence, then one question. We do jobs all over Massachusetts, Connecticut, Rhode Island and New Jersey and send the closest crew. Without their ZIP: "We do jobs all over MA, CT, RI and NJ. What's your ZIP? I'll check the closest crew to you." With their ZIP or town: "Yes, we cover [their town or ZIP]! We have crews all over MA, CT, RI and NJ." and then the next missing item. If they ask where the COMPANY is: "We're a family company out of the Boston area, with local crews all over MA, CT, RI and NJ." For a Connecticut customer add "In Connecticut your manager is Ben, want him to give you a quick call?" Never name a town for the office, never say a place is too far or outside the area.
- "What's the earliest / any time today?" → "The techs are already booked for today, the soonest I can get someone out to you is the next working day. What's your address and a good phone so I can lock it in?" Never promise or offer same-day.
- Stating a town or ZIP ("we are in Mansfield", "I'm in 02048") is NEVER by itself a reason to set needs_human — it is a normal answer to the qualifying flow. Confirm you cover it and ask the next missing item (service, address, phone, day). Do not apologize ("Sorry about that!") for a location — nothing went wrong. Reserve needs_human for the cases this prompt actually lists (discount/hardship, 3+ systems, commercial/mold/insurance, reschedule of a confirmed job, spam) — not for ordinary qualifying answers you're simply unsure how to continue from.

## Multi-family / 2-family / duplex / "forced air no AC"
Count HEATING/AC SYSTEMS, not units or square feet. A 2-family house with one shared forced-air furnace = 1 system = $99. A 2-family with a furnace in each unit = 2 systems = $198. "Forced air" is just the heating type, it does NOT mean more than one system. Ask: "How many heating or AC systems total, one furnace, or one per unit?"

## Facts you can use (don't invent anything else)
- Family owned, 30 years, fully insured. No deposit, customer pays the day of service.
- Appointments run Monday to Friday only. NEVER offer or accept a Saturday or Sunday; offer the next weekday instead.
- We book from the next working day. NEVER offer or promise same-day, and never say we often have same-day openings. The only exception is a safety emergency, which the code handles on its own.
- Air duct cleaning: $99 flat rate per heating/AC system (main duct system). Supply vents/registers are NOT included in the $99, the tech checks them on-site and gives options before starting; nothing extra without the customer's approval. Includes: free inspection of dryer vent, furnace/AC, blower motor and plenum box, and before-and-after photos. Multiple systems: $99 each.
- Dryer vent cleaning: $99 flat rate. Tech checks on-site and tells the customer first if anything extra is needed. No work done without approval.
- Chimney: inspection first, exact quote on site. If they ask for a chimney price, say the tech quotes it on site after inspection. Do not give a number.
- Hours: ${HOURS}.
- Openings start the next working day.
- The technician calls the customer the same day of the appointment with his ETA.
- Phone: (617) 812-5319. Offer it only if they ask to call or seem to prefer calling.

## Special pricing, hand these to a human, never quote a discount yourself
Our best closes come from a person offering the right package. You must recognize these and hand off warmly, never quote a plain per-system total and never invent a discount:
- Senior, veteran or military family, disabled, fixed income, "careful with money", or any request for a discount → acknowledge them warmly and specifically, say you'll put together the best package for them with the team, collect address + phone + preferred day, and set needs_human to true. (Example that closed: a senior couple, disabled-veteran husband, 9 duct systems + dryer vent, a person offered a special package for $149 total. You would have quoted $891 and lost it.)
- 3 or more systems: the raw total gets big, so don't state $99×N as if final, say the tech will confirm the best package / you'll check the best price with the team, collect the details, and set needs_human to true.
- Straightforward 1-2 system jobs with no hardship or discount mention: quote normally ($99 each) and move to scheduling.
- Price matching: if a customer says they have another offer or quote (e.g. "I got a $69 quote", "someone else offered $79"), match it immediately: "We can match that! $69 it is." Do NOT hand off to a human for price matching under $99, just match and keep booking. This only applies when THEY bring up a competing price, never offer below $99 unprompted.

## Talk like a person, not a script
- If the customer asks a question, ANSWER IT FIRST in plain words, then move on. Never dodge a question with a stock line, never answer a question with only a time slot.
- Never send a sentence you already sent in this chat. If they did not take an offer, do not repeat it, say something different.
- If they say you did not answer, or sound annoyed ("No!", "you didn't answer", "are you even reading"), start with "Sorry about that!" and answer exactly what they asked. Do not offer a day or slot in that message.
- Match their tone. Short, warm, casual, like a person texting from the office.

## Things you never do
- NEVER tell a customer, directly or by implication, that they are outside our area or too far. Never name Belmont. You MAY say we are a family company out of the Boston area with local crews in MA, CT, RI and NJ, and that Ben is the Connecticut manager. You MAY say we do jobs all over MA, CT, RI and NJ (our ads say so). Never claim other states. A ZIP in any state is a normal lead: keep collecting, the owner assigns the crew.
- NEVER turn a job away or tell a customer we don't do something. You do not decide what Vonkam takes on, the owner does. Commercial buildings, warehouses, apartment complexes, mold, sanitizing, asbestos, insurance claims, complaints, refunds: every one of these gets "Let me check with the team and get back to you in a bit." plus needs_human = true. Never "that's outside what we do", never suggest another company. A big commercial job is exactly the kind of money we do not hand to a competitor.
- NEVER set stage to ready_to_confirm until you have the full street address, not just a ZIP. A ticket without a street address cannot be worked. If you have the day, window and phone but no address, ask: "Perfect! Last thing, what's the full street address so I can lock it in?"
- CONFIRM the day and the 2-hour window directly, do not make them wait for a callback. A customer told to wait for confirmation goes and books someone else. If no tech turns out to be free, the team reschedules them, which is far easier than winning back a lead who went cold.
- Never quote prices not listed above. If asked something not covered (commercial jobs, mold, sanitizing, insurance claims, complaints, discounts, cancelling or moving an already-confirmed job): say "Let me check with the team and get back to you in a bit" and set needs_human to true.
- Never send more than one message per incoming message.
- Never write more than about 60 words.
- If they say they already found someone else or booked with someone, do ONE polite push-to-close (never a second). Something like: "Totally understand! Just so you know, we can get a tech out as soon as the next working day, no deposit, you only pay once it's done, and you get before-and-after photos. If the other quote falls through or you want a quick second opinion, just let me know." Keep stage at qualifying. If they decline again or stay silent, THEN set stage to closed_lost next turn with: "No problem at all, thank you! We're here whenever you need us." Do NOT push a second time.
- If the message is clearly not a customer (spam, another business, a job seeker): reply with an empty string and set needs_human to false and stage to closed_lost.

## Reschedule / change of plans
If they want to move or change an appointment ("can we move it", "need to reschedule", "something came up", "what about Thursday instead", "actually make it the afternoon"):
- Do NOT start the qualification over. You already have their service, ZIP, phone and address, keep all of it.
- Acknowledge in one short line and capture the NEW day/window into preferred_day and preferred_window.
- Reply like: "No problem at all! I have you down for Thursday afternoon instead."
- Set needs_human to true and put the change in handoff_summary, e.g. "RESCHEDULE: Chris, 12 Main St Boston, Tue 9-11 moved to Thu 1-3". A person has to move it on the calendar.
- If they turn down a day YOU just offered ("working Friday", "can't do that day", "no"), that is not a reschedule: say "No problem" in a few words and do not repeat that day. The system adds the next open slot, so do not invent one.

## Closing moves (use sparingly, one per message, never stacked)
You are trying to book, not just collect data. Once you have service + ZIP:
- Offer a concrete window instead of an open question: "I have an opening tomorrow 9-11, want me to hold it?" beats "what day works for you?"
- Light, true scarcity: "mornings usually fill first, do you prefer AM or PM?"
- Reduce risk at the moment of commitment: "No deposit, you only pay the day of service once it's done."
- Specific local proof when it fits naturally: "we did a 2-family in Chelsea last week."
- When they give the last piece, close warmly by name: "You're all set, David! Someone will text you shortly to confirm."
- If they hesitate, sound unsure, ask who they are talking to, or ask something you are not allowed to answer, offer the call: "want someone to give you a quick call? Send me your best number." A voice closes faster than a chat.
Never invent a specific slot as if it is already booked, never promise a tech by name, never confirm the appointment yourself.

## Objections: answer it, reassure, then steer back to the booking (every time)
The goal of every message is the appointment. Never argue, never leave an objection hanging, never end on a dead end. One short answer, one reason to feel safe, one easy next step.
- "Are you in my area? / Where are you? / That's far" → "Yes, we cover [their town or ZIP]! We have crews all over MA, CT, RI and NJ." then the next step. We ARE in those areas, never say or imply they are outside it.
- "Is it really $99? / What's the catch?" → "Yes, $99 per system, and you only pay the day of service once it's done. The tech shows you before-and-after photos." then the next step.
- "I'm working that day / can't do Friday" → "No problem! Mornings or afternoons better for you?" or offer another day. Never re-offer the day they turned down.
- "Let me think / talk to my husband / check my schedule" → "Of course! No deposit, so I can pencil you in and you can move it anytime. Want me to hold [day] for you?"
- "Are you legit? / Never heard of you" → "Totally fair! We're family owned, 30 years, fully insured, and you don't pay anything until the job is done." then the next step.
- "No" with no reason → "No problem! Is it the day, the time, or something else? I'll work around you."
- Annoyed or confused → "Sorry about that!" plus a straight answer. No slot in that same message.
Always give them a choice that is easy to say yes to ("morning or afternoon?", "this week or next?") instead of an open question when you can.

## Handoff message (when all items are collected)
"You're all set, [name]! [day] [window]. The tech will call you that morning with his ETA."
Then set stage to ready_to_confirm.

## Output format
Return ONLY a JSON object, no prose, no code fences:
{"reply":"<message to send, or empty string>","lead":{"first_name":null,"last_name":null,"service":null,"zip":null,"town":null,"phone":null,"preferred_day":null,"preferred_window":null,"address":null,"stage":"new|qualifying|ready_to_confirm|closed_lost|needs_human"},"needs_human":false,"handoff_summary":null,"job_type":null,"job_context":null,"job_scale":null}
Fill every lead field you know from the whole conversation (not just this message). handoff_summary is one line for the team when stage is ready_to_confirm or needs_human, e.g. "Chris - air ducts - 02135 - (617) 555-0134 - Tue 10-12 - 12 Main St Boston" or "Ruth asks about mold; needs a person".

Two more fields, only when stage is ready_to_confirm:
- job_type and job_context are ALWAYS written in English, even when the whole conversation is in Spanish. The technicians and the owner read the ticket, not the customer.
- job_type: one short line naming the job the way a technician would read it, starting with the service. "Air duct cleaning \u2014 main heating duct system in a mobile home", "Dryer vent cleaning \u2014 second floor laundry". No price, no name, no address, no date.
- job_context: anything the customer said that changes how the team treats this job or this customer, in one line. Who they are if it matters ("VP of a 144-unit mobile home park, potential for referrals"), access problems, pets, an elderly person at home. Leave it null when there is nothing. Never put the price, phone, address or date here.

One more field, filled at ANY stage, the moment the customer says it, not only when the ticket is ready:
- job_scale: how big the job is, in the customer's own words, written in English. Number of heating/AC systems, number of vents or registers, square footage, how long since the last cleaning, whether a dryer vent or chimney is also in play, more than one unit. "2 systems, never cleaned", "1 system + dryer vent", "8 registers, 3,000 sq ft". You already ask how many systems they have, so this is where that answer goes: write it down the moment they answer instead of dropping it. Leave it null until they actually say something about size, and never infer it from the town or the kind of house.`;

type AgentOut = {
  reply: string;
  lead: Record<string, string | null>;
  needs_human: boolean;
  handoff_summary: string | null;
  job_type?: string | null;
  job_context?: string | null;
  job_scale?: string | null;
};

type ProcessResult = {
  reply: string;
  stage: string;
  needs_human: boolean;
  skipped?: boolean;
  paused?: boolean;
};

function json(status: number, body: unknown) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// Build an explicit state note so the model can never re-ask for something we already have.
// This is the structural fix for the #1 observed failure: re-asking ZIP / day / phone that
// the customer already gave. Prompt text alone was not reliable enough.
// v86 — THE NAG. Jeanmarie Martin asked four real questions in a row ("what is the main duct
// system?", "I want the supply vents cleaned", "ball park quote?", "I'll get back to you") and
// every single reply ended with "What's your best phone number?". Four times. She never gave it
// and signed off. Step 3 below says "ask for the first item still missing", and after v85 put
// phone first in the order, a lead who won't hand over a number gets the same question forever.
// Answering well and then asking the same thing again reads as a form, not a person.
// So: a field asked in either of the last two agent messages is OFF the table for this one.
// The reply still answers them — it just stops ending every message with the same demand.
const FIELD_ASK_RE: Record<string, RegExp> = {
  "phone": /(phone number|best number|number (for|to reach) you|number i can|n[úu]mero de tel[ée]fono|tu mejor n[úu]mero|a qu[ée] n[úu]mero)/i,
  "ZIP / town": /(zip code|your zip|what town|c[óo]digo postal|qu[ée] ciudad|en qu[ée] pueblo)/i,
  "preferred day": /(what day works|which day works|qu[ée] d[íi]a te (funciona|queda|viene))/i,
  "time window": /(mornings or afternoons|morning or afternoon|ma[ñn]ana o (en la )?tarde)/i,
  "first name": /(what name should|name should i put|a nombre de qui[ée]n)/i,
  "full address": /(street address|full address|direcci[óo]n completa)/i,
  "service": /(air ducts, dryer vent or chimney|ductos, secadora o chimenea)/i,
};

// Fields the agent already asked for in its last `turns` messages and did not get.
function recentlyAskedFields(
  history: { role: string; text: string }[],
  turns = 2,
): Set<string> {
  const recentAgent = history.filter((m) => m.role === "agent").slice(-turns);
  const asked = new Set<string>();
  for (const m of recentAgent) {
    for (const [field, re] of Object.entries(FIELD_ASK_RE)) {
      if (re.test(m.text ?? "")) asked.add(field);
    }
  }
  return asked;
}

function buildStateNote(lead: Record<string, unknown>, justAsked?: Set<string>): string {
  const f = (v: unknown) => (v === null || v === undefined || v === "" ? null : String(v));
  const known: string[] = [];
  const missing: string[] = [];

  const fields: Array<[string, string | null, string]> = [
    ["first name", f(lead.first_name), "first_name"],
    ["service", f(lead.service), "service"],
    ["ZIP / town", f(lead.zip) ?? f(lead.town), "zip"],
    ["phone", f(lead.phone), "phone"],
    ["preferred day", f(lead.preferred_day), "preferred_day"],
    ["time window", f(lead.preferred_window), "preferred_window"],
    ["full address", f(lead.address), "address"],
  ];
  for (const [label, val] of fields) {
    if (val) known.push(`- ${label}: ${val}`);
    else missing.push(label);
  }

  // Ask order: service -> zip -> phone -> day -> window -> name -> address.
  //
  // v85 moved phone ahead of ZIP on the theory that phone is the field that rescues a dead
  // chat. v87 moves it back, because the 30-day numbers say the theory was wrong:
  //
  //   phone AND zip   16 leads → 13 appointments (81%)
  //   zip only        26 leads →  0
  //   phone only       1 lead  →  0
  //   neither         47 leads →  0
  //
  // What converts is the PAIR, not the phone: nobody with just one of them ever booked. And a
  // ZIP is given 2.5x more often than a phone (42 leads vs 17). Leading with the question
  // people refuse does not make them answer it — it loses them before they reach the easy one.
  // Operationally it is also the wrong order: service + ZIP is enough to qualify the job and
  // find a crew, while a phone number with no area attached cannot be dispatched at all.
  const order = ["service", "ZIP / town", "phone", "preferred day", "time window", "first name", "full address"];
  const blocked = justAsked ?? new Set<string>();
  // The next ask skips anything already asked in the last two messages, so the conversation
  // moves on instead of repeating. If everything still missing is blocked, we ask for nothing.
  const nextAsk = order.find((o) => missing.includes(o) && !blocked.has(o)) ?? null;
  const nagging = [...blocked].filter((b) => missing.includes(b));

  return `(Internal note, NOT from the customer. Do not mention it, do not quote it.

ALREADY COLLECTED as of before their latest message. NEVER ask for any of these again:
${known.length ? known.join("\n") : "- (nothing yet)"}

STILL MISSING: ${missing.length ? missing.join(", ") : "nothing, you have everything"}
${nagging.length ? `
ALREADY ASKED, DO NOT ASK AGAIN IN THIS MESSAGE: ${nagging.join(", ")}.
You asked for this in your last message(s) and they did not give it. Asking a third time reads
as pushy and loses the customer. Answer what they actually asked, give them one genuinely useful
detail, and let them lead. They will give it when they are ready, or a teammate will call them.
` : ""}
STEP 1: Read their latest message and pull out anything new (a ZIP, a day, a phone, an address, how many systems). Treat anything they just gave as COLLECTED too, never ask for it back.
STEP 2: Answer whatever they actually asked, properly. If they asked something real, this is the whole job of the message.
STEP 3: ${nextAsk
  ? `Then ask for exactly ONE thing: ${nextAsk}.`
  : missing.length
    ? `Ask for NOTHING this time. End on the answer, or on something helpful about the job. No question at the end.`
    : `Nothing is missing: go straight to the handoff message and set stage to ready_to_confirm.`}

Do NOT recap or summarize back what they told you. Do NOT list what the package includes unless they just asked. Do NOT ask two things in one message. NEVER end two messages in a row with the same question.)`;
}

// --- Special-pricing safety net -------------------------------------------
// The model reliably writes the right WORDS for a hardship/discount lead but
// sometimes forgets to set needs_human, which means no alert fires and the bot
// keeps going alone. That is the most expensive miss in the system: the senior +
// disabled-veteran couple closed at $149 as a human package; a mechanical $99xN
// quote would have been $891 and lost it. So we detect it in code and force the
// flag regardless of what the model returned.
const HARDSHIP_RE =
  /\b(senior|seniors|elderly|retired|fixed income|social security|veteran|veterans|military|army|navy|marine|disabled|disability|discount|coupon|cheaper|best price|better price|tight budget|on a budget|low income|hardship|can'?t afford|afford)\b/i;

function extractSystemCount(text: string): number | null {
  const m = text.match(/(\d+)\s*(?:heating|ac|a\/c|hvac|de)?\s*(systems?|units?|furnaces?|zones?|air handlers?|sistemas?|unidades?|calderas?|hornos?|zonas?)\b/i);
  if (m) return parseInt(m[1], 10);
  const words: Record<string, number> = {
    one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9, ten: 10,
    uno: 1, una: 1, dos: 2, tres: 3, cuatro: 4, cinco: 5, seis: 6, siete: 7, ocho: 8, nueve: 9, diez: 10,
  };
  const w = text.match(/\b(one|two|three|four|five|six|seven|eight|nine|ten|uno|una|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez)\s+(systems?|units?|furnaces?|zones?|air handlers?|sistemas?|unidades?|calderas?|hornos?|zonas?)\b/i);
  if (w) return words[w[1].toLowerCase()] ?? null;
  return null;
}

// Jobs the agent must NEVER decide on alone. It has turned away a 40,000 sq ft
// commercial job on its own ("outside what we do") — that is Afek's call, not the
// bot's. Anything here gets handed to a person instead of refused or answered.
const ESCALATE_RE =
  /\b(commercial|warehouse|office building|apartment (complex|building)|condo association|property manager|multi[- ]unit|mold|mould|remediation|sanitiz(e|ing|ation)|asbestos|insurance claim|lawsuit|attorney|complaint|refund|manager|supervisor)\b/i;

// Safety first, sales second. QA: "My dryer is smoking and smells like burning, can
// someone come right now??" got "That sounds urgent — yes, we can help with dryer vent
// cleaning same-day. What's your ZIP code?" A smoking dryer is a fire in progress; the
// only correct first move is stop using it and call 911 if there is smoke or flame.
const EMERGENCY_RE =
  /\b(smoking|smoke|burning smell|smells? (like )?burning|burn(ed|ing)? smell|on fire|flames?|sparks?|sparking|gas leak|smell(s|ing)? gas|carbon monoxide|co detector|alarm (is )?going off|caught fire)\b/i;

// A gas smell or a CO alarm is a different hazard: unplugging an appliance is the wrong
// move, getting out of the house is the right one. Keep these two replies separate
// (COPY.emergency vs COPY.gas below).
const GAS_RE = /\b(gas leak|smell(s|ing)? (like )?gas|smell of gas|carbon monoxide|co detector)\b/i;

// Every reply the code writes itself has to follow the customer's language too, or the
// safety nets undo the one rule Michel set: the bot accommodates the customer. Spanish is
// detected from accents/punctuation or common words across THEIR messages, not ours.
const ES_STRONG_RE =
  /[ñáéíóúü¿¡]|\b(hola|buenas|buenos|gracias|cuanto|cuantos|cuesta|precio|limpieza|ductos?|secadora|chimenea|quiero|quisiera|necesito|ocupo|puedes|podria|favor|ayuda|cita|disponible|manana|direccion|telefono|cuando|donde|tambien|ustedes|servicio|casa|sistemas?|llamar|llamame|marcame|eres|persona|hablo|hablar)\b/i;

// Two or more distinct Spanish function words is also Spanish, which catches the messages
// that carry no accents and none of the keywords above.
const ES_STOPWORD_RE =
  /\b(que|qu[\u00e9e]|por|para|con|mis|los|las|del|una|mi|tengo|tiene|tienen|usan|son|esta|est[\u00e1a]n|estan|hay|muy|antes|trabajo|cu[\u00e1a]l|cual|como|c[\u00f3o]mo|te|nos|ellos|pero|tambien|tambi[\u00e9e]n|perros?|gatos?|ni[\u00f1n]os?|dias?|d[\u00edi]as?)\b/gi;

function isSpanish(customerText: string): boolean {
  if (ES_STRONG_RE.test(customerText)) return true;
  // A Spanish safety/FAQ pattern matching is itself proof of Spanish. ESCALATE_ES_RE is
  // left out on purpose: "supervisor", "comercial" and friends also read as English.
  if (
    EMERGENCY_ES_RE.test(customerText) || GAS_ES_RE.test(customerText) ||
    UNKNOWN_FACT_ES_RE.test(customerText) || HARDSHIP_ES_RE.test(customerText) ||
    RESCHEDULE_ES_RE.test(customerText)
  ) return true;
  const hits = customerText.match(ES_STOPWORD_RE);
  return !!hits && new Set(hits.map((h) => h.toLowerCase())).size >= 2;
}

// Paired copy for everything the code says on its own behalf.
type Pair = { en: string; es: string };
const COPY: Record<string, Pair> = {
  emergency: {
    en: "Please stop using it and unplug it if you can reach the plug safely. If you see smoke or flames, call 911 first. Once it's safe we can get a tech out to you today. Are you alright?",
    es: "Por favor deja de usarla y desconéctala si puedes alcanzar el enchufe sin riesgo. Si ves humo o llamas, llama al 911 primero. En cuanto sea seguro te mandamos un técnico hoy. ¿Estás bien?",
  },
  gas: {
    en: "Please get everyone out of the house first and call 911 or your gas company from outside. Don't touch any switches. Once it's cleared we can get a tech out to you. Are you safe?",
    es: "Por favor saquen a todos de la casa y llama al 911 o a la compañía de gas desde afuera. No toques ningún interruptor. En cuanto esté seguro te mandamos un técnico. ¿Estás a salvo?",
  },
  bigTotal: {
    en: "That's a bigger job, so let me check the best package price with the team and come right back to you",
    es: "Es un trabajo más grande, déjame checar el mejor precio de paquete con el equipo y te regreso enseguida",
  },
  escalate: {
    en: "Let me check with the team and get back to you in a bit.",
    es: "Déjame checarlo con el equipo y te regreso en un rato.",
  },
  nameNeeded: {
    en: "Perfect! And what's the full name I should put the appointment under?",
    es: "¡Perfecto! ¿Y a nombre completo de quién pongo la cita?",
  },
  address: {
    en: "Perfect! Last thing, what's the full street address so I can lock it in?",
    es: "¡Perfecto! Lo último, ¿cuál es la dirección completa para apartarlo?",
  },
  tooSoon: {
    en: "I can't promise today, the techs are already out on jobs. The soonest I can get someone to you is",
    es: "Hoy no te lo puedo prometer, los técnicos ya están en ruta. Lo más pronto que te puedo mandar a alguien es",
  },
  misclick: {
    en: "No problem at all, have a great day!",
    es: "\u00a1Sin problema, que tengas buen d\u00eda!",
  },
  area: {
    en: "Yes, we do jobs all over MA, CT, RI and NJ, the closest crew comes out to you.",
    es: "Sí, trabajamos en todo MA, CT, RI y NJ, te mandamos el equipo más cercano.",
  },
  areaZip: {
    en: "We do jobs all over MA, CT, RI and NJ. What's your ZIP? I'll check the closest crew to you.",
    es: "Trabajamos en todo MA, CT, RI y NJ. ¿Cuál es tu código postal? Reviso el equipo más cercano.",
  },
  softClose: {
    // v86: no question, no nudge. Make coming back easy and get out of the way.
    en: "Of course, take your time. No deposit either, you only pay the day it's done, so we're here whenever you're ready.",
    es: "Claro, t\u00f3mate tu tiempo. Y sin dep\u00f3sito, pagas el d\u00eda del servicio, as\u00ed que aqu\u00ed estamos cuando quieras.",
  },
  callOffer: {
    en: "I'm on the Vonkam team here in the chat, but honestly it's quicker by phone. Want a quick call? Send me your best number and someone will ring you in a few minutes.",
    es: "Soy del equipo de Vonkam, aquí en el chat, pero la verdad por teléfono es más rápido. ¿Te marcamos? Mándame tu mejor número y alguien te llama en unos minutos.",
  },
};

// Every call offer the code makes, in one place. "unknown" is a question whose answer we
// have not defined; "human" is someone asking if this is a bot or asking for a person.
const callLine = (es: boolean, phone: string | null, reason: "unknown" | "human"): string => {
  if (reason === "unknown") {
    if (phone) {
      return es
        ? `Buena pregunta, y es más fácil explicártelo por teléfono. Alguien te marca al ${phone} en unos minutos y te lo aclara, ¿te funciona?`
        : `Good question, and it's easier to just tell you by phone. Someone will call you at ${phone} in a few minutes to walk you through it, does that work?`;
    }
    return es
      ? "Buena pregunta, y es más fácil explicártelo por teléfono. Mándame tu mejor número y alguien te llama en unos minutos."
      : "Good question, and it's easier to just tell you by phone. Send me your best number and someone will call you in a few minutes.";
  }
  if (phone) {
    return es
      ? `Soy del equipo de Vonkam, aquí en el chat, y por teléfono es más rápido. Alguien te marca al ${phone} en unos minutos, ¿te funciona?`
      : `I'm on the Vonkam team here in the chat, and it's quicker by phone. Someone will call you at ${phone} in a few minutes, does that work?`;
  }
  return es ? COPY.callOffer.es : COPY.callOffer.en;
};

// Meta sends photos, videos, audio, files, locations and stickers as attachments with no
// text at all. The webhook used to skip those outright: the customer got silence and nobody
// on the team was told. In duct cleaning people send photos of their vents constantly, so a
// photo has to get an answer and the team has to see it.
const ATTACH_COPY: Record<string, Pair> = {
  photo: {
    en: "Got the photo, thank you! The tech will take a proper look on site",
    es: "\u00a1Gracias por la foto! El t\u00e9cnico la revisa bien cuando llegue",
  },
  other: {
    en: "Got it, thank you",
    es: "Listo, gracias",
  },
  done: {
    en: "Got it, thank you! Someone will confirm your slot shortly.",
    es: "\u00a1Listo, gracias! Alguien te confirma la cita en un momento.",
  },
};

// Uppercase the first actual letter, so an ask line can follow a sentence.
const cap = (t: string) => t.replace(/^(\W*)(\w)/, (_m, pre, c) => pre + c.toUpperCase());

function describeAttachments(msg: Record<string, any>): { label: string; kinds: string[]; urls: string[] } {
  const atts = Array.isArray(msg?.attachments) ? msg.attachments : [];
  const kinds: string[] = [];
  const urls: string[] = [];
  for (const a of atts) {
    kinds.push(String(a?.type ?? "file").toLowerCase());
    const u = a?.payload?.url;
    if (typeof u === "string" && u) urls.push(u);
  }
  if (msg?.sticker_id && kinds.length === 0) kinds.push("sticker");
  const label = kinds.length > 1
    ? `[customer sent ${kinds.length} attachments: ${kinds.join(", ")}]`
    : `[customer sent a ${kinds[0] ?? "file"}]`;
  return { label, kinds, urls };
}

// "Are you a bot?" and "can someone call me?" are the same opportunity: the customer wants
// a human voice, and a voice closes faster than a chat. Michel's call: never claim to be a
// person, offer the call instead and let the phone prove it. Also the strongest close we
// have — whoever asks this is usually ready to buy.
const BOT_QUESTION_RE =
  /\b(are you (a |an )?(bot|robot|ai|real|human|a person|a real person)|is this (a |an )?(bot|robot|ai|real person|human|automated|automation)|am i (talking|chatting|speaking|texting) (to|with) (a |an )?(bot|robot|ai|real person|human|someone real|person)|is this automated|are you automated|chatgpt|eres (un |una )?(bot|robot|ia|real|humano|persona)|esto es (un )?(bot|autom[áa]tico)|hablo con (una )?persona|es un robot)\b/i;

const CALL_REQUEST_RE =
  /\b(call me|give me a call|can (you|someone) call|could (you|someone) call|rather (talk|speak)|prefer to (talk|speak)|talk on the phone|by phone|over the phone|ll[áa]mame|ll[áa]menme|me puedes? llamar|pueden llamarme|por tel[ée]fono|m[áa]rcame|m[áa]rquenme|quiero hablar con)\b/i;

// ---------------------------------------------------------------------------------------
// THE TICKET. One fixed shape, because Afek reads these on a phone between jobs and a ticket
// he has to decode is a ticket he mis-dispatches. Every field is built in code from stored
// fields, so two tickets never come out looking different.
//
// The two things the model used to leave ambiguous are resolved here: "Wednesday" becomes a
// real calendar date, and "1-3" becomes 1:00-3:00 PM. A tech dispatched on the wrong
// Wednesday costs a whole job, and that ambiguity has no business leaving this function.
const MONTHS_EN = ["January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"];
const TODAY_RE = /\b(today|tonight|this (after ?noon|morning|evening)|hoy|esta (tarde|noche|mañana)|ahorita|ya mismo|right now|same day|mismo día)\b/i;
const WEEKDAY_RE = /\b(sunday|monday|tuesday|wednesday|thursday|friday|saturday|domingo|lunes|martes|mi[ée]rcoles|jueves|viernes|s[áa]bado)\b/i;
const WEEKDAY_INDEX: Record<string, number> = {
  sunday: 0, domingo: 0, monday: 1, lunes: 1, tuesday: 2, martes: 2,
  wednesday: 3, "miércoles": 3, miercoles: 3, thursday: 4, jueves: 4,
  friday: 5, viernes: 5, saturday: 6, "sábado": 6, sabado: 6,
};

// "Tue Oct 6" for the ticket header. The long form stays for anywhere a full date is wanted.
function shortDate(raw: string | null): string {
  const d = resolveDateObj(raw);
  if (!d) return (raw ?? "").trim();
  return `${["Sun","Mon","Tue","Wed","Thu","Fri","Sat"][d.getUTCDay()]} ${MONTHS_EN[d.getUTCMonth()].slice(0,3)} ${d.getUTCDate()}`;
}

const fmtDate = (d: Date) =>
  `${["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"][d.getUTCDay()]}, ${MONTHS_EN[d.getUTCMonth()]} ${d.getUTCDate()}, ${d.getUTCFullYear()}`;

// Resolves the customer's words to an actual Date, so code can compare it against the
// earliest slot we are allowed to offer. Null when the text names no date at all.
//
// "today" was missing from the first version, and Winnie's ticket came out reading
// "Date: today" — a line that becomes a lie the moment the clock passes midnight.
function resolveDateObj(raw: string | null): Date | null {
  const t = (raw ?? "").trim();
  if (!t) return null;
  const today = bostonToday();

  const md = t.match(/\b(\d{1,2})\s*\/\s*(\d{1,2})\b/);
  if (md) {
    const mo = Number(md[1]), day = Number(md[2]);
    if (mo >= 1 && mo <= 12 && day >= 1 && day <= 31) {
      let y = today.getUTCFullYear();
      let d = new Date(Date.UTC(y, mo - 1, day, 12));
      // A date already behind us means they meant next year, not last week.
      if (d.getTime() < today.getTime() - 86_400_000) d = new Date(Date.UTC(++y, mo - 1, day, 12));
      return d;
    }
  }
  if (TODAY_RE.test(t)) return today;
  if (/\btomorrow\b|\bma[ñn]ana\b/i.test(t)) return new Date(today.getTime() + 86_400_000);

  const wd = t.match(WEEKDAY_RE);
  if (wd) {
    const want = WEEKDAY_INDEX[wd[1].toLowerCase()];
    if (want !== undefined) {
      // Always the NEXT one. "Wednesday" said on a Wednesday means the coming Wednesday.
      let delta = (want - today.getUTCDay() + 7) % 7;
      if (delta === 0) delta = 7;
      return new Date(today.getTime() + delta * 86_400_000);
    }
  }
  return null;
}

// "Wednesday" / "Tue 10/6" / "10/6" -> "Wednesday, October 7, 2026". Unparseable text is
// returned as the customer said it, never guessed at.
function resolveDate(raw: string | null): string {
  const t = (raw ?? "").trim();
  if (!t) return "NOT SET, ask the customer";
  const d = resolveDateObj(t);
  return d ? fmtDate(d) : t;
}

// "1-3" -> "1:00-3:00 PM". Anything we cannot read comes through untouched.
function resolveWindow(raw: string | null): string {
  const t = (raw ?? "").trim();
  if (!t) return "NOT SET, ask the customer";
  const m = t.match(/^(\d{1,2})\s*(?:am|pm)?\s*[-–to]+\s*(\d{1,2})\s*(am|pm)?$/i);
  if (m) {
    const a = Number(m[1]), b = Number(m[2]);
    const ampm = (h: number) => (h >= 8 && h < 12 ? "AM" : "PM");
    const sa = ampm(a), sb = ampm(b);
    return sa === sb ? `${a}:00–${b}:00 ${sa}` : `${a}:00 ${sa}–${b}:00 ${sb}`;
  }
  if (/before noon/i.test(t)) return "Before 12:00 PM";
  return t;
}

// "Brookfield MA 01506" -> "Brookfield, MA 01506", so every ticket reads the same way.
function tidyAddress(raw: string | null): string {
  const t = (raw ?? "").trim();
  if (!t) return "NOT SET, ask the customer";
  return t
    .replace(/(,?)\s+([A-Z]{2})\s+(\d{5})\b/, ", $2 $3")
    .replace(/,\s*,/g, ",")
    .replace(/\s{2,}/g, " ");
}

const SERVICE_LABEL: Record<string, string> = {
  "air ducts": "Air duct cleaning",
  "dryer vent": "Dryer vent cleaning",
  "chimney": "Chimney cleaning",
};
function serviceLabel(raw: string | null): string {
  const t = (raw ?? "").trim().toLowerCase();
  if (!t) return "NOT SET";
  // The separators need word boundaries. Without them the "y" alternative matched the y
  // inside "dryer" and the one ending "chimney", and both services lost their label.
  const parts = t.split(/\s*(?:,|\+|\band\b|\by\b)\s*/).map((p) => SERVICE_LABEL[p.trim()] ?? null).filter(Boolean);
  return parts.length ? parts.join(" + ") : raw!.trim();
}

// Winnie's job was 1-3 PM and her ticket still said the tech would call "that morning".
// A fixed line is only right half the time, so the line follows the hour.
function callWhen(window: string | null): string {
  const t = (window ?? "").trim();
  if (/before noon|morning|ma[ñn]ana/i.test(t)) return "morning";
  const m = t.match(/^(\d{1,2})/);
  if (m) return Number(m[1]) >= 12 || Number(m[1]) < 8 ? "afternoon" : "morning";
  return "morning";
}

// The tech routes by ZIP before he reads the street. Add it when the address does not
// already carry one (David, Hollis 03049, went out as "31 Shipley Drive, Hollis").
function withZip(addr: string, zip?: string | null): string {
  const z = (zip ?? "").trim();
  if (!addr || !/^\d{5}$/.test(z) || /\b\d{5}\b/.test(addr)) return addr;
  return `${addr} ${z}`;
}

function buildTicket(f: {
  name: string | null; jobType: string | null; service: string | null;
  day: string | null; window: string | null; address: string | null;
  phone: string | null; context: string | null; callAfter?: string | null;
  scale?: string | null; zip?: string | null;
}): string {
  // Short on purpose. Afek and the techs read these on a phone between jobs, so the ticket
  // leads with WHEN (what a dispatcher sorts by), then the four facts needed to show up, then
  // at most a line of whatever is not obvious. "IVY JOB" marks it as ours at a glance.
  //
  // What was cut: the "TICKET" header (the IVY JOB line already says what this is), the
  // "Full Name:/Phone:/Address:" labels (a name, a phone number and a street address do not
  // need labels), and the "Technician will call that morning with an ETA" line, which was on
  // every single ticket and therefore carried no information.
  // A missing field must not leak "NOT SET, ask the customer" into the header line.
  const real = (x: string) => (x && !/NOT SET/i.test(x) ? x : "");
  const when = [real(shortDate(f.day)), real(resolveWindow(f.window))].filter(Boolean).join(", ");
  const lines = [
    when ? `IVY JOB — ${when}` : "IVY JOB — DATE NOT SET",
    "",
    f.name ?? "NAME NOT GIVEN",
    f.phone ?? "NO PHONE",
    withZip(real(tidyAddress(f.address)), f.zip) || "NO ADDRESS",
    (f.jobType && f.jobType.trim()) || serviceLabel(f.service),
  ];
  // The extras only appear when there is something to say.
  const tail: string[] = [];
  // Size goes on the ticket because the tech is the one who sells the add-ons, and he cannot
  // sell what he does not know is there. A $99 base job does not pay for the ads; the vents,
  // the second system and the sanitation do.
  if (f.scale && f.scale.trim()) tail.push(`Size: ${f.scale.trim()}`);
  if (f.callAfter) tail.push(`Can take a call ${f.callAfter}.`);
  // Mike Lilla turned out to run a 144-unit park. Dropping that line to keep the ticket tidy
  // would have thrown away the referrals with it, so context survives the trim.
  if (f.context && f.context.trim()) tail.push(f.context.trim());
  if (tail.length) lines.push("", ...tail);
  return lines.join("\n");
}


// v84 — SIZE IS THE WHOLE BUSINESS. Four jobs in five weeks: three at $150–$299 and one at
// $1,282 (7 vents, sanitation, 2 UV lights). That single job is 66% of everything billed and
// made the only profitable week we have had. A $99 base ticket cannot pay for $232 of weekly
// ads at one job a week, so the ticket has to be bigger. The bot was already asking every
// customer how many systems they had and throwing the answer away, which left nobody able to
// tell a $99 lead from a $1,282 one until the tech was in the driveway. Now the answer is
// kept (leads.job_scale), the big ones get a person on the phone the same hour, and the
// ticket carries the size so the tech knows what is there to sell.
const BIG_JOB_RE = new RegExp([
  String.raw`\b([2-9]|[1-9]\d)\s*\+?\s*(systems?|units?|furnaces?|zones?|air handlers?)\b`,
  String.raw`\b(two|three|four|five|six|seven|eight|nine|ten)\s+(systems?|units?|furnaces?|zones?)\b`,
  String.raw`\b[1-9]\d\s*(vents?|registers?|ducts?|runs?)\b`,
  String.raw`\bnever\s+(been\s+)?clean`,
  String.raw`\b([2-9][,.]?\d{3}|\d{2},\d{3})\s*(sq|square)\b`,
  String.raw`\bmulti[\s-]?(family|unit)\b`,
  String.raw`\b(duplex|triplex|two[\s-]?family|three[\s-]?family)\b`,
].join("|"), "i");
function bigJob(scale: string | null | undefined): boolean {
  return !!scale && BIG_JOB_RE.test(scale);
}

// ---------------------------------------------------------------------------------------
// THE OPEN QUESTION. Every real lead died on the same sentence: "What day works best for
// you?" Mansfield was asked three times and never answered. Donna twice, never answered.
// Janet is the only booking we have, and she was never closed by that question either, she
// arrived already saying "Tues 10/6 before noon".
//
// Line 120 of the system prompt has told the model to offer a concrete window since day one.
// It asked the open question anyway, 10 times out of 10. Same lesson as every other defect
// in this file: a rule that costs money does not live in a prompt, it lives in code.
//
// So the bot stops asking and starts offering. A yes/no is a far smaller ask than inventing
// a date. The rule itself lives in fadc.settings so Afek's answer is one UPDATE, not a deploy.
type Availability = { lead_time_days: number; days: string[]; windows: string[]; max_days_out: number; max_per_window: number };
const AVAIL_FALLBACK: Availability = {
  lead_time_days: 1, days: ["Mon", "Tue", "Wed", "Thu", "Fri"], windows: ["9-11", "1-3", "4-6"], max_days_out: 7, max_per_window: 1,
};
let availCache: { v: Availability; at: number } | null = null;

async function loadAvailability(): Promise<Availability> {
  if (availCache && Date.now() - availCache.at < 300_000) return availCache.v;
  try {
    const [row] = await sql`select value from fadc.settings where key = 'availability'`;
    const raw = row?.value ?? null;
    const parsed = raw ? JSON.parse(String(raw)) : null;
    const v: Availability = { ...AVAIL_FALLBACK, ...(parsed ?? {}) };
    // A bad row must never make the bot promise a slot that cannot exist.
    if (!Array.isArray(v.days) || !v.days.length) v.days = AVAIL_FALLBACK.days;
    if (!Array.isArray(v.windows) || !v.windows.length) v.windows = AVAIL_FALLBACK.windows;
    if (!(v.lead_time_days >= 0)) v.lead_time_days = AVAIL_FALLBACK.lead_time_days;
    if (!(v.max_days_out > 0)) v.max_days_out = AVAIL_FALLBACK.max_days_out;
    if (!(v.max_per_window >= 1)) v.max_per_window = AVAIL_FALLBACK.max_per_window;
    availCache = { v, at: Date.now() };
    return v;
  } catch (_) {
    return AVAIL_FALLBACK;
  }
}

const DAY_ABBR = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const DAY_EN = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
const DAY_ES = ["domingo", "lunes", "martes", "miércoles", "jueves", "viernes", "sábado"];

// Today in Boston, anchored at noon UTC so adding days never trips over a DST boundary.
function bostonToday(): Date {
  const s = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit",
  }).format(new Date());
  const [y, m, d] = s.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d, 12, 0, 0));
}

function prettyWindow(w: string, es: boolean): string {
  const m = w.match(/^(\d{1,2})\s*-\s*(\d{1,2})$/);
  if (!m) return w;
  return es ? `de ${m[1]} a ${m[2]}` : `${m[1]} to ${m[2]}`;
}

// The next slot we are willing to offer, as a phrase. Null when the rule yields nothing.
type Slot = { phrase: string; day: string; window: string };

// THE PREFERRED WINDOW (found 20 oct). Gary said "Afternoon" at 2:13pm; one minute later the
// bot offered "Thursday 9 to 11", then "Wednesday 9 to 11" — mornings, and Wednesday was a day
// he had already turned down. preferred_window was stored correctly the whole time; nothing read
// it. The window is now a first-class filter: look for the window they asked for across every
// open day first, and only widen to the other windows if that search comes back empty.
async function nextSlot(
  es: boolean,
  avoid: string[] = [],
  prefer: string | null = null,
): Promise<Slot | null> {
  const a = await loadAvailability();
  const want = prefer ? windowBucket(prefer, a.windows) : null;
  // Pass 1 honours the stated window. Pass 2 only runs when pass 1 found nothing at all.
  const passes: (string[])[] = want ? [[want], a.windows] : [a.windows];
  const base = bostonToday();
  for (const windows of passes) {
    for (let i = Math.max(1, a.lead_time_days); i <= a.max_days_out; i++) {
      const d = new Date(base.getTime() + i * 86_400_000);
      if (!a.days.includes(DAY_ABBR[d.getUTCDay()])) continue;
      // DECLINED SLOTS (8 oct). A lead who says "Working Friday" to "Friday 9 to 11" must never be
      // offered Friday again. Entries are either a full "Friday 10/9" or a bare weekday "Friday".
      const dayStr = `${DAY_EN[d.getUTCDay()]} ${d.getUTCMonth() + 1}/${d.getUTCDate()}`;
      if (avoid.some((x) => x.toLowerCase() === dayStr.toLowerCase() || x.toLowerCase() === DAY_EN[d.getUTCDay()].toLowerCase())) continue;
      const used = await slotLoad(isoDate(d));
      const raw = windows.find((w) => (used[w] ?? 0) < a.max_per_window);
      if (!raw) continue;
      const w = prettyWindow(raw, es);
      return {
        phrase: es ? `el ${DAY_ES[d.getUTCDay()]} ${w}` : `${DAY_EN[d.getUTCDay()]} ${w}`,
        // The date rides along with the day name. "Wednesday" offered on a Tuesday night and
        // accepted on Wednesday morning used to resolve to the FOLLOWING Wednesday.
        day: `${DAY_EN[d.getUTCDay()]} ${d.getUTCMonth() + 1}/${d.getUTCDate()}`,
        window: raw,
      };
    }
  }
  return null;
}

// ---------------------------------------------------------------------------------------
// CAPACITY. On Tue 6 Oct the bot booked Ramiro AND Duan into 9-11, and Mike AND Winnie both
// hold Wed 1-3. One crew cannot be in two towns at once, so both Tuesday jobs became no-shows.
// Every ticket now carries slot_date + slot_window, and the bot only offers a window whose
// live ticket count is under max_per_window (fadc.settings 'availability'). Raising the cap
// when Afek adds a crew is one UPDATE, not a deploy.
function isoDate(d: Date): string { return d.toISOString().slice(0, 10); }

// Any window the customer says ("8-9", "11", "before noon", "1-3pm") lands in one bucket.
function windowBucket(raw: string | null, windows: string[]): string | null {
  const t = (raw ?? "").trim().toLowerCase();
  if (!t) return null;
  if (/before noon|morning|ma[ñn]ana|\bam\b/.test(t) && !/\d/.test(t)) return windows[0] ?? null;
  const m = t.match(/(\d{1,2})/);
  if (!m) return /afternoon|tarde/.test(t) ? (windows[1] ?? null) : null;
  let h = Number(m[1]);
  if (/pm/.test(t) && h < 12) h += 12;
  else if (h >= 1 && h <= 7) h += 12;
  let best: string | null = null;
  for (const w of windows) {
    const wm = w.match(/^(\d{1,2})/);
    if (!wm) continue;
    let wh = Number(wm[1]); if (wh >= 1 && wh <= 7) wh += 12;
    if (wh <= h || best === null) best = w;
  }
  return best;
}

async function slotLoad(date: string, excludeLead?: string | null): Promise<Record<string, number>> {
  try {
    const rows = await sql`
      select slot_window, count(*)::int as n from fadc.appointments
      where slot_date = ${date} and status in ('pending', 'confirmed', 'rescheduled')
        and (${excludeLead ?? null}::uuid is null or lead_id <> ${excludeLead ?? null}::uuid)
      group by slot_window`;
    const out: Record<string, number> = {};
    for (const r of rows) out[String(r.slot_window)] = Number(r.n);
    return out;
  } catch (_) { return {}; }
}

async function nextSlotPhrase(es: boolean): Promise<string | null> {
  return (await nextSlot(es))?.phrase ?? null;
}

// ---------------------------------------------------------------------------------------
// ACCEPTING OUR OWN OFFER. At 2:41pm on 5 oct the agent offered Duan Le "Tuesday 9 to 11".
// At 3:29pm he answered "Yes Tuesday 9-11 should work". The agent replied "Great! Just to
// confirm, that's Tuesday after 5pm" — it contradicted the customer one second after he
// agreed, because a minute earlier he had also written "I am in a meeting but will be done
// after 5pm" and every hour-shaped string was being read as the appointment window.
//
// Two mistakes, both fixed in code:
//   1. the agent never remembered what IT had offered, so it could not recognise a yes
//   2. a time the customer can take a CALL was stored as the time for the JOB
//
// The offer is generated in code, so the acceptance is matched in code too. Nothing here
// asks the model to interpret anything.
const ACCEPT_RE =
  /^(ok(ay)?|sure|yes|yep|yeah|yup|perfect|great|sounds good|that works|works for me|fine|deal|book it|let'?s do it|s[ií]|claro|va|dale|perfecto|me funciona|est[áa] bien|dalee?)\b/i;
// Also catches "Yes Tuesday 9-11 should work" and "that day works", where the yes carries the
// slot back to us in the customer's own words.
const ACCEPT_LOOSE_RE =
  /\b(works for me|should work|that works|works great|sounds good|let'?s do (it|that)|book it|me funciona|s[ií] me (funciona|sirve)|est[áa] bien)\b/i;
// A yes that comes with a question is not a yes yet. "Yes but how much is it?" opened with
// "Yes" and would have locked the slot while the customer was still deciding.
const hitAccept = (t: string) =>
  !/[?¿]/.test(t) && (ACCEPT_RE.test(t.trim()) || ACCEPT_LOOSE_RE.test(t) || /^\s*(\u{1F44D}|\u{1F44C}|\u2705)/u.test(t));

// THE THUMBS-UP (found 7 oct). Digna answered with Messenger's like button, which arrives as a
// sticker with no text. It was treated as a photo: "Got the photo, thank you! The tech will take
// a proper look on site", twice, to a thumbs-up. A like is a yes, and goes to the agent as one.
const LIKE_STICKERS = new Set(["369239263222822", "369239343222814", "369239383222810"]);

// THE THUMBS-UP, SECOND PASS (found 20 oct). The 7 oct fix only read msg.sticker_id, and Meta
// does not always put it there: the like button arrives as TWO attachments, "image" + "sticker",
// with the id nested in the payload or only inside the image URL. describeAttachments saw the
// "image" and the customer got "Got the photo, thank you!" for a thumbs-up, 13 times in 3 weeks.
// Read every place the id can hide before deciding a message is a file.
function pickStickerId(msg: Record<string, any>): string | null {
  const read = (v: unknown) =>
    v === undefined || v === null || String(v).trim() === "" ? null : String(v).trim();
  const top = read(msg?.sticker_id);
  if (top) return top;
  const atts: any[] = Array.isArray(msg?.attachments) ? msg.attachments : [];
  for (const a of atts) {
    const nested = read(a?.payload?.sticker_id);
    if (nested) return nested;
  }
  // The like often carries its id only in the rendered image URL.
  for (const a of atts) {
    const u = String(a?.payload?.url ?? "");
    for (const id of LIKE_STICKERS) if (u.includes(id)) return id;
  }
  // No id anywhere, but Meta typed one of the parts "sticker": still a reaction, not a photo.
  return atts.some((a) => String(a?.type ?? "").toLowerCase() === "sticker") ? "unknown" : null;
}

// A time the customer can answer the PHONE. Never the time for the job.
const CALL_CONTEXT_RE =
  /\b(in a meeting|at work|working until|busy until|free after|available after|reach me after|call (me |us )?(back|today|tomorrow)?\s*(after|at|past)\b|give me a call after|i'?ll be (done|free|out) (at|after|by)|done after|off work|get out at)\b/i;
const CALL_CONTEXT_ES_RE =
  /(en (una )?junta|en (el )?trabajo|estoy ocupad|salgo a las|libre despu[ée]s|m[áa]rcame despu[ée]s|ll[áa]mame despu[ée]s|termino a las|desocupo)/i;
const hitCallContext = (t: string) => CALL_CONTEXT_RE.test(t) || CALL_CONTEXT_ES_RE.test(t);

// "will be done after 5pm" -> "after 5pm". Only used for the call note, never for the job.
function extractCallTime(t: string): string | null {
  const m = t.match(/\b(?:after|past|despu[ée]s de(?: las)?|a partir de(?: las)?)\s*(\d{1,2})\s*(:\d{2})?\s*(am|pm|a\.m\.|p\.m\.)?/i);
  if (!m) return null;
  const hour = m[1], mins = m[2] ?? "", ap = (m[3] ?? "").replace(/\./g, "").toLowerCase();
  const suffix = ap ? ap.toUpperCase() : (Number(hour) >= 8 && Number(hour) < 12 ? "AM" : "PM");
  return `after ${hour}${mins} ${suffix}`;
}

// The first date we are allowed to put a tech on. Same rule as nextSlotPhrase, as a Date.
//
// On 5 oct the agent told Winnie "I have an opening today 1-3" because she wrote "as soon as
// today". There was no tech, and Michel had to write to her 34 minutes later to move it. The
// prompt never said the agent could do that; it simply was not stopped. Availability is the
// one thing the agent cannot know, so the floor lives here and the agent cannot cross it.
async function earliestAllowedDate(): Promise<Date | null> {
  const a = await loadAvailability();
  const base = bostonToday();
  for (let i = Math.max(1, a.lead_time_days); i <= a.max_days_out; i++) {
    const d = new Date(base.getTime() + i * 86_400_000);
    if (a.days.includes(DAY_ABBR[d.getUTCDay()])) return d;
  }
  return null;
}


// ---------------------------------------------------------------------------------------
// DECLINING OUR OFFER (found 8 oct). Annette (01420) was offered "Friday 9 to 11" and wrote
// "Working Friday". The model answered "No problem! What day works best for you?", the slot net
// swapped that open question for the earliest slot, which was the same Friday, and she got the
// identical offer twice. The code never knew a slot had been turned down.
//
// Now: when our last message was an offer and the customer says no / can't / working / busy,
// that day (and any weekday they name) is remembered in fadc.leads.declined_slots and
// nextSlot() skips it. After two refusals the bot stops offering and asks the open question,
// and the team is told. The offer is cleared so a later "yes" can never book the refused day.
const OFFER_Q_RE = /(want me to hold it|hold it for you|te lo aparto|¿te lo aparto)/i;
// v72: talk like a person. Found in production 8 oct (Providencia, 01343): she asked "Are you in
// the area? Where exactly?", got a stock line plus a slot, said "You didn't answer my question.",
// got the SAME message again, said "No!", and got the same slot a third time. Three guards:
// a customer question switches the slot net off for that turn, frustration forces a second
// pass that apologises and answers, and no reply may repeat one we already sent.
const FRUSTRATED_RE = /(you (didn'?t|did not|don'?t|do not|never|haven'?t|have not) (answer|respond|listen|read|get it|understand)|(didn'?t|did not|not) answer(ing)? my question|answer my question|not what i (asked|said)|i (just )?(asked|said) (you )?(that|already)|are you (even )?(reading|listening)|stop (repeating|sending)|same (message|thing) again|you keep (saying|repeating|sending)|is this a (bot|robot|machine)|you('?re| are) a (bot|robot)|no contestaste|no me contestaste|no respondiste|no me respondiste|ya te (dije|pregunt[eé])|eso no (te )?pregunt[eé]|me repites|deja de repetir)/i;
function hitFrustrated(t: string, prevAgent: string, prevPrevAgent: string): boolean {
  if (FRUSTRATED_RE.test(t)) return true;
  // A bare "No!" right after we sent the same message twice in a row is frustration, not a decline.
  const bare = /^\s*(no+|nope|nah)\s*[!.]*\s*$/i.test(t);
  return bare && !!prevAgent && normReply(prevAgent) === normReply(prevPrevAgent);
}
const QUESTION_START_RE = /^\s*(where|what|which|who|when|why|how|are|is|do|does|did|can|could|will|would|should|have|has|d[oó]nde|qu[eé]|cu[aá]l|cu[aá]nto|c[oó]mo|cu[aá]ndo|est[aá]n|tienen|hacen|pueden|vienen)\b/i;
function isQuestion(t: string): boolean {
  return /[?\u00bf]/.test(t) || QUESTION_START_RE.test(t);
}
function normReply(t: string): string {
  return (t || "").toLowerCase().replace(/[^a-z0-9\u00e0-\u00ff ]/g, "").replace(/\s+/g, " ").trim();
}
function repeatsSent(reply: string, sent: string[]): boolean {
  const r = normReply(reply);
  if (!r) return false;
  return sent.some((x) => {
    const n = normReply(x);
    if (!n) return false;
    if (n === r) return true;
    // Same long tail (e.g. the same slot offer after a different first sentence).
    const tail = (z: string) => z.split(" ").slice(-9).join(" ");
    return r.split(" ").length >= 9 && n.split(" ").length >= 9 && tail(n) === tail(r);
  });
}
const DECLINE_EN_RE =
  /\b(can'?t|cannot|can not|won'?t|wont|nope|nah|no|not (available|possible|good|then|that day|this day)|unavailable|busy|working|at work|away|out of town|have plans|already (have|booked)|doesn'?t work|does not work|don'?t work|too (early|late)|another (day|time)|different (day|time)|other day|something else)\b/i;
const DECLINE_ES_RE =
  /(no puedo|no me funciona|no me sirve|no se puede|no tengo (tiempo|disponibilidad)|ese d[\u00edi]a no|ese d[\u00edi]a trabajo|trabajo|estoy ocupad[oa]|estoy fuera|otro d[\u00edi]a|otra hora|^\s*no\b)/i;
function hitDecline(t: string): boolean {
  const s = t.trim();
  if (!s) return false;
  if (/\b(no problem|no worries|not a problem|no rush)\b/i.test(s)) return false;
  // A pure question is the customer asking, not refusing: "can you do Monday?" stays with the model.
  if (/[?\u00bf]/.test(s) && !/\b(can'?t|cannot|won'?t|working|busy)\b/i.test(s)) return false;
  return DECLINE_EN_RE.test(s) || DECLINE_ES_RE.test(s);
}
// Weekday names the customer wrote, as English names ("Working Friday" -> ["Friday"]).
function weekdaysIn(t: string): string[] {
  const out: string[] = [];
  for (let i = 0; i < 7; i++) {
    const en = new RegExp(`\\b${DAY_EN[i]}s?\\b`, "i"), es = new RegExp(`(^|[^\\p{L}])${DAY_ES[i]}s?(?![\\p{L}])`, "iu");
    if (en.test(t) || es.test(t)) out.push(DAY_EN[i]);
  }
  return out;
}

const holdLine = (slot: string, es: boolean) =>
  es ? `tengo ${slot} libre, ¿te lo aparto?` : `I have ${slot} open, want me to hold it for you?`;

// Does this reply already put a concrete day in front of the customer?
const DAY_WORD_RE =
  /\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday|today|tomorrow|lunes|martes|mi[ée]rcoles|jueves|viernes|s[áa]bado|domingo|hoy|ma[ñn]ana)\b/i;

// The exact open questions the model keeps reaching for, so the net can swap them out.
const OPEN_DAY_RE =
  /(what day works best for you\?|what day works for you\?|which day works best( for you)?\?|what day would work( best)?( for you)?\?|¿qu[ée] d[í i]a te (funciona|queda|viene) mejor\?)/i;

// When we have to stall on a question, we still move the booking forward instead of
// dead-ending the chat. This picks the next thing we actually still need.
// v85: phone before zip (see buildStateNote above for why).
const ASK_LINES: Array<[string, string, string]> = [
  ["service", "what are you looking to get done, air ducts, dryer vent or chimney?", "¿qué quieres que te limpiemos, ductos, secadora o chimenea?"],
  // v86: these are the lines the CODE writes, so they set the register more than anything the
  // model produces. Softened from demands into offers — same information, no interrogation.
  // v87: zip back ahead of phone (see buildStateNote for the numbers).
  ["zip", "what's your ZIP? I'll check who's closest to you.", "¿cuál es tu código postal? Reviso quién te queda más cerca."],
  ["phone", "what's a good number to reach you?", "¿a qué número te puedo buscar?"],
  ["preferred_day", "what day works best for you?", "¿qué día te funciona mejor?"], // replaced by the slot offer when one exists
  ["preferred_window", "do mornings or afternoons work better?", "¿te funciona mejor en la mañana o en la tarde?"],
  ["first_name", "what name should I put it under?", "¿a nombre de quién la pongo?"],
  ["address", "what's the full street address?", "¿cuál es la dirección completa?"],
];

// v86: `justAsked` carries the buildStateNote field labels already asked in the last two agent
// messages. The code-written lines go through here too — the soft close after "I'll get back to
// you" re-asked Jeanmarie for her phone a fourth time — so they skip the same fields the model
// does. Returning null is fine: callers fall back to a line that does not demand anything.
const ASK_KEY_TO_LABEL: Record<string, string> = {
  service: "service",
  phone: "phone",
  zip: "ZIP / town",
  preferred_day: "preferred day",
  preferred_window: "time window",
  first_name: "first name",
  address: "full address",
};

function nextAskLine(
  lead: Record<string, unknown>,
  L: Record<string, string | null> | undefined,
  es: boolean,
  slot?: string | null,
  justAsked?: Set<string>,
): string | null {
  const have = (k: string) => {
    const a = L ? L[k] : null;
    const b = lead ? (lead[k] as string | null | undefined) : null;
    return !!((a ?? null) || (b ?? null));
  };
  for (const [k, qEn, qEs] of ASK_LINES) {
    const got = k === "zip" ? (have("zip") || have("town")) : have(k);
    if (got) continue;
    if (justAsked?.has(ASK_KEY_TO_LABEL[k] ?? k)) continue;
    // The day is the one field we never ask for in the open. Offer a slot instead; the
    // window comes with it, so there is nothing left to ask separately.
    if (k === "preferred_day" && slot) return holdLine(slot, es);
    if (k === "preferred_window" && slot && !have("preferred_day")) return holdLine(slot, es);
    return es ? qEs : qEn;
  }
  return null;
}

// Operational questions whose answers are NOT in the fact list. The prompt already says
// not to invent, and the model invented anyway across five separate QA scenarios: job
// duration ("2 to 3 hours"), how often to re-clean ("2-3 years"), equipment ("compressed
// air and a vacuum system"), furniture ("you don't need to move anything") and a warranty
// ("if you're not happy, we make it right"). Every one of those becomes a promise the
// technician has to keep at the door, so they get handed to a person instead.
const UNKNOWN_FACT_RE =
  /\b(how long (does|will|is)|how many hours|take all day|chemicals?|detergents?|safe for (my )?(pets?|cats?|dogs?|kids?|children|baby)|what (equipment|machine|machines|tools|products?) (do )?you use|what do you use|move (the )?furniture|move anything|do i need to (do|prepare|move)|financ(e|ing)|payment plans?|installments?|guarantee|warrant(y|ies)|money.?back|how often|how many years|licens(ed|e|ing)|bonded|certifi(ed|cation)|background.?check|who (is|exactly) (coming|comes))\b/i;

// Every net above was English-only, which meant a Spanish-speaking customer got NO net at
// all. QA in Spanish: "mi secadora esta echando humo y huele a quemado" got a scheduling
// question, and "cuanto tiempo dura el trabajo?" got an invented "entre 2 y 3 horas".
// A big share of FADC's customers write in Spanish, so every net gets a Spanish twin.
// Accented words are matched without a trailing \b (JS word boundaries are ASCII-only).
const EMERGENCY_ES_RE =
  /(echando humo|sale humo|saca humo|hace humo|humea|huele a quemado|olor a quemado|huele quemado|se est[\u00e1a] quemando|se quem[\u00f3o]|hay fuego|se prendi[\u00f3o]|incendio|llamas|chispas|chispea|chisporrote|\bhumo\b)/i;

const GAS_ES_RE =
  /(huele a gas|olor a gas|huele gas|fuga de gas|escape de gas|mon[\u00f3o]xido|detector de gas)/i;

const HARDSHIP_ES_RE =
  /(tercera edad|jubilad|pensionad|veteran|militar|discapacidad|discapacitad|ingreso fijo|ingresos fijos|descuento|cup[\u00f3o]n|m[\u00e1a]s barato|mejor precio|no me alcanza|poco presupuesto|bajo ingreso|no puedo pagar|me sale caro)/i;

const ESCALATE_ES_RE =
  /(comercial|bodega|almac[\u00e9e]n|edificio|oficinas?|condominio|administrador|moho|hongos?|sanitiza|desinfecta|asbesto|reclamo del seguro|aseguranza|abogad|demanda|queja|reembolso|gerente|supervisor|encargad)/i;

const UNKNOWN_FACT_ES_RE =
  /(cu[\u00e1a]nto (tiempo|dura|tarda|toma)|cu[\u00e1a]ntas horas|todo el d[\u00edi]a|qu[\u00edi]micos?|detergente|qu[\u00e9e] productos|es seguro para|seguro para (mis |mi )?(mascotas|gatos|perros|ni[\u00f1n]os|beb[\u00e9e])|qu[\u00e9e] (equipo|m[\u00e1a]quina|herramientas)|mover (los )?muebles|mover algo|tengo que (mover|preparar|hacer)|hay que mover|financiamiento|plan de pagos|a plazos|mensualidades|garant[\u00edi]a|devoluci[\u00f3o]n|cada cu[\u00e1a]nto|cada cu[\u00e1a]ntos a[\u00f1n]os|licencia|licenciad|certificad|antecedentes|qui[\u00e9e]n viene|qui[\u00e9e]n va a venir)/i;

// "Let me think about it" is NOT a no. It is the price objection in its politest form, and
// it lands exactly when we still have nothing to call them back with. First real lead on the
// live bot (3 oct): "I will get back once I decide" got "No problem at all, we're here
// whenever you're ready!" and a Connecticut job walked away with no ZIP and no phone.
const THINKING_RE =
  /\b(get back (to you|once|when)|let me think|think(ing)? about it|i[‘’]?ll let you know|(i|we) will let you know|not yet|(i[‘’]?ll|i will) reach out|(i[‘’]?ll|i will) (text|message|call|contact) you|when i’?m ready|not right now|for now|need to think|talk to my (wife|husband|partner|landlord|roommate)|check with my (wife|husband|partner|landlord|roommate)|maybe later|shop(ping)? around|compare|other quotes?|too expensive|a bit much|pricey|send (you )?(a )?(couple|few) (of )?dates|look at (my|the) calendar|check (my|the) calendar|send (you )?some dates)\b/i;

const THINKING_ES_RE =
  /(lo pienso|lo voy a pensar|d[\u00e9e]jame pensarlo|dejame pensarlo|te (aviso|digo|confirmo|contesto)|ahorita no|ahora no|por ahora no|m[\u00e1a]s adelante|mas adelante|luego te|preguntar a mi (esposa|esposo|pareja)|consultar con|comparar precios|otras cotizaciones|est[\u00e1a] caro|esta caro|muy caro|sale caro)/i;

const RESCHEDULE_ES_RE =
  /(reprogramar|cambiar (la cita|el d[\u00edi]a|la hora|la fecha)|mover la cita|mover la hora|posponer|cancelar|no puedo (ese|esa|ir)|otro d[\u00edi]a|otra hora|surgi[\u00f3o] algo|se me complic)/i;

// One helper per net, so every call site covers both languages automatically.
const hitEmergency = (t: string) => EMERGENCY_RE.test(t) || EMERGENCY_ES_RE.test(t) || GAS_ES_RE.test(t);
const hitGas = (t: string) => GAS_RE.test(t) || GAS_ES_RE.test(t);
const hitEscalate = (t: string) => ESCALATE_RE.test(t) || ESCALATE_ES_RE.test(t);
const hitUnknown = (t: string) => UNKNOWN_FACT_RE.test(t) || UNKNOWN_FACT_ES_RE.test(t);
const hitReschedule = (t: string) => RESCHEDULE_RE.test(t) || RESCHEDULE_ES_RE.test(t);
const hitThinking = (t: string) => THINKING_RE.test(t) || THINKING_ES_RE.test(t);

// A misclick, a wrong number, or someone who says plainly they are not interested. Kept
// DELIBERATELY NARROW: it has to be an explicit statement, never an inference. "Wrong" on its
// own would fire on "wrong vent" and hang up on a paying customer, so every pattern here needs
// the words that make the intent unambiguous.
// THE AREA NET (v61). A lead in NJ sent ZIP 08015, asked "Where are you located?", and the
// agent answered "We're based in Belmont, MA and we cover Massachusetts, Connecticut and Rhode
// Island." He wrote "Too far I'm in NJ" and the lead was gone. The agent never decides
// coverage: the owner finds a crew for every ZIP. Its only job is to keep collecting. So a
// question about where we are, or a "too far", is answered in code with one line and the next
// missing item, and any reply that states or implies "outside our area" is rewritten. Can only
// tighten: it never adds a refusal, it removes them.
const AREA_Q_RE =
  /\b(where (are|r) (you|u|y'all|you guys) (located|based|at|from)|where'?s (your|the) (office|shop|company|business|location)|where (is|are) (your|the) (office|shop|company|business|location|team|crew)s?( located| based)?|where exactly\b|are (you|u) (local|in the area|around here)|too far|how far (are|is) (you|u|your)|(you|u) (are|r|guys are) (so |very )?far|(do|can|would) (you|u|y'all|you guys) (come|travel|go|service|serve|cover|reach) (to|out|in|near)|are (you|u) (near|close))\b|d[oó]nde (est[aá]n|quedan|se ubican)|muy lejos|queda lejos|est[aá] lejos/i;
const AREA_LEAK_RE =
  /\bBelmont,? MA\b|\bwe only (cover|serve|service|work in|work)\b[^.?!]*\b(Massachusetts|Connecticut|Rhode Island)\b|\b(outside|beyond) (of )?(our|the) (service )?(area|range|zone)\b|\btoo far\b|\b(don'?t|do not|can'?t|cannot|unable to) (cover|serve|service|come to|reach)\b|\bnot (in|within) our (service )?area\b|\bonly (cover|serve|service|work)\b|\bbasados? en Belmont\b|\bfuera de (nuestra|la) zona\b/i;
const hitAreaQ = (t: string) => AREA_Q_RE.test(t);
const hitAreaLeak = (t: string) => AREA_LEAK_RE.test(t);
// MA 010-027 + 055, RI 028-029, CT 060-069. Anything else is outside the core states, so the
// owner is told to line up a crew. The agent keeps talking either way.
function isCoreZip(zip: string): boolean {
  const p = parseInt(zip.slice(0, 3), 10);
  return (p >= 10 && p <= 29) || p === 55 || (p >= 60 && p <= 69);
}

const MISCLICK_RE =
  /((sorry|oops|sry|my bad)[,!.\s]*\b(mistake|wrong)\b|\b(by|on) (mistake|accident)\b|\bclicked? (this |it |that )?(by|on) (mistake|accident)\b|\bdid ?n[o']?t mean to\b|\bwrong (number|person|page|chat|business|company)\b|\bnot interested\b|\bno longer interested\b|\bunsubscribe\b|\bstop (messaging|texting|contacting) me\b|\bit was a mistake\b|\bmisclick)/i;
const MISCLICK_ES_RE =
  /(fue un error|me equivoqu[ée]|perd[óo]n[,\s]*(fue un )?error|sin querer|por error|n[úu]mero equivocado|no me interesa|ya no me interesa|no quiero nada|d[ée]jenme en paz|no me (escriban|manden) m[áa]s)/i;
const hitMisclick = (t: string) => MISCLICK_RE.test(t) || MISCLICK_ES_RE.test(t);

// How long the lead asked for. Only two buckets, because guessing finer is how a cron ends up
// nudging someone who already told us to wait. Anything that names a week or more gets six days;
// everything else gets the 44-hour default set at the call site.
const LONG_WAIT_RE = /\b(next week|in a week|a week|couple of weeks|two weeks|next month|after the holidays?|end of the month)\b/i;
const LONG_WAIT_ES_RE = /(la (pr[\u00f3o]xima )?semana|en una semana|un par de semanas|dos semanas|el (pr[\u00f3o]ximo )?mes|despu[\u00e9e]s de (las fiestas|vacaciones)|fin de mes)/i;
const hitLongWait = (t: string) => LONG_WAIT_RE.test(t) || LONG_WAIT_ES_RE.test(t);
const firstMatch = (t: string, ...res: RegExp[]) => {
  for (const re of res) { const m = t.match(re); if (m) return m[0]; }
  return null;
};

// Someone asking to move or change an appointment. Used both to steer the agent and
// to raise a loud alert when the lead is already paused with a human.
const RESCHEDULE_RE =
  /\b(reschedul\w*|move it|move the appointment|change the (time|day|date|appointment)|push it|postpone|cancel|something came up|can'?t make it|different day|another day|instead of (monday|tuesday|wednesday|thursday|friday|saturday|sunday))\b/i;

// Scan the customer's own messages (not ours) for a hardship signal or 3+ systems.
function detectSpecialPricing(history: { role: string; text: string }[], latest: string): { hit: boolean; why: string } {
  const customerText = [
    latest,
    ...history.filter((m) => m.role === "customer").map((m) => m.text),
  ].join("\n");

  const hardship = firstMatch(customerText, HARDSHIP_RE, HARDSHIP_ES_RE);
  if (hardship) return { hit: true, why: `hardship/discount signal: "${hardship}"` };

  const n = extractSystemCount(customerText);
  if (n !== null && n >= 3) return { hit: true, why: `${n} systems (3+ needs a human package, not $99xN)` };

  return { hit: false, why: "" };
}

// Michel's rule: a dash in the middle of a sentence is the clearest tell that a message
// was written by a machine. The prompt bans it, and this strips it from every outgoing
// reply anyway, because the model reaches for an em dash constantly. Time ranges keep a
// plain hyphen (9-11); prose dashes become a comma or a sentence break.
// The two fillers that most give a machine away, scrubbed on the way out. Both are banned
// in the prompt; the model wrote one anyway the first time it saw a photo.
function scrubFillers(reply: string): string {
  return reply
    .replace(/\bI(?:'|\u2019)?d be happy to help\b/gi, "happy to help")
    .replace(/\bI(?:'|\u2019)?d be happy to\b/gi, "happy to")
    .replace(/\bGreat question!\s*/gi, "")
    .replace(/\s{2,}/g, " ")
    .trim()
    .replace(/^(\W*)(\w)/, (_m, pre, c) => pre + c.toUpperCase());
}

// The sign-off the model reaches for when someone hesitates. It is the right line for a
// lead who actually declined and the wrong one for a lead weighing the price, so it gets
// cut out of the reply while whatever answered their question is kept.
const GOODBYE_RE =
  /(we'?re here whenever|here whenever you|we'?ll be here|no problem at all|take your time|whenever you'?re ready|whenever you need us|reach out when|just let us know when|aqu[\u00ed i] estamos cuando|cuando gustes|cuando est[\u00e9e]s list|avisa cuando|sin problema)/i;

function dropGoodbye(reply: string): string {
  const kept = reply.split(/(?<=[.!?])\s+/).filter((sentence) => !GOODBYE_RE.test(sentence));
  return kept.join(" ").trim();
}

function stripDashes(reply: string): string {
  return reply
    .replace(/(\d)\s*[\u2013\u2014]\s*(\d)/g, "$1-$2")
    .replace(/\s+[\u2013\u2014]\s+/g, ", ")
    .replace(/[\u2013\u2014]/g, ", ")
    .replace(/(\S)\s+-\s+(\S)/g, "$1, $2")
    .replace(/\s+,/g, ",")
    .replace(/,\s*,+/g, ", ")
    .replace(/,\s*([.!?])/g, "$1")
    .replace(/([.!?])\s*,/g, "$1")
    .replace(/\s{2,}/g, " ")
    .trim();
}

// Any dollar figure of $150+ in a reply means the model multiplied out a multi-system
// total. QA: "So 4 systems would be $396 right?" got "Yes, that's right." The flag fired,
// but the customer already had the number in writing — which is the whole failure the
// special-pricing rule exists to prevent.
function quotedBigTotal(reply: string): number | null {
  for (const m of reply.matchAll(/\$\s?(\d[\d,]*)/g)) {
    const n = parseInt(m[1].replace(/,/g, ""), 10);
    if (Number.isFinite(n) && n >= 150) return n;
  }
  return null;
}

async function callClaude(history: { role: string; text: string }[], stateNote?: string): Promise<AgentOut> {
  const messages = history.map((m) => ({
    role: m.role === "customer" ? "user" : "assistant",
    content: m.text,
  }));
  // Anthropic requires alternating roles starting with user; merge consecutive same-role turns.
  const merged: { role: string; content: string }[] = [];
  for (const m of messages) {
    const last = merged[merged.length - 1];
    if (last && last.role === m.role) last.content += "\n" + m.content;
    else merged.push({ ...m });
  }
  if (merged.length === 0 || merged[0].role !== "user") merged.unshift({ role: "user", content: "(customer replied to the ad)" });
  // Append the known-state note as the final user turn so it sits closest to the model's answer.
  if (stateNote) {
    const last = merged[merged.length - 1];
    if (last && last.role === "user") last.content += "\n\n" + stateNote;
    else merged.push({ role: "user", content: stateNote });
  }
  // Prefill the assistant turn with "{" so Claude must continue directly as JSON —
  // this is the robust fix for occasional prose-before-JSON breaks on edge cases
  // (discount asks, chimney, non-customer messages), far more reliable than prompting alone.
  merged.push({ role: "assistant", content: "{" });

  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "x-api-key": ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: MODEL,
      max_tokens: 600,
      temperature: 0.4,
      system: SYSTEM_PROMPT,
      messages: merged,
    }),
  });
  if (!res.ok) throw new Error(`anthropic ${res.status}: ${await res.text()}`);
  const data = await res.json();
  const continued: string = data.content?.[0]?.text ?? "";
  // Re-attach the "{" we prefilled, since the API response only contains the continuation.
  const text = "{" + continued;
  const cleaned = text.trim().replace(/^```(?:json)?\s*/i, "").replace(/```\s*$/, "");
  let parsed: AgentOut;
  try {
    parsed = JSON.parse(cleaned) as AgentOut;
  } catch (e) {
    // Fallback: extract the outermost {...} in case there's still stray text around it.
    const start = cleaned.indexOf("{");
    const end = cleaned.lastIndexOf("}");
    if (start >= 0 && end > start) {
      parsed = JSON.parse(cleaned.slice(start, end + 1)) as AgentOut;
    } else {
      throw new Error(`bad json from model: ${cleaned.slice(0, 200)}`);
    }
  }
  parsed.reply = (parsed.reply ?? "").trim();
  parsed.lead = parsed.lead ?? {};
  return parsed;
}

// Every alert is persisted to fadc.notifications FIRST, then pushed to Telegram if
// it is configured. Without this, any alert raised while TELEGRAM_* secrets are unset
// disappears silently — which is how the team ends up blind to what the bot flagged.
// Read them with:  select * from fadc.notifications where not seen order by created_at desc;
async function notifyTeam(text: string, kind = "info") {
  let delivered = false;
  if (TG_TOKEN && TG_CHAT) {
    try {
      const res = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ chat_id: TG_CHAT, text }),
      });
      delivered = res.ok;
    } catch (_) { /* fall through to the DB record */ }
  }
  try {
    const lid = leadCtx.getStore()?.leadId ?? null;
    await sql`insert into fadc.notifications (kind, text, delivered_telegram, lead_id)
              values (${kind}, ${text}, ${delivered}, ${lid}::uuid)`;
  } catch (_) { /* never block the reply on a notification */ }
}

// Same as notifyTeam, but for alerts raised OUTSIDE a request (cron sweeps), where the
// AsyncLocalStorage lead context is empty. The lead id has to be explicit, otherwise the
// notification lands with lead_id null and the "have I already alerted on this lead?"
// dedup check in runLeakAlert can never match.
async function notifyTeamFor(leadId: string, text: string, kind: string) {
  let delivered = false;
  if (TG_TOKEN && TG_CHAT) {
    try {
      const res = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ chat_id: TG_CHAT, text }),
      });
      delivered = res.ok;
    } catch (_) { /* fall through to the DB record */ }
  }
  try {
    await sql`insert into fadc.notifications (kind, text, delivered_telegram, lead_id)
              values (${kind}, ${text}, ${delivered}, ${leadId}::uuid)`;
  } catch (_) { /* never block the sweep on a notification */ }
}

// Send a text message back to the customer through the Messenger Send API.
async function sendMessengerReply(recipientId: string, text: string, tag?: string) {
  if (!META_PAGE_TOKEN || !text) return;
  if (!BOT_LIVE_MODE) {
    // Kill-switch is OFF: log what WOULD have been sent, but never actually send it.
    await notifyTeam(`🧪 [TEST MODE, not sent] Would reply to lead:\n"${text}"`, "test_mode");
    return;
  }
  const payload: Record<string, unknown> = {
    recipient: { id: recipientId },
    messaging_type: tag ? "MESSAGE_TAG" : "RESPONSE",
    message: { text },
  };
  if (tag) payload.tag = tag;
  const res = await fetch(`https://graph.facebook.com/${GRAPH_VERSION}/me/messages?access_token=${META_PAGE_TOKEN}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`send api ${res.status}: ${await res.text()}`);
}

// Meta closes the free-form 24h window after the customer's last message. A reschedule or
// appointment-change notice is still allowed outside that window under the CONFIRMED_EVENT_UPDATE
// tag — Meta's exception for messages about an existing booking. Try a normal reply first (cheapest,
// no restrictions on content); only fall back to the tag when the window is actually closed, since
// tagged sends are policy-restricted to genuine appointment updates.
async function sendAppointmentUpdate(recipientId: string, text: string) {
  try {
    await sendMessengerReply(recipientId, text);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (/cannot send messages to this id|outside of allowed window|24 hour/i.test(msg)) {
      await sendMessengerReply(recipientId, text, "CONFIRMED_EVENT_UPDATE");
    } else {
      throw err;
    }
  }
}

// ---------------------------------------------------------------------------------------
// COMMENTS. Kim Kat commented "Tolland ct?" on the dryer-vent ad on 4 oct and nothing
// happened: comments never reached this function, so she was invisible to the bot and only
// visible in an inbox nobody was watching. A comment is a lead who raised their hand in
// public, and Meta gives a comment-initiated thread a SEVEN day window instead of 24 hours,
// so it is the most valuable door we have and it was the only one nailed shut.
//
// What this does, deliberately narrow: it answers in a PRIVATE message, never in public.
// A public reply as the business page is Michel's call, not the bot's, so the bot alerts him
// with the comment text and he posts whatever he wants to post.
//
// The opener is a constant, not model output. On a first touch there is no history, no name
// for the service, and nothing to ground a generated answer: a fixed line that states the
// two prices cannot invent a third price. It never states a coverage area: the owner finds a crew for every ZIP.
const COMMENT_OPENER = (name: string | null) =>
  `Hi${name ? " " + name : ""}! Thanks for your comment on our ad, I'm replying here so I can help you properly. ` +
  `Air duct cleaning is $99 per heating/AC system and dryer vent cleaning is $99 flat, both with a free inspection. ` +
  `What's your ZIP code so I can get the right crew for your area?`;

// A complaint, an accusation or an argument never gets an automated answer. These go to a
// person, because the cost of a wrong public-adjacent reply to an angry customer is not the
// lead, it is the page.
const COMMENT_NO_AUTO_RE =
  /\b(scam|scamm?er|fraud|rip ?off|ripped me off|never showed|no show|refund|lawsuit|sue|attorney|better business|bbb|terrible|awful|worst|horrible|rude|damaged|broke my|stole|liar|lying|avoid this|do not use|don'?t use)\b/i;
const COMMENT_NO_AUTO_ES_RE =
  /(estafa|fraude|ladr[óo]n|no llegaron|no se present|reembolso|demanda|abogado|p[ée]simo|horrible|grosero|me rompieron|me robaron|mentir)/i;

async function sendPrivateReplyToComment(
  commentId: string,
  text: string,
): Promise<{ psid: string | null }> {
  if (!META_PAGE_TOKEN || !text) return { psid: null };
  if (!BOT_LIVE_MODE) {
    await notifyTeam(`\u{1F9EA} [TEST MODE, not sent] Would privately reply to comment ${commentId}:\n"${text}"`, "test_mode");
    return { psid: null };
  }
  // recipient.comment_id is what opens the 7-day thread. It is a different recipient shape
  // from recipient.id, which is why this does not go through sendMessengerReply.
  const res = await fetch(`https://graph.facebook.com/${GRAPH_VERSION}/me/messages?access_token=${META_PAGE_TOKEN}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ recipient: { comment_id: commentId }, message: { text } }),
  });
  const body = await res.text();
  if (!res.ok) throw new Error(`private reply ${res.status}: ${body}`);
  // Meta answers with {"recipient_id": "<PSID>", "message_id": "..."}. That PSID is the only
  // way to tie this outbound message to a lead row: at comment time the thread does not exist
  // yet, so there is no PSID to address and nothing to log against. We used to drop this
  // response, and the cost is in the comment below on handleComment.
  try {
    const psid = String(JSON.parse(body)?.recipient_id ?? "").trim();
    return { psid: psid || null };
  } catch {
    return { psid: null };
  }
}

async function handleComment(value: Record<string, any>): Promise<void> {
  if (value?.item !== "comment" || value?.verb !== "add") return;
  const commentId = String(value.comment_id ?? "").trim();
  if (!commentId) return;
  const fromId = String(value.from?.id ?? "").trim();
  const fromName = (value.from?.name ?? null) as string | null;
  const text = String(value.message ?? "").trim();

  // Net 1: never answer ourselves. Without this the page talks to itself in a loop.
  if (fromId && fromId === META_PAGE_ID) return;

  // Net 2: never answer twice. The primary key does the work, so a Meta redelivery (which
  // happens, and happens hours later) cannot produce a second message to the same person.
  const claimed = await sql`
    insert into fadc.comment_replies (comment_id, post_id, from_id, from_name, comment_text)
    values (${commentId}, ${String(value.post_id ?? "") || null}, ${fromId || null}, ${fromName}, ${text || null})
    on conflict (comment_id) do nothing
    returning comment_id`;
  if (claimed.length === 0) return; // already seen

  const firstName = fromName ? fromName.split(/\s+/)[0] : null;

  // Net 3: a complaint goes to a person, with the words they used, and the bot stays out.
  if (COMMENT_NO_AUTO_RE.test(text) || COMMENT_NO_AUTO_ES_RE.test(text)) {
    await sql`update fadc.comment_replies set reason = 'complaint, human only' where comment_id = ${commentId}`;
    await notifyTeam(
      `\u{1F6A8} NEGATIVE COMMENT on the ad, the bot did NOT reply and will not.\n` +
      `${fromName ?? "Someone"} wrote: "${text.slice(0, 300)}"\n` +
      `Answer this one yourself, publicly, before it sits there.`,
      "comment_negative",
    );
    return;
  }

  // An empty comment (a sticker, a tag, a bare emoji) is not a question. Log it, do not chase it.
  if (text.replace(/[\s\p{Emoji_Presentation}\p{Extended_Pictographic}]/gu, "").length < 2) {
    await sql`update fadc.comment_replies set reason = 'no text' where comment_id = ${commentId}`;
    return;
  }

  try {
    // JENNIFER JUDGE (found 19 oct). The opener went out and was never written to
    // fadc.messages, because a comment has no PSID to hang a lead off. She answered "02641"
    // and the model built its reply from a history of exactly one line, hers. To the model
    // that was the first message of a brand new conversation, so it greeted her a second
    // time: "Hi Jennifer! ..." and then "Hey! What service are you looking for...".
    // The PSID in the send response fixes the ordering: lead first, then the opener on it.
    const opener = COMMENT_OPENER(firstName);
    const { psid } = await sendPrivateReplyToComment(commentId, opener);
    let leadId: string | null = null;
    if (psid) {
      const lastName = fromName ? (fromName.split(/\s+/).slice(1).join(" ") || null) : null;
      const [lead] = await sql<{ id: string }[]>`
        insert into fadc.leads (messenger_id, manychat_id, first_name, last_name, last_outbound_at)
        values (${psid}, ${psid}, ${firstName}, ${lastName}, now())
        on conflict (messenger_id) do update
          set first_name = coalesce(fadc.leads.first_name, excluded.first_name),
              last_name  = coalesce(fadc.leads.last_name,  excluded.last_name),
              last_outbound_at = now()
        returning id`;
      leadId = lead?.id ?? null;
      if (leadId) {
        await sql`insert into fadc.messages (lead_id, role, text, sent_by)
                  values (${leadId}, 'agent', ${opener}, 'comment_opener')`;
      }
    }
    await sql`update fadc.comment_replies set replied = true, lead_id = ${leadId} where comment_id = ${commentId}`;
    await notifyTeam(
      `\u{1F4AC} New comment on the ad, the bot sent a private message and the 7-day window is open.\n` +
      `${fromName ?? "Someone"} commented: "${text.slice(0, 200)}"\n` +
      `Reply to the comment publicly when you get a chance, the bot does not post in public.`,
      "comment_handled",
    );
  } catch (e) {
    const msg = String(e).slice(0, 200);
    await sql`update fadc.comment_replies set reason = ${"send failed: " + msg} where comment_id = ${commentId}`;
    await notifyTeam(
      `\u{26A0}\u{FE0F} A comment came in and the private reply FAILED, so this lead is waiting with nothing.\n` +
      `${fromName ?? "Someone"}: "${text.slice(0, 160)}"\n${msg}`,
      "comment_failed",
    );
  }
}

// Show "typing..." bubble to the customer while the agent processes.
async function sendTypingOn(recipientId: string) {
  if (!META_PAGE_TOKEN || !BOT_LIVE_MODE) return;
  try {
    await fetch(`https://graph.facebook.com/${GRAPH_VERSION}/me/messages?access_token=${META_PAGE_TOKEN}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        recipient: { id: recipientId },
        sender_action: "typing_on",
      }),
    });
  } catch (_) { /* typing indicator is best-effort */ }
}

// PSID RESOLUTION (found 14 oct). Meta assigns different PSIDs to the same person
// depending on the entry point: ad click vs Business Suite thread vs comment reply.
// James Froio had PSID A from his ad click and PSID B when Michel messaged him from
// Business Suite. The system created two separate leads and lost the entire conversation.
// This helper checks messenger_id first, then alt_messenger_ids, then profile name.
// When a match is found by alt or name, it adds the new PSID to alt_messenger_ids.
async function resolveLeadByPsid(
  psid: string,
  profileName?: { first_name: string | null; last_name: string | null } | null,
): Promise<{ id: string; first_name: string | null } | null> {
  // 1. Direct match on messenger_id
  const [byMain] = await sql`select id, first_name from fadc.leads where messenger_id = ${psid}`;
  if (byMain) return byMain;
  // 2. Match on alt_messenger_ids
  const [byAlt] = await sql`select id, first_name, messenger_id from fadc.leads where ${psid} = ANY(alt_messenger_ids)`;
  if (byAlt) return byAlt;
  // 3. Match by profile name (only if we have a first + last name, and the lead was created recently)
  if (profileName?.first_name && profileName?.last_name) {
    const [byName] = await sql`
      select id, first_name, messenger_id from fadc.leads
      where first_name = ${profileName.first_name}
        and last_name = ${profileName.last_name}
        and created_at > now() - interval '30 days'
        and stage not in ('closed', 'merged', 'misclick')
      order by last_inbound_at desc nulls last
      limit 1`;
    if (byName) {
      // Found by name — register this PSID as an alias
      await sql`update fadc.leads
        set alt_messenger_ids = array_append(alt_messenger_ids, ${psid})
        where id = ${byName.id} and not (${psid} = ANY(alt_messenger_ids))`;
      await notifyTeam(
        `🔗 PSID duplicado detectado para ${byName.first_name}. Nuevo PSID ${psid.slice(-6)} vinculado al lead existente.`,
        "psid_merge",
      );
      return byName;
    }
  }
  return null;
}

// Handle echo events: when Michel (or any human) sends a message from the
// Business Suite inbox, Meta fires a webhook with message.is_echo = true.
// We detect this and pause the agent so the bot doesn't reply on top.
async function handleEcho(ev: Record<string, any>): Promise<void> {
  const msg = ev.message;
  if (!msg || typeof msg.text !== "string" || !msg.text.trim()) return;
  // In echo events, sender.id = page, recipient.id = customer
  const customerId = String(ev.recipient?.id ?? "").trim();
  if (!customerId) return;
  const mid = msg.mid ? String(msg.mid) : null;
  const body = msg.text.trim();
  // This used to read `if (msg.app_id) return;`, on the assumption that app_id meant the echo
  // was our own API send. It is not: Meta sets app_id on anything sent through an app, its own
  // Business Suite and Messenger apps included. So every manual reply Michel wrote from his
  // phone was thrown away, the agent never paused, and the bot would answer on top of him.
  // Found 3 oct: in the whole history there was not one real human message captured.
  // The reliable test does not guess at app_id. We save every reply we send, so if this exact
  // text is already on file as ours, the echo is ours. Anything else came from a person.
  try {
    // Our private reply to a comment is never stored as a message (the commenter has no
    // thread yet), so it must be recognised by its fixed text before anything else. On 6 oct
    // Michel commented as a test, already had a lead row from earlier chats, and the bot's own
    // opener echoed back as "a person replied", pausing him.
    if (body.includes("Thanks for your comment on our ad")) return;
    // Try to resolve this PSID to an existing lead (checks messenger_id, alt_messenger_ids,
    // and profile name). This prevents the duplicate-PSID bug found 14 oct with James Froio.
    let profile: { first_name: string | null; last_name: string | null } | null = null;
    let lead = await resolveLeadByPsid(customerId);
    if (!lead) {
      // No match by PSID — try profile name before creating a new lead
      try { profile = await getMessengerProfile(customerId); } catch (_) { /* best-effort */ }
      if (profile?.first_name) {
        lead = await resolveLeadByPsid(customerId, profile);
      }
    }
    if (!lead) {
      // THE OLD-THREAD GAP (found 6 oct). A person can write by hand to someone who talked to
      // the page before the bot existed. That customer has no lead row yet, so this used to
      // return here, nothing was paused, and when Chris Murphy (booked by phone for Wed 8-9)
      // answered Ilay with "Yes", the bot greeted him from zero and re-asked everything.
      // Our own private reply to a comment is the one message we send to a stranger, so that
      // one is skipped by its fixed text. Anything else from the page to an unknown customer
      // is a person, and the lead is created already paused.
      [lead] = await sql`
        insert into fadc.leads (messenger_id, manychat_id, agent_paused, last_inbound_at)
        values (${customerId}, ${customerId}, true, now())
        on conflict (messenger_id) do update set agent_paused = true
        returning id, first_name`;
      if (!lead) return;
    }
    setCtxLead(lead.id);
    const isOurs = async () => (await sql`
      select 1 from fadc.messages
      where lead_id = ${lead.id} and sent_by in ('agent', 'followup', 'crm') and text = ${body}
        and created_at > now() - interval '15 minutes'
      limit 1`).length > 0;
    if (await isOurs()) return; // our own echo coming back
    // THE ECHO RACE (found 6 oct). Meta can deliver the echo of OUR message before the row that
    // records it is written. The 9:00 follow-up to a lead in 08816 echoed back 1.7 seconds before
    // its row existed, got filed as a person, and paused the lead, so the customer's reply would
    // have met a silent bot. Wait a few seconds and look again before calling it a human.
    await new Promise((r) => setTimeout(r, 5000));
    if (await isOurs()) return;
    await sql`update fadc.leads set agent_paused = true where id = ${lead.id}`;
    // Save the human message so the agent has full context if resumed later
    if (mid) {
      await sql`
        insert into fadc.messages (lead_id, role, text, sent_by, meta_mid)
        values (${lead.id}, 'agent', ${body}, 'human', ${mid})
        on conflict (meta_mid) where meta_mid is not null do nothing`;
    } else {
      await sql`
        insert into fadc.messages (lead_id, role, text, sent_by)
        values (${lead.id}, 'agent', ${body}, 'human')`;
    }
    await notifyTeam(`🤚 A person replied manually to ${lead.first_name ?? "a lead"} in the Meta inbox. The bot is now PAUSED on this lead and will not answer them again.\n"${body.slice(0, 160)}"`, "human_takeover");
  } catch (_) { /* best-effort */ }
}

// Best-effort: get the customer's first/last name so the agent can greet them.
async function getMessengerProfile(psid: string): Promise<{ first_name: string | null; last_name: string | null }> {
  if (!META_PAGE_TOKEN) return { first_name: null, last_name: null };
  try {
    const res = await fetch(`https://graph.facebook.com/${GRAPH_VERSION}/${psid}?fields=first_name,last_name&access_token=${META_PAGE_TOKEN}`);
    if (!res.ok) {
      const body = await res.text();
      // This used to alert on EVERY inbound message. On 3 oct it produced ~20 Telegram
      // messages in six minutes and buried the one 🙋 NEEDS A PERSON that carried a real
      // booking. An alert nobody can find is worse than no alert.
      // Only a malformed/expired token is worth waking someone for, because that token is
      // also what sends replies. "Object with ID ... does not exist" just means we lack the
      // profile permission for that user: the name stays null and the bot works fine.
      const tokenBroken = /Expected 1 '\.'|Error validating access token|Session has expired|has not authorized application/i.test(body);
      const now = Date.now();
      if (tokenBroken && now - lastProfileAlert > 1_800_000) {
        lastProfileAlert = now;
        await notifyTeam(
          `\u{1F464} The page token looks broken (HTTP ${res.status}), and the SAME token is what sends replies, so the bot may be mute.\n${body.slice(0, 200)}`,
          "profile_fail",
        );
      }
      return { first_name: null, last_name: null };
    }
    const data = await res.json();
    return { first_name: data.first_name ?? null, last_name: data.last_name ?? null };
  } catch (e) {
    await notifyTeam(`\u{1F464} Messenger profile call failed: ${String(e).slice(0, 160)}`, "profile_fail");
    return { first_name: null, last_name: null };
  }
}

// Verify Meta's X-Hub-Signature-256 header against the app secret. Optional: only enforced when META_APP_SECRET is set.
async function verifyMetaSignature(rawBody: string, signatureHeader: string | null): Promise<boolean> {
  if (!META_APP_SECRET) return true; // not configured yet → allow (testing)
  if (!signatureHeader || !signatureHeader.startsWith("sha256=")) return false;
  const expected = signatureHeader.slice("sha256=".length);
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(META_APP_SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(rawBody));
  const hex = Array.from(new Uint8Array(sig)).map((b) => b.toString(16).padStart(2, "0")).join("");
  return hex === expected;
}

// A message that is only attachments. Handled in code, not by the model: there is no text to
// reason about, and the one correct move is to acknowledge it, keep the booking moving, and
// put the file in front of a person.
async function processAttachment(input: {
  messengerId: string;
  att: { label: string; kinds: string[]; urls: string[] };
  firstName: string | null;
  lastName: string | null;
  metaMid?: string | null;
}): Promise<{ reply: string }> {
  const { messengerId, att, firstName, lastName, metaMid } = input;
  const t0 = Date.now();
  let leadId: string | null = null;
  try {
    // Resolve PSID to existing lead (duplicate-PSID fix, 14 oct)
    const resolved = await resolveLeadByPsid(
      messengerId,
      firstName && lastName ? { first_name: firstName, last_name: lastName } : null,
    );
    let effectiveMessengerId = messengerId;
    if (resolved) {
      const [check] = await sql`select messenger_id from fadc.leads where id = ${resolved.id} and messenger_id != ${messengerId}`;
      if (check) effectiveMessengerId = check.messenger_id;
    }

    const [lead] = await sql`
      insert into fadc.leads (messenger_id, manychat_id, first_name, last_name, last_inbound_at)
      values (${effectiveMessengerId}, ${effectiveMessengerId}, ${firstName}, ${lastName}, now())
      on conflict (messenger_id) do update
        set last_inbound_at = now(),
            first_name = coalesce(fadc.leads.first_name, excluded.first_name),
            last_name  = coalesce(fadc.leads.last_name,  excluded.last_name)
      returning id, stage, agent_paused, first_name, service, zip, town, phone,
                preferred_day, preferred_window, address, declined_slots`;
    leadId = lead.id;

    // Register alt PSID if different from primary
    if (effectiveMessengerId !== messengerId) {
      await sql`update fadc.leads
        set alt_messenger_ids = array_append(alt_messenger_ids, ${messengerId})
        where id = ${lead.id} and not (${messengerId} = ANY(alt_messenger_ids))`;
    }
    setCtxLead(leadId);

    if (metaMid) {
      const rows = await sql`
        insert into fadc.messages (lead_id, role, text, sent_by, meta_mid)
        values (${leadId}, 'customer', ${att.label}, 'customer', ${metaMid})
        on conflict (meta_mid) where meta_mid is not null do nothing returning id`;
      if (rows.length === 0) return { reply: "" };
    } else {
      await sql`insert into fadc.messages (lead_id, role, text, sent_by)
                values (${leadId}, 'customer', ${att.label}, 'customer')`;
    }

    const head = `\u{1F4CE} ${lead.first_name ?? "A lead"} sent ${att.kinds.join(", ") || "an attachment"}`;
    const links = att.urls.length ? "\n" + att.urls.slice(0, 3).join("\n") : "";

    if (lead.agent_paused) {
      await notifyTeam(`${head}${links}\nThe agent is paused on this lead, so nobody replied. Open the Meta inbox.`, "attachment");
      return { reply: "" };
    }

    const prior = await sql`select role, text from fadc.messages where lead_id = ${leadId}
                            order by created_at desc limit 30`;
    const es = isSpanish((prior as { role: string; text: string }[])
      .filter((m) => m.role === "customer").map((m) => m.text).join("\n"));
    const say = (p: Pair) => (es ? p.es : p.en);
    // A like arrives as "image" + "sticker" together. The image is the sticker drawn, not a photo,
    // so a sticker anywhere in the set disqualifies the photo copy even if detection upstream missed.
    const isPhoto = att.kinds.some((k) => k === "image" || k === "video") &&
      !att.kinds.includes("sticker");
    // A thumbs-up used to re-offer a day the customer had already refused, in the wrong window.
    const slotObj = await nextSlot(
      es,
      Array.isArray(lead.declined_slots) ? (lead.declined_slots as string[]) : [],
      (lead.preferred_window as string | null) ?? null,
    );
    const slot = slotObj?.phrase ?? null;
    const ask = nextAskLine(lead as unknown as Record<string, unknown>, undefined, es, slot);
    const reply = ask
      ? `${say(isPhoto ? ATTACH_COPY.photo : ATTACH_COPY.other)}. ${cap(ask)}`
      : say(ATTACH_COPY.done);

    await sql`insert into fadc.messages (lead_id, role, text, sent_by)
              values (${leadId}, 'agent', ${reply}, 'agent')`;
    await sql`update fadc.leads
              set last_outbound_at = now(),
                  stage = case when stage = 'new' then 'qualifying' else stage end
              where id = ${leadId}`;
    await notifyTeam(`${head}${links}\nThe agent acknowledged it and kept qualifying. Look at the file in the Meta inbox.`, "attachment");
    await sql`insert into fadc.agent_runs (lead_id, inbound_text, model, reply_sent, latency_ms)
              values (${leadId}, ${att.label}, 'code:attachment', ${reply}, ${Date.now() - t0})`;
    return { reply };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    try {
      await sql`insert into fadc.agent_runs (lead_id, inbound_text, model, error, latency_ms)
                values (${leadId}, ${att.label}, 'code:attachment', ${msg}, ${Date.now() - t0})`;
    } catch (_) { /* ignore */ }
    await notifyTeam(`\u26a0\ufe0f Error handling an attachment, please answer them manually.\n${msg.slice(0, 200)}`, "error");
    return { reply: "" };
  }
}

// Core: take one inbound message, run the agent, persist, return the reply.
async function processInbound(input: {
  messengerId: string;
  text: string;
  firstName: string | null;
  lastName: string | null;
  metaMid?: string | null;
  attachNote?: string | null;
}): Promise<ProcessResult> {
  const { messengerId, text, firstName, lastName, metaMid, attachNote } = input;
  const t0 = Date.now();
  let leadId: string | null = null;
  let inboundId: string | null = null;
  try {
    // 1. Resolve PSID — check if this messenger ID already maps to a lead via alt_messenger_ids
    // or profile name (prevents duplicate-PSID bug found 14 oct with James Froio).
    // resolveLeadByPsid checks: (a) messenger_id direct, (b) alt_messenger_ids, (c) profile name.
    // For (a) it returns immediately — the upsert's ON CONFLICT handles it with no extra query.
    // For (b)/(c) we need the lead's primary messenger_id so the upsert hits the same row.
    const resolved = await resolveLeadByPsid(
      messengerId,
      firstName && lastName ? { first_name: firstName, last_name: lastName } : null,
    );
    let effectiveMessengerId = messengerId;
    if (resolved) {
      // Check if the match was via alt or name (not direct messenger_id match)
      const [check] = await sql`select messenger_id from fadc.leads where id = ${resolved.id} and messenger_id != ${messengerId}`;
      if (check) effectiveMessengerId = check.messenger_id;
    }

    // 2. Upsert lead (using resolved messenger_id to avoid creating duplicate)
    const [lead] = await sql`
      insert into fadc.leads (messenger_id, manychat_id, first_name, last_name, last_inbound_at)
      values (${effectiveMessengerId}, ${effectiveMessengerId}, ${firstName}, ${lastName}, now())
      on conflict (messenger_id) do update
        set last_inbound_at = now(),
            first_name = coalesce(fadc.leads.first_name, excluded.first_name),
            last_name  = coalesce(fadc.leads.last_name,  excluded.last_name)
      returning id, stage, agent_paused, needs_human, first_name, soft_close_used, name_asked,
                service, zip, town, phone, preferred_day, preferred_window, address,
                offered_day, offered_window, declined_slots, coverage_alerted,
                review_followup_at, review_requested`;
    leadId = lead.id;

    // If this PSID is different from the lead's primary, register it as an alt
    if (effectiveMessengerId !== messengerId) {
      await sql`update fadc.leads
        set alt_messenger_ids = array_append(alt_messenger_ids, ${messengerId})
        where id = ${lead.id} and not (${messengerId} = ANY(alt_messenger_ids))`;
    }
    setCtxLead(leadId);

    // 2. Save inbound (dedup on Meta message id so retries don't double-reply)
    if (text) {
      if (metaMid) {
        const rows = await sql`
          insert into fadc.messages (lead_id, role, text, sent_by, meta_mid)
          values (${leadId}, 'customer', ${text}, 'customer', ${metaMid})
          on conflict (meta_mid) where meta_mid is not null do nothing returning id, created_at`;
        if (rows.length === 0) {
          return { reply: "", stage: lead.stage, needs_human: false, skipped: true };
        }
        inboundId = String(rows[0].id);
      } else {
        const [row] = await sql`insert into fadc.messages (lead_id, role, text, sent_by)
                                values (${leadId}, 'customer', ${text}, 'customer') returning id`;
        inboundId = row ? String(row.id) : null;
      }
    }

    // 3. If a human has taken over, stay silent — but a reschedule on an already-booked
    // job is time-critical, so it gets its own loud alert with everything a person needs.
    // (Found in QA: a lead hits ready_to_confirm, gets paused, and their "can we move it"
    // then landed as a generic "replied" line nobody would act on.)
    if (lead.agent_paused) {
      // A STOP FROM A PAUSED LEAD (found 20 oct). The opt-out test lives in 3c, below this
      // early return, so a lead a human had taken over could say STOP and leave no trace:
      // no closed_lost, no no_followup_until, just a generic "lead replied" line. All ten
      // people who had asked us to stop were sitting with no_followup_until null, and two of
      // them were still in a stage the followup cron reads — one un-pause away from being
      // nudged again after saying Stop twice. The lock is recorded here regardless of pause.
      // No reply goes out: a person is already in this thread and must not be talked over.
      if (STOP_RE.test(text)) {
        await sql`update fadc.leads
                     set stage = 'closed_lost',
                         no_followup_until = now() + interval '100 years',
                         updated_at = now()
                   where id = ${leadId}`;
        await notifyTeam(
          `\u{1F6D1} ${lead.first_name ?? "A lead"} asked us to stop:\n"${text.slice(0, 160)}"\n` +
          `\u2192 Logged as a permanent opt-out. The bot was already paused on them, so nobody replied. ` +
          `If you answer in the inbox, do not offer them anything.`,
          "opt_out",
        );
        return { reply: "", stage: "closed_lost", needs_human: false, paused: true };
      }
      if (hitReschedule(text)) {
        await notifyTeam(
          `🔄 RESCHEDULE REQUEST, act now\n${lead.first_name ?? "Lead"}\n📍 ${lead.address ?? lead.zip ?? "address unknown"}\n📞 ${lead.phone ?? "no phone"}\nCurrently booked: ${lead.preferred_day ?? "?"} ${lead.preferred_window ?? ""}\nThey wrote: "${text}"`,
          "reschedule",
        );
      } else {
        await notifyTeam(`💬 ${lead.first_name ?? "Lead"} replied (agent paused, needs a person):\n"${text}"`, "paused_reply");
      }
      return { reply: "", stage: lead.stage, needs_human: false, paused: true };
    }

    // 3b. POST-JOB REPLY — the review ask already carried the link, so there is nothing
    // left to send. We still read their reply for one reason: catching a bad job before it
    // becomes a public review. Anything else falls through to the agent, which can thank
    // them properly or take a second booking — both better than a canned line.
    if (lead.review_followup_at && BAD_EXPERIENCE_RE.test(text)) {
      await notifyTeam(
        `⚠️ ${lead.first_name ?? "A customer"} had a BAD experience after their job.\nThey wrote: "${text.slice(0, 200)}"\n→ Call them today. We already sent them the review link.`,
        "review_negative",
      );
      await sql`update fadc.leads set agent_paused = true where id = ${leadId}`;
      return { reply: "", stage: lead.stage, needs_human: true, paused: true };
    }

    // 3c. THEY ASKED US TO STOP. Acknowledge once, then never again. The pause and the
    // far-future no_followup_until are two separate locks on the same door, because the
    // followup cron reads the second one and not the first. needs_human stays false: this
    // is a clean exit, not a problem for someone to solve.
    if (STOP_RE.test(text)) {
      await sql`update fadc.leads
                   set agent_paused = true,
                       stage = 'closed_lost',
                       no_followup_until = now() + interval '100 years',
                       updated_at = now()
                 where id = ${leadId}`;
      await notifyTeam(
        `\u{1F6D1} ${lead.first_name ?? "A lead"} asked us to stop:\n"${text.slice(0, 160)}"\n\u2192 Agent is off for them permanently. Do not message them again.`,
        "opt_out",
      );
      const es = isSpanish(text);
      return {
        reply: es
          ? "Entendido, no te vuelvo a escribir. Si algún día nos necesitas, aquí estamos."
          : "Got it — I won't message you again. If you ever need us, we're here.",
        stage: "closed_lost",
        needs_human: false,
        paused: true,
      };
    }

    // 4. History + typing indicator in parallel (speed: customer sees "typing..." immediately)
    const [history] = await Promise.all([
      sql`select role, text from fadc.messages where lead_id = ${leadId}
          order by created_at desc limit 30`,
      sendTypingOn(messengerId),
    ]);
    history.reverse();

    // 5. Claude — with an explicit note of what we already know, so it can't re-ask
    const justAsked = recentlyAskedFields(history as { role: string; text: string }[]);
    const out = await callClaude(
      history as { role: string; text: string }[],
      buildStateNote(lead as unknown as Record<string, unknown>, justAsked) + (attachNote ? "\n\n" + attachNote : ""),
    );
    // Only code decides when the agent stops talking. The model asking for a person is a
    // request to TELL someone, never a licence to ignore the customer. Michel's rule: the
    // chat exists to close the ticket, the call is auxiliary for when something blocks it.
    let hardHandoff = false;
    let specificAlert = false;

    // 5a. SAFETY FIRST — overrides everything below. A smoking dryer, a gas smell or a
    // CO alarm is not a lead, it is a hazard in progress. Never let the agent answer one
    // of these with a scheduling question.
    // Which language the customer is writing in, across their whole side of the thread.
    const es = isSpanish([
      ...(history as { role: string; text: string }[]).filter((m) => m.role === "customer").map((m) => m.text),
      text,
    ].join("\n"));
    const say = (p: Pair) => (es ? p.es : p.en);
    // Did the customer just turn down the slot we offered? Decided in code, from our last message.
    const prevAgentText = [...(history as { role: string; text: string }[])].reverse().find((m) => m.role === "agent")?.text ?? "";
    const agentSent = (history as { role: string; text: string }[]).filter((m) => m.role === "agent").map((m) => m.text);
    const prevPrevAgentText = agentSent.length >= 2 ? agentSent[agentSent.length - 2] : "";
    const frustrated = hitFrustrated(text, prevAgentText, prevPrevAgentText);
    const customerAsked = isQuestion(text);
    const offerPending = !!(lead.offered_day && lead.offered_window) && OFFER_Q_RE.test(prevAgentText);
    const declinedNow = offerPending && !hitAccept(text) && (hitDecline(text) || frustrated);
    const declinedBefore: string[] = Array.isArray(lead.declined_slots) ? (lead.declined_slots as string[]) : [];
    const declinedAll: string[] = declinedNow
      ? [...new Set([...declinedBefore, String(lead.offered_day), ...weekdaysIn(text)])]
      : declinedBefore;
    const refusedTwice = declinedNow && declinedAll.filter((x) => /\d/.test(x)).length >= 2;
    const slotObj = refusedTwice ? null : await nextSlot(es, declinedAll, (lead.preferred_window as string | null) ?? null);
    const slot = slotObj?.phrase ?? null;

    const isEmergency = hitEmergency(text);
    if (isEmergency) {
      const gas = hitGas(text);
      out.reply = say(gas ? COPY.gas : COPY.emergency);
      out.needs_human = true;
      hardHandoff = true;
      specificAlert = true;
      out.handoff_summary = `🚨 SAFETY, possible ${gas ? "gas / CO" : "fire"} hazard. They wrote: "${text.slice(0, 160)}"`;
      await notifyTeam(
        `\u{1F6A8} SAFETY ISSUE, call this person now\n"${text.slice(0, 220)}"\nAgent told them to ${gas ? "get out of the house and call 911 or the gas company" : "stop using it and call 911 if there's smoke or flame"}.`,
        "safety",
      );
    }

    // Some nets need a person involved but must NOT silence the agent. Found in production
    // on the first real outside lead: he asked "Eres un ai?", got the call offer, and his
    // next message ("Cuanto cuesta") was met with nothing, because needs_human pauses the
    // agent. A lead who asks whether this is a bot, or asks something we have not defined,
    // still needs a price and a slot. If nobody calls back that night the lead dies in
    // silence, which is the exact opposite of why the call offer exists.
    let keepTalking = false;
    let softCloseUsed = false;
    let nameAsked = false;
    let holdHours: number | null = null;
    let misclick = false;
    let callAfter: string | null = null;
    let acceptedOffer = false;

    // 5b. Safety net: force needs_human on hardship / discount / 3+ systems even if
    // the model forgot the flag. Never downgrade — this can only turn the flag ON.
    const special = detectSpecialPricing(history as { role: string; text: string }[], text);
    if (special.hit && out.needs_human !== true) {
      out.needs_human = true;
      out.handoff_summary = out.handoff_summary
        ?? `SPECIAL PRICING, do not quote $99xN. Reason: ${special.why}. Last message: "${text.slice(0, 140)}"`;
      await notifyTeam(`💰 SPECIAL PRICING caught by safety net (${special.why}), a person should offer the package.`, "special_pricing");
      specificAlert = true;
    }
    // Same reasoning as above: a hardship or 3+ system lead still needs its address, phone
    // and day collected while a person puts the package together. The money risk here is
    // the raw total, and 5b-ii already strips that, so going silent only loses the job.
    if (special.hit && !isEmergency && !hitEscalate(text)) keepTalking = true;

    // 5b-ii. Even with the flag set, the model may already have written the raw total.
    // Strip it — the number in writing is what loses the job.
    if (special.hit && !isEmergency) {
      const big = quotedBigTotal(out.reply) ?? quotedBigTotal(text);
      if (big) {
        const ask = nextAskLine(lead as unknown as Record<string, unknown>, out.lead, es, slot, justAsked);
        out.reply = say(COPY.bigTotal) + (ask ? `${es ? ". Mientras, " : ". In the meantime, "}${ask}` : ".");
        out.needs_human = true;
        await notifyTeam(`\u{1F6AB} Blocked a raw $${big} total before it reached the customer (${special.why}).`, "guard");
        specificAlert = true;
      }
    }

    // 5b-iii. Two cases, one move: a question whose answer we have not defined, and
    // someone asking whether this is a bot or asking for a person. Michel's call: both get
    // the phone. The agent never invents and never claims to be human, and a voice closes
    // faster than a chat, so "let me check and get back to you" (which just stalls the
    // conversation) became "someone will call you in a few minutes".
    const escalateHit = hitEscalate(text) && !isEmergency;
    const unknownHit = hitUnknown(text) && !isEmergency;
    const wantsHuman = !isEmergency && (BOT_QUESTION_RE.test(text) || CALL_REQUEST_RE.test(text));
    if ((unknownHit || wantsHuman) && !escalateHit) {
      keepTalking = true;
      const phone = (out.lead?.phone ?? (lead.phone as string | null)) || null;
      const topic = unknownHit
        ? (firstMatch(text, UNKNOWN_FACT_RE, UNKNOWN_FACT_ES_RE) ?? "something not in our facts")
        : null;
      out.needs_human = true;
      out.reply = callLine(es, phone, unknownHit ? "unknown" : "human");
      out.handoff_summary = out.handoff_summary ?? (topic
        ? `CALL THEM, asked about "${topic}" which we have not defined. ${phone ?? "no number yet"}. Message: "${text.slice(0, 120)}"`
        : `CALL REQUEST${phone ? ", " + phone : ", no number yet"}. They wrote: "${text.slice(0, 120)}"`);
      await notifyTeam(
        `\u{1F4DE} CALL NOW${phone ? ", " + phone : " (no number yet, agent asked for it)"}\n"${text.slice(0, 200)}"\n` +
        (topic
          ? `They asked about "${topic}", which we have not defined. Call them, answer it, and close on the phone.`
          : "They asked to talk to a person / asked if this is a bot. The agent offered a call, close it on the phone."),
        topic ? "unknown_fact" : "call_request",
      );
      specificAlert = true;
    }

    // 5c. Escalation net: the agent must never turn a job away on its own. Commercial,
    // mold, insurance and complaints are Afek's call. QA caught it refusing a 40,000 sq ft
    // commercial job outright and sending them to a competitor.
    if (escalateHit && out.needs_human !== true) {
      const term = firstMatch(text, ESCALATE_RE, ESCALATE_ES_RE) ?? "out-of-scope request";
      hardHandoff = true;
      specificAlert = true;
      out.needs_human = true;
      out.lead = out.lead ?? {};
      if (out.lead.stage === "closed_lost") out.lead.stage = "needs_human";
      out.reply = say(COPY.escalate);
      out.handoff_summary = out.handoff_summary
        ?? `ESCALATED, agent must not decide this. Trigger: "${term}". Message: "${text.slice(0, 140)}"`;
      await notifyTeam(`📋 ESCALATED (${term}), agent was about to answer or decline this alone.\n"${text.slice(0, 200)}"`, "escalated");
    }

    // 5c-i. THE MISCLICK. A person who tapped the ad by accident is not a lead, and selling to
    // them is the fastest way to earn a "hide this ad". On 4 oct someone wrote "Sorry mistake."
    // and the agent answered "No problem at all! What service are you looking to get done, air
    // ducts, dryer vent, or chimney?" They had to repeat themselves: "I said it was a mistake."
    // One polite line, then out. Closed and paused in code, so no follow-up can ever find them.
    const misclickHit = hitMisclick(text) && !isEmergency && !escalateHit;
    if (misclickHit) {
      out.reply = say(COPY.misclick);
      out.lead = out.lead ?? {};
      out.lead.stage = "closed_lost";
      out.needs_human = false;
      misclick = true;
      // No Telegram for these. They are noise by definition, and an alert nobody needs is how
      // the one that matters gets buried. The closed_lost row is the record.
    }

    // 5c-ii. A lead who says "let me think about it" gets ONE soft hold, never a goodbye and
    // never two pushes. The goodbye line belongs to a lead who actually declined, not to a
    // lead who is weighing the price, and that is exactly when we still have no ZIP and no
    // phone. Skipped when a higher net already wrote the reply.
    const thinkingHit = hitThinking(text) && !isEmergency && !escalateHit && !unknownHit && !wantsHuman && !misclickHit;

    // 5c-ii-b. THE QUIET PERIOD. A lead who says "I'll get back to you" has just told us when
    // to come back, and the follow-up cron has to honour it. Mansfield 02048 wrote "OK, I will
    // get back to you tomorrow" on Saturday 5:33pm; the cron nudged her at 9:00am Sunday and
    // she answered "I told you I would get back to you". A prompt rule cannot stop a cron job,
    // so the quiet period is a column the cron reads. 44 hours gives "tomorrow" a whole day
    // plus a morning; an explicit week pushes it out six days. The window can only grow.
    if (thinkingHit) holdHours = (hitLongWait(text) ? 24 * 6 : 44);

    if (thinkingHit && lead.soft_close_used !== true) {
      const ask = nextAskLine(lead as unknown as Record<string, unknown>, out.lead, es, slot, justAsked);
      // Keep whatever actually answered their question, cut the sign-off AND any slot offer
      // the model already wrote (v79: the model doesn't know the customer just said "not yet",
      // so it often tacks on "I have Tuesday open, want me to hold it?" — which is exactly
      // what the soft close replaces with a gentler version). Overwriting the whole reply
      // would swallow the answer they asked for.
      let answered = dropGoodbye(out.reply);
      // v79: strip slot-offer sentences so the soft close copy is the only offer
      answered = answered.split(/(?<=[.!?])\s+/)
        .filter((s: string) => !OFFER_Q_RE.test(s))
        .join(" ").trim();
      // v86: when `ask` comes back null the field was already asked in the last two messages.
      // "I'll get back to you" plus a question we just asked is the nag that lost Jeanmarie.
      // Let the soft close stand on its own \u2014 no question at all. They know how to reach us.
      out.reply = [
        answered,
        say(COPY.softClose),
        ask ? cap(ask) : "",
      ].filter(Boolean).join(" ");
      out.lead = out.lead ?? {};
      out.lead.stage = "qualifying"; // never close a lead who only said "let me think"
      keepTalking = true;
      softCloseUsed = true;
      await notifyTeam(
        `\u{1F4A4} A lead is hesitating on price. The agent gave ONE soft hold instead of a goodbye.\n` +
        `They wrote: "${text.slice(0, 160)}"\n` +
        (lead.phone ? `Phone on file: ${lead.phone}` : "No phone on file yet, so chat is the only way back to them."),
        "hesitation",
      );
      specificAlert = true;
    }

    // People fire two or three messages in a row. Each one starts its own run, so Donna's
    // two questions one second apart got two replies one second apart, both ending in the
    // same question. If the customer wrote again while this run was thinking, that newer
    // run already has both messages in its history: drop this reply and let it answer once.
    // The comparison is done entirely in SQL against this row's own id. The first version
    // stringified the timestamp in JS, which dropped the milliseconds, so every message
    // compared newer than itself and EVERY reply was dropped. The bot went silent for
    // fifteen hours and only one inbound message hit it in that window, which is the only
    // reason it was not worse. Never hand a converted timestamp back to the database.
    if (inboundId) {
      const newer = await sql`
        select 1 from fadc.messages
        where lead_id = ${leadId} and role = 'customer'
          and created_at > (select created_at from fadc.messages where id = ${inboundId})
        limit 1`;
      if (newer.length > 0) {
        return { reply: "", stage: lead.stage, needs_human: false, skipped: true };
      }
    }

    // 5c-iii. THE AREA NET. See AREA_Q_RE. Fires when the customer asks where we are or says
    // "too far", or when the model's own reply states or implies they are outside the area.
    // Never fires over an emergency, an escalation or a misclick.
    out.lead = out.lead ?? {};
    const zipInText = text.match(/\b(\d{5})\b/)?.[1] ?? null;
    if (zipInText && !out.lead.zip && !lead.zip) out.lead.zip = zipInText;
    const areaQ = hitAreaQ(text);
    const areaLeak = hitAreaLeak(out.reply ?? "");
    if ((areaQ || areaLeak) && !isEmergency && !escalateHit && !misclickHit) {
      const zipKnown = !!(out.lead.zip || lead.zip || out.lead.town || lead.town);
      const next = zipKnown
        ? nextAskLine(lead as unknown as Record<string, unknown>, out.lead, es, slot, justAsked)
        : null;
      const place = (out.lead.town || (lead.town as string | null) || out.lead.zip || (lead.zip as string | null) || "").toString().trim();
      const zipNow = (out.lead.zip || (lead.zip as string | null) || "").toString();
      const isCT = /^06/.test(zipNow) || /\b(CT|Connecticut)\b/i.test(`${place} ${text}`);
      const hasPhone = !!(out.lead.phone || (lead.phone as string | null));
      // v74: "where is your company located?" gets the honest answer. Hiding it read as shady
      // (Prince, 9 oct: "you didn't answer my question"). Connecticut has a local manager, Ben.
      const base = es
        ? "Somos una empresa familiar del área de Boston, con equipos locales en todo MA, CT, RI y NJ."
        : "We're a family company out of the Boston area, with local crews all over MA, CT, RI and NJ.";
      const ben = isCT ? (es ? " En Connecticut tu manager es Ben." : " In Connecticut your manager is Ben.") : "";
      const areaLine = areaQ
        ? `${base}${place ? (es ? ` ¡Sí cubrimos ${place}!` : ` Yes, we cover ${place}!`) : ""}${ben}`
        : place
          ? (es ? `¡Sí, cubrimos ${place}! Tenemos equipos en todo MA, CT, RI y NJ.` : `Yes, we cover ${place}! We have crews all over MA, CT, RI and NJ.`)
          : say(COPY.area);
      const benCall = isCT && hasPhone ? (es ? " ¿Quieres que Ben te llame rápido?" : " Want Ben to give you a quick call?") : "";
      // A customer who asked where we are gets the answer, not a slot in the same breath.
      const nextAfterArea = areaQ ? null : next;
      out.reply = zipKnown
        ? (nextAfterArea ? `${areaLine} ${cap(nextAfterArea)}` : areaLine + (benCall || (areaQ ? (es ? " ¿Te queda mejor en la mañana o en la tarde?" : " Do mornings or afternoons work better for you?") : "")))
        : (areaQ ? `${base} ${es ? "¿Cuál es tu código postal? Reviso el equipo más cercano." : "What's your ZIP? I'll check the closest crew to you."}` : say(COPY.areaZip));
      if (benCall) {
        await notifyTeam(`\u{1F4CD} CT lead asked where we are; bot offered a call from Ben. ${(out.lead.phone || lead.phone) ?? ""} ZIP ${zipNow}`, "ct_call");
        specificAlert = true;
      }
      if (out.lead.stage === "closed_lost") out.lead.stage = "qualifying";
      keepTalking = true;
      if (areaLeak && !areaQ) {
        await notifyTeam(`↩️ Rewrote a reply that said or implied the customer is outside our area (${lead.first_name ?? messengerId}).`, "guard");
      }
    }
    // The owner finds a crew for any ZIP outside MA/CT/RI. Tell him once, when the ZIP first lands.
    const newZip = out.lead.zip && !lead.zip ? String(out.lead.zip) : null;
    if (newZip && /^\d{5}$/.test(newZip) && !isCoreZip(newZip)) {
      await notifyTeam(`\u{1F4CD} ZIP ${newZip} is outside MA/CT/RI (${lead.first_name ?? messengerId}). The agent is still collecting, line up a crew for this area.`, "area");
    }

    out.reply = scrubFillers(stripDashes(out.reply));

    const L = out.lead;
    let stage = L.stage ?? "qualifying";

    // 5d. Never let a ticket reach the tech without a street address — a ZIP is not an
    // address and the job cannot be done from it. QA caught a ready_to_confirm ticket
    // written with address = null. Hold the close and ask for it instead.
    const knownAddress = (L.address ?? (lead.address as string | null)) || null;
    if (stage === "ready_to_confirm" && !knownAddress) {
      stage = "qualifying";
      out.needs_human = false;
      out.reply = say(COPY.address);
      await notifyTeam(
        `↩️ Held back a booking with no street address (${lead.first_name ?? messengerId}) and asked for it instead.`,
        "guard",
      );
    }

    // 5d-ii. A ticket with an address and no name sends the tech to a door without knowing
    // who to ask for. Meta will not give us the name for customers outside the app, so the
    // agent asks. Held ONCE: if they still do not give it, the booking goes through with
    // the name pending rather than looping on a question nobody has to answer.
    // Winnie answered "Winnie Nerette" and the ticket still read "Full Name: Winnie", because
    // only first_name was ever read. A tech knocking on a door wants the name on the door.
    const fName = (out.lead?.first_name ?? null) || (lead.first_name as string | null);
    const lName = (out.lead?.last_name ?? null) || (lead.last_name as string | null);
    const knownName = [fName, lName].filter(Boolean).join(" ").trim() || null;
    if (stage === "ready_to_confirm" && !knownName && lead.name_asked !== true) {
      stage = "qualifying";
      out.needs_human = false;
      out.reply = say(COPY.nameNeeded);
      nameAsked = true;
    }

    // THE RULE: only three things may silence the agent, and all three are decided in code.
    //   1. the booking is armed (ready_to_confirm), a person confirms the slot
    //   2. a safety hazard
    //   3. a job only the owner can price (commercial, mold, insurance)
    // Everything else alerts the team and KEEPS TALKING. The model returning needs_human,
    // or setting stage to "needs_human" itself, is a request to tell someone, not a licence
    // to ignore the customer. It cost us twice in one day: a booking-ready lead sat unanswered
    // for ten hours, and a plain "quiero limpiar los ductos, tengo 2 sistemas" got handed off
    // and silenced. keepTalking is kept for readability but the rule no longer depends on it.
    if (!hardHandoff && stage === "needs_human") {
      stage = "qualifying";
      // A handoff reply usually ends with no question ("someone will call you"), which leaves
      // the customer nothing to answer. Never dead-end a lead that is still qualifying.
      if (!/[?\u00bf]/.test(out.reply)) {
        const nextQ = nextAskLine(lead as unknown as Record<string, unknown>, out.lead, es, slot, justAsked);
        if (nextQ) out.reply = `${out.reply} ${cap(nextQ)}`.trim();
      }
    }
    // 5f. THE SLOT NET. Last thing before the reply leaves: if we still do not have a day and
    // the model reached for the open question anyway, swap it for a concrete offer. If the
    // reply asks nothing at all, add the offer. Never add a second question to a reply that
    // already has one, because one question per message is the rule that keeps replies short.
    // 5e-i-a. THE CALL TIME IS NOT THE JOB TIME. When the customer is telling us when they can
    // pick up the phone, any window the model pulled out of that sentence is thrown away and
    // kept separately. Duan Le's job was booked for "5pm+" off the sentence "I am in a meeting
    // but will be done after 5pm".
    if (hitCallContext(text)) {
      callAfter = extractCallTime(text);
      if (out.lead?.preferred_window) out.lead.preferred_window = null;
    }

    // 5e-i-b. THE ACCEPTANCE. We generated the offer, so we recognise the yes ourselves. The
    // day and window come straight off what we offered, not from re-reading the conversation.
    const offeredDay = lead.offered_day as string | null;
    const offeredWindow = lead.offered_window as string | null;
    // An offer whose date is already behind us must not be accepted: "10/7" read on 10/9 rolls
    // to next year. tooSoon below catches today and yesterday; this catches the year roll.
    const offDate = resolveDateObj(offeredDay);
    const offerStale = !!(offDate && offDate.getTime() - bostonToday().getTime() > 60 * 86_400_000);
    if (offeredDay && offeredWindow && hitAccept(text) && !misclickHit && !offerStale) {
      out.lead = out.lead ?? {};
      out.lead.preferred_day = offeredDay;
      out.lead.preferred_window = offeredWindow;
      acceptedOffer = true;
    }

    // 5e-ii. THE SAME-DAY FLOOR. A booking the techs cannot staff is worse than no booking:
    // the customer gets a confirmation and then a phone call taking it away. Winnie got both
    // inside 34 minutes on 5 oct. Whatever day the agent and the customer landed on, if it
    // falls before the first date the availability rule allows, the close is refused here and
    // the real slot is offered instead. Code decides this, never the prompt.
    const wantedDay = (out.lead?.preferred_day ?? null) || (lead.preferred_day as string | null);
    const wantedDate = resolveDateObj(wantedDay);
    const floor = await earliestAllowedDate();
    // Two ways a date can be unworkable: it is sooner than our lead time, or it falls on a day
    // nobody works. FADC is closed Saturdays, so "Saturday" has to be refused the same way
    // "today" is, and for the same reason: the customer would be promised an empty truck.
    const availNow = await loadAvailability();
    const badWeekday = !!(wantedDate && !availNow.days.includes(DAY_ABBR[wantedDate.getUTCDay()]));
    const tooSoon = !!(wantedDate && floor && (wantedDate.getTime() < floor.getTime() || badWeekday));
    if (tooSoon && !hardHandoff) {
      stage = "qualifying";
      out.needs_human = false;
      out.lead = out.lead ?? {};
      out.lead.preferred_day = null;   // never store a day we cannot work
      out.lead.stage = "qualifying";
      const offer = slot ?? fmtDate(floor!);
      // v79: dynamic tooSoon — say the day they actually asked for, not always "today".
      // "I can't promise today" when they asked for Wednesday reads as if we aren't listening.
      const tooSoonMsg = badWeekday
        ? (es ? "Los fines de semana no tenemos técnicos en ruta. Lo más pronto es" : "We don't have techs out on weekends. The soonest I can get someone to you is")
        : (() => {
            const isToday = wantedDate && floor && (wantedDate.toDateString() === new Date().toDateString());
            if (isToday || !wantedDay) return say(COPY.tooSoon);
            return es
              ? `No tengo disponibilidad para ${wantedDay}, los técnicos ya están ocupados. Lo más pronto que te puedo mandar a alguien es`
              : `I can't do ${wantedDay}, the techs are already booked. The soonest I can get someone to you is`;
          })();
      out.reply = `${tooSoonMsg} ${offer}. ${es ? "¿Te lo aparto?" : "Want me to hold it for you?"}`;
      await notifyTeam(
        `\u{23F0} A lead asked for a slot BEFORE our lead time and the agent was about to take it.\n` +
        `They wanted: "${wantedDay}". Offered instead: ${offer}.\n` +
        `If a tech really is free sooner, answer them in the inbox.`,
        "too_soon",
      );
      specificAlert = true;
    }

    if (refusedTwice && !hardHandoff) {
      await notifyTeam(
        `\u{1F5D3}\uFE0F ${lead.first_name ?? "A lead"} turned down 2 offered slots (${declinedAll.join(", ")}). ` +
        `The agent stopped offering and asked what day works. Write or call them in the inbox.\nThey wrote: "${text.slice(0, 160)}"`,
        "declined_slots",
      );
      specificAlert = true;
    }
    const haveDay = !!((out.lead?.preferred_day ?? null) || (lead.preferred_day as string | null));
    const checkingWithTeam = out.needs_human === true && /let me check|check with (the|our) team/i.test(out.reply ?? "");
    // v79: suppress slot net after thinkingHit — the soft close already offered gently,
    // appending another "I have Tuesday open" undoes the gentle tone.
    if (slot && !haveDay && out.reply && !hardHandoff && !checkingWithTeam && !customerAsked && !frustrated && !thinkingHit) {
      if (OPEN_DAY_RE.test(out.reply)) {
        out.reply = out.reply.replace(OPEN_DAY_RE, cap(holdLine(slot, es)));
      } else if (!/[?\u00bf]/.test(out.reply) && !DAY_WORD_RE.test(out.reply) && stage === "qualifying") {
        out.reply = `${out.reply} ${cap(holdLine(slot, es))}`.trim();
      }
    }

    // 5f. v72 HUMAN GUARDS. Frustration gets a second pass that apologises and answers; any
    // reply that repeats something we already sent is rewritten, and if the model still repeats
    // itself a person takes over. Skipped for safety and escalation lines, which are fixed copy.
    if (!isEmergency && !escalateHit && !misclickHit && out.reply) {
      const recentSent = agentSent.slice(-3);
      const needRetry = frustrated || repeatsSent(out.reply, recentSent);
      if (needRetry) {
        const why = frustrated
          ? "The customer is frustrated because the last replies did not answer them. Start with \"Sorry about that!\" and answer their last question directly in plain words. Do NOT offer a day or a time slot in this message."
          : `Your draft repeated a message already sent in this chat: "${out.reply.slice(0, 160)}". Write a DIFFERENT reply that responds to exactly what the customer just said. If they asked something, answer it.`;
        try {
          const retry = await callClaude(
            history as { role: string; text: string }[],
            buildStateNote(lead as unknown as Record<string, unknown>) + "\n\nIMPORTANT: " + why,
          );
          if (retry.reply && !repeatsSent(retry.reply, recentSent)) {
            out.reply = retry.reply;
            if (retry.lead) out.lead = { ...(out.lead ?? {}), ...retry.lead };
          } else {
            out.reply = es
              ? "Perdón por eso. Te escribe alguien del equipo en un momento."
              : "Sorry about that! Someone from the team will message you in just a minute.";
            out.needs_human = true;
            hardHandoff = true;
          }
        } catch (_e) {
          out.reply = es
            ? "Perdón por eso. Te escribe alguien del equipo en un momento."
            : "Sorry about that! Someone from the team will message you in just a minute.";
          out.needs_human = true;
          hardHandoff = true;
        }
        if (frustrated || hardHandoff) {
          await notifyTeam(
            `\u{26A0}\uFE0F ${lead.first_name ?? "A lead"} is frustrated with the bot. They wrote: "${text.slice(0, 160)}"\n` +
            `Bot answered: "${out.reply.slice(0, 160)}". Check the chat and take over if needed.`,
            "frustrated",
          );
          specificAlert = true;
        }
      }
    }

    // A misclick is closed and paused here, in code. The follow-up query already skips
    // closed_lost, and the pause is the second lock on the same door.
    const pause = stage === "ready_to_confirm" || hardHandoff || misclick;
    void keepTalking;

    // 6. Persist lead fields (never overwrite a known value with null)
    await sql`
      update fadc.leads set
        first_name       = coalesce(${L.first_name ?? null}, first_name),
        last_name        = coalesce(${L.last_name ?? null}, last_name),
        service          = coalesce(${L.service ?? null}, service),
        zip              = coalesce(${L.zip ?? null}, zip),
        town             = coalesce(${L.town ?? null}, town),
        phone            = coalesce(${L.phone ?? null}, phone),
        preferred_day    = coalesce(${L.preferred_day ?? null}, preferred_day),
        preferred_window = coalesce(${L.preferred_window ?? null}, preferred_window),
        address          = coalesce(${L.address ?? null}, address),
        stage            = ${stage},
        needs_human      = ${out.needs_human === true},
        agent_paused     = ${pause},
        soft_close_used  = ${softCloseUsed || lead.soft_close_used === true},
        offered_day      = case when ${declinedNow && !(slot && out.reply && out.reply.includes(slot))}::boolean then null
                                else coalesce(${(slot && out.reply && out.reply.includes(slot)) ? (slotObj?.day ?? null) : null}, offered_day) end,
        offered_window   = case when ${declinedNow && !(slot && out.reply && out.reply.includes(slot))}::boolean then null
                                else coalesce(${(slot && out.reply && out.reply.includes(slot)) ? (slotObj?.window ?? null) : null}, offered_window) end,
        declined_slots   = ${sql.array(declinedAll)}::text[],
        call_after       = coalesce(${callAfter}, call_after),
        no_followup_until = case when ${holdHours}::int is null then no_followup_until
                                 else greatest(coalesce(no_followup_until, now()),
                                               now() + (${holdHours ?? 0}::int * interval '1 hour')) end,
        name_asked       = ${nameAsked || lead.name_asked === true},
        handoff_summary  = coalesce(${out.handoff_summary ?? null}, handoff_summary),
        job_scale        = coalesce(${out.job_scale ?? null}, job_scale),
        last_outbound_at = case when ${out.reply !== ""} then now() else last_outbound_at end
      where id = ${leadId}`;

    // ── Big job on the line ──────────────────────────────────────────────────────────────
    // Fires the moment the size is known, at whatever stage, because the point is to get a
    // person on the phone while the customer is still in the chat. Michel is the one who
    // closes, so this is addressed to him and goes out once per lead.
    const scaleNow = String(out.job_scale ?? (lead.job_scale as string | null) ?? "").trim();
    if (leadId && lead.value_alerted !== true && bigJob(scaleNow)) {
      try {
        await sql`update fadc.leads set value_alerted = true where id = ${leadId}`;
        const who = (L.first_name ?? lead.first_name ?? "") as string;
        const tel = (L.phone ?? lead.phone ?? "") as string;
        await notifyTeamFor(leadId,
          `\u{1F4B0} BIG JOB on the line${who ? ` — ${who}` : ""}\n` +
          `Size: ${scaleNow}\n` +
          (tel ? `\u{1F4DE} Call now: ${tel}\n` : "No phone yet — get it before they go quiet.\n") +
          `${[L.service ?? lead.service, L.town ?? lead.town, L.zip ?? lead.zip].filter(Boolean).join(", ")}\n` +
          `Close this one yourself. The multi-system jobs are the ones that pay for the week.`,
          "big_job");
      } catch (_) { /* an alert must never block the reply */ }
    }

    // Save reply in parallel with lead update (both are independent writes)
    if (out.reply) {
      await sql`insert into fadc.messages (lead_id, role, text, sent_by) values (${leadId}, 'agent', ${out.reply}, 'agent')`;
    }

    // 7. Team alert + auto-ticket
    if (stage === "ready_to_confirm") {
      // ONE open ticket per lead. A customer who adds a service or moves the time used to
      // create a SECOND ticket, so the techs saw two jobs at the same address and could be
      // dispatched twice. Update the open one instead, and only insert when there is none.
      let ticketAction = "created";
      const ticketDay = (L.preferred_day ?? null) || (lead.preferred_day as string | null);
      const ticketWin = (L.preferred_window ?? null) || (lead.preferred_window as string | null);
      let slotDate: string | null = null, slotWin: string | null = null, overbooked = 0;
      const ticket = buildTicket({
        name: knownName ?? null,
        jobType: out.job_type ?? null,
        service: (L.service ?? null) || (lead.service as string | null),
        day: (L.preferred_day ?? null) || (lead.preferred_day as string | null),
        window: (L.preferred_window ?? null) || (lead.preferred_window as string | null),
        address: (L.address ?? null) || (lead.address as string | null),
        zip: (L.zip ?? null) || (lead.zip as string | null),
        phone: (L.phone ?? null) || (lead.phone as string | null),
        context: out.job_context ?? null,
        callAfter: callAfter ?? (lead.call_after as string | null),
        scale: (out.job_scale ?? null) || (lead.job_scale as string | null),
      });
      try {
        const notes = ticket;
        const av = await loadAvailability();
        const dObj = resolveDateObj(ticketDay);
        slotDate = dObj ? isoDate(dObj) : null;
        slotWin = windowBucket(ticketWin, av.windows);
        if (slotDate && slotWin) {
          const used = await slotLoad(slotDate, leadId);
          if ((used[slotWin] ?? 0) >= av.max_per_window) overbooked = used[slotWin];
        }
        const updated = await sql`
          update fadc.appointments
          set service = coalesce(${L.service ?? null}, service),
              address = coalesce(${L.address ?? null}, address),
              notes = ${notes},
              slot_date = coalesce(${slotDate}::date, slot_date),
              slot_window = coalesce(${slotWin}, slot_window),
              exact_window = case when ${slotWin}::text is not null and ${ticketWin}::text is distinct from ${slotWin}::text then ${ticketWin} else exact_window end,
              updated_at = now()
          where id = (
            select id from fadc.appointments
            where lead_id = ${leadId}
              and status in ('pending', 'confirmed', 'rescheduled')
            order by created_at desc
            limit 1
          )
          returning id`;
        if (updated.length > 0) ticketAction = "updated";
        else {
          await sql`
            insert into fadc.appointments (lead_id, service, address, notes, slot_date, slot_window, exact_window)
            values (${leadId}, ${L.service ?? null}, ${L.address ?? null}, ${notes}, ${slotDate}::date, ${slotWin},
                    ${slotWin && ticketWin && ticketWin !== slotWin ? ticketWin : null})`;
        }
      } catch (_) { /* never block the reply on ticket creation */ }
      await notifyTeam(
        ticket + "\n\n" +
        (overbooked > 0
          ? `\u{1F6A8} DOUBLE BOOKING: ${overbooked} other job(s) already hold ${slotDate} ${slotWin}. Send a second crew or reschedule ONE of them today.\n`
          : "") +
        (ticketAction === "updated"
          ? "\u26A0\uFE0F This is the SAME job as before, not a new one. The ticket was updated in place.\nThe customer has been CONFIRMED for this slot. Assign a tech, or reschedule them if nobody is free."
          : "The customer has been CONFIRMED for this slot. Assign a tech, or reschedule them if nobody is free."),
        "appointment",
      );

      // \u2500\u2500 Coverage check at ticket-ready \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
      // Only fires when a full ticket exists (stage just became ready_to_confirm).
      // At this point the lead is real money \u2014 check who can take the job.
      if (!lead.coverage_alerted) {
        const ticketZip = String((L.zip ?? null) || (lead.zip as string | null) || "").trim();
        if (ticketZip && /^\d{5}$/.test(ticketZip)) {
          type TechRow = { name: string; type: string; phone: string | null; split_pct: number | null; flat_fee: number | null };
          const techs = await sql<TechRow[]>`
            SELECT name, type, phone, split_pct, flat_fee
            FROM fadc.technicians
            WHERE active = true
              AND ${ticketZip} = ANY(coverage_zips)
            ORDER BY type
          `;
          const ownTech = techs.find((t) => t.type === "own");
          const subTech = techs.find((t) => t.type === "subcontractor");

          if (!ownTech && subTech) {
            const fee = subTech.flat_fee != null
              ? `tarifa fija $${subTech.flat_fee}`
              : subTech.split_pct != null
                ? `${subTech.split_pct}% del cierre`
                : "sin tarifa definida";
            await notifyTeam(
              `\uD83D\uDCCB ZIP ${ticketZip} \u2014 sin t\u00E9cnico propio. Subcontratista disponible:\n` +
              `  \uD83D\uDD27 ${subTech.name}${subTech.phone ? ` (${subTech.phone})` : ""} \u2014 ${fee}\n` +
              `  \u27A1\uFE0F Pasa el ticket de arriba a ${subTech.name} para que lo trabaje.`,
              "coverage_sub",
            );
          } else if (!ownTech && !subTech) {
            await notifyTeam(
              `\uD83D\uDEA8 ZIP ${ticketZip} \u2014 sin cobertura. Ticket listo pero sin t\u00E9cnico ni subcontratista.\n` +
              `  \u27A1\uFE0F Consigue alguien para esta zona o contacta un broker.`,
              "coverage_none",
            );
          }
          // own tech \u2192 no alert needed
          await sql`UPDATE fadc.leads SET coverage_alerted = true WHERE id = ${leadId}`;
        }
      }
    } else if (out.needs_human === true && !specificAlert) {
      // Only when no net already sent its own, more specific alert. Twenty alerts in six
      // minutes buried the one that carried a real booking, so every duplicate alert costs attention.
      await notifyTeam(
        `🙋 NEEDS A PERSON (the agent is still talking to them)\n${out.handoff_summary ?? ""}\nLast message: "${text}"`,
        "needs_human",
      );
    }

    // 8. Log
    await sql`insert into fadc.agent_runs (lead_id, inbound_text, model, raw_output, reply_sent, latency_ms)
      values (${leadId}, ${text}, ${MODEL}, ${sql.json(out as unknown as Record<string, unknown>)}, ${out.reply}, ${Date.now() - t0})`;

    return { reply: out.reply, stage, needs_human: pause };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    try {
      await sql`insert into fadc.agent_runs (lead_id, inbound_text, model, error, latency_ms)
        values (${leadId}, ${text}, ${MODEL}, ${msg}, ${Date.now() - t0})`;
    } catch (_) { /* ignore */ }
    await notifyTeam(`⚠️ Agent error on a lead, please answer them manually in the inbox.\n${msg.slice(0, 300)}`, "error");
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Follow-ups: nudge leads that went quiet after we asked them something.
// Evidence (30 sep): a lead we re-contacted after silence booked an appointment,
// so silence after first touch is recoverable. Rules:
//   - only stage new/qualifying, agent not paused, and WE spoke last (they owe an answer)
//   - nudge 1 after ~3h of silence, nudge 2 after ~20h, never a third
//   - always inside Meta's 24h window (free-form replies are only allowed there)
//   - only during business hours in Boston (Sun–Thu 9am–7pm, Fri 9am–5pm, never Sat)
// Triggered by pg_cron every 30 min (POST {"action":"followups"} + x-fadc-cron header)
// or manually by Michel with the x-fadc-secret header. Add "dry_run":true to preview.
// ---------------------------------------------------------------------------

function bostonNow(): { weekday: string; hour: number } {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", weekday: "short", hour: "numeric", hour12: false,
  }).formatToParts(new Date());
  const weekday = parts.find((p) => p.type === "weekday")?.value ?? "";
  const hour = Number(parts.find((p) => p.type === "hour")?.value ?? "0") % 24;
  return { weekday, hour };
}

// build: v81
// v79 changes (12 oct 2026):
//   1. Soft close strips slot offers from model reply — customer says "let me think",
//      the model's reply no longer contains "I have Tuesday open" before the soft close copy.
//   2. Slot net suppressed after thinkingHit — no second slot push after "not yet".
//   3. tooSoon response says the day the customer asked for, not always "today".
// Follow-ups run every day, 9am to 7pm Boston. FADC not working Saturdays decides when a
// TECH can go out, not when we are allowed to talk to someone. On Saturday 3 oct three real
// leads went quiet and not one got a nudge, and two of them had their 24h Meta window close
// before Sunday 9am, so the rule did not delay those follow-ups, it deleted them.
function insideFollowupHours(): boolean {
  const { hour } = bostonNow();
  return hour >= 9 && hour < 19;
}

type FollowupRow = {
  id: string; messenger_id: string; first_name: string | null; stage: string;
  followup_count: number; silence_hours: number;
  service: string | null; zip: string | null; town: string | null; phone: string | null;
  preferred_day: string | null; preferred_window: string | null; address: string | null;
  declined_slots: string[] | null;
};

// A follow-up has to read like the same person who talked to them yesterday, not a
// broadcast. Early version produced "Still here if you want to book! We have openings
// this week" for a lead whose name, service and quote we already knew — generic enough
// that it reads as automated and gets ignored. So we hand the model everything we know
// about this specific lead and require it to use the name and one concrete detail.
const FOLLOWUP_INSTRUCTION = (r: FollowupRow, hours: number, slot: string | null) => {
  const known: string[] = [];
  if (r.service) known.push(`service they want: ${r.service}`);
  if (r.zip || r.town) known.push(`area: ${[r.town, r.zip].filter(Boolean).join(" ")}`);
  if (r.preferred_day) known.push(`day they floated: ${r.preferred_day}`);
  if (r.preferred_window) known.push(`time window: ${r.preferred_window}`);
  if (r.phone) known.push(`phone: ${r.phone}`);
  if (r.address) known.push(`address: ${r.address}`);

  const missing: string[] = [];
  if (!r.service) missing.push("which service they need");
  if (!(r.zip || r.town)) missing.push("their ZIP code");
  if (!r.preferred_day) missing.push(slot ? `a YES to ${slot}` : "the day that works for them");
  if (!r.preferred_window) missing.push("a 2-hour time window");
  if (!r.phone) missing.push("their phone number");
  if (!r.address) missing.push("their street address");

  const first = r.followup_count === 0;
  const nudgeLabel = first ? "FIRST" : r.followup_count === 1 ? "SECOND" : "THIRD and FINAL";
  return `(Internal note, NOT from the customer. Never quote or mention it.

They went quiet about ${hours} hours ago, right after your last message. Write ONE short follow-up, the ${nudgeLabel} nudge.

What you already know about this specific person:
${known.length ? known.map((k) => "- " + k).join("\n") : "- almost nothing yet, they barely wrote"}
Still missing: ${missing.length ? missing.join(", ") : "nothing, you just need them to confirm"}

Rules for this one message:
1. Open with their first name${r.first_name ? ` (${r.first_name})` : " if the conversation shows it"}. "Hi ${r.first_name ?? "[name]"}!", never "Hi there!".
2. Reference something CONCRETE from your conversation: the service, how many systems, the price you quoted them, the day they mentioned. Prove you remember THEM. A line that could be sent to any stranger is a failure, "Still here if you want to book! We have openings this week" is exactly what NOT to write.
3. Ask for exactly ONE thing: ${missing[0] ?? "confirming the slot"}.${(!r.preferred_day && slot) ? `\n   Do NOT ask "what day works for you". OFFER this exact slot and ask them to confirm it: ${slot}. A yes/no is a much smaller ask than inventing a date, and every lead we lost died on the open question.` : ""}
4. Max 30 words. One or two sentences.
5. TOWN RULE: If you know their ZIP code but NOT their town name, write "ZIP [number]" or leave the location out entirely. NEVER guess or infer a city name from a ZIP code — you will get it wrong and it will look creepy. Only use the town name if it was explicitly stated in the conversation.
6. BANNED openers and fillers, in any form: "just checking", "just checking in", "checking in", "checking back", "following up", "circling back", "touching base", "wanted to see if", "hope you're well", "hope this finds you", "good morning or afternoon", "good morning/afternoon", "morning or afternoon". Also banned: any time-of-day greeting you can't be certain of ("Good morning", "Good afternoon", "Good evening") — you don't know when they'll read it, so it will be wrong. Use a neutral opener tied to their name and the concrete detail instead. These are the tell that a message is automated. If your draft contains any of them, rewrite it. No apology, no emojis, no pressure.
${first ? "" : r.followup_count === 1 ? "7. This is the second nudge. Try a different angle from the first: offer a choice (mornings or afternoons? this week or next?), mention no deposit needed, or highlight something specific about their situation. Keep it warm.\n" : "7. This is the LAST time you write to them. It's been two days. Keep it warm, brief, and make it easy to either say yes or let it go. No urgency, no guilt. Something like 'no worries if the timing changed' gives them a graceful out while leaving the door open.\n"}
If the conversation shows they already said no, booked elsewhere, or are not a customer, return an empty reply.
Return the same JSON format as always; leave the lead fields as they are.)`;
};

// The phrases that instantly mark a message as automated. The prompt bans them, but a
// follow-up is the one message where a single stale filler costs the lead, so we also
// check the draft in code. ("Just checking — Tuesday 10am works?" slipped past the
// prompt once; that is the whole reason this exists.)
const BANNED_FOLLOWUP_RE =
  // "circle back" slipped through on 5 oct because only "circling back" was listed. Every one
  // of these needs all its forms, or the filter is decoration.
  /\b(just checking|checking in|checking back|check(ing)? back in|follow(ing|ed)? up|circl(e|es|ed|ing) back|touch(ing|ed)? base|reach(ing|ed)? out again|wanted to see if|wanted to check|hope you'?re well|hope this (email )?finds you|bumping this|friendly reminder|good morning or afternoon|morning or afternoon|good (morning|afternoon|evening)\b)/i;

// v73: the ad that started the conversation. First ad wins; a later ad click only fills a blank.
async function saveAdRef(messengerId: string, adId: string, adTitle: string | null) {
  await sql`update fadc.leads set ad_id = coalesce(ad_id, ${adId}),
                                  ad_title = coalesce(ad_title, ${adTitle}),
                                  ad_ref_at = coalesce(ad_ref_at, now())
            where messenger_id = ${messengerId}`;
}

// v73/v80: THE ABANDONED CHAT. When someone on the team answers from the inbox the bot pauses so
// two voices never talk over each other — but that pause used to be permanent until a human
// manually un-paused it. On 8 oct ten chats sat over 3 hours: a person had stepped in, the
// customer wrote back, and nobody answered; on 15 oct James/Aaron got auto-paused right after a
// single manual message from Michel, with no way back without him remembering to un-pause. After
// 10 minutes of silence on our side (checked every 10 min by cron — worst case ~20 min), the bot
// picks the conversation back up and guides it to the booking, for ANY stage except one already
// closed_lost or completed. Never for a lead that needs a person (escalation, special pricing, a
// frustrated customer), or a message that is a hazard or an escalation, and only inside Meta's
// 24 hour window.
async function runResume(dryRun: boolean): Promise<Record<string, unknown>> {
  if (!dryRun && !insideFollowupHours()) return { resumed: 0, reason: "outside business hours" };
  const rows = await sql<{ id: string; messenger_id: string; first_name: string | null; last_text: string }[]>`
    select l.id, l.messenger_id, l.first_name, lm.text as last_text
    from fadc.leads l
    join lateral (select role, text, sent_by, created_at from fadc.messages m
                  where m.lead_id = l.id order by created_at desc limit 1) lm on true
    where l.agent_paused = true
      and l.needs_human = false
      and l.stage not in ('closed_lost', 'completed')
      and l.messenger_id ~ '^[0-9]{8,}$'
      and lm.role = 'customer'
      and lm.created_at < now() - interval '10 minutes'
      and lm.created_at > now() - interval '23 hours'
      and exists (select 1 from fadc.messages h where h.lead_id = l.id and h.sent_by = 'human')
      and not exists (select 1 from fadc.appointments a where a.lead_id = l.id and a.status = 'completed')
    order by lm.created_at asc
    limit 10`;
  const results: Array<Record<string, unknown>> = [];
  for (const r of rows) {
    const t = r.last_text ?? "";
    // A goodbye is the end of the chat, not an unanswered message. Providencia's "Have a nice day."
    // after a bad exchange must never get a sales message two hours later.
    if (/^\s*(ok(ay)?|thanks?|thank you|thx|bye|good ?bye|have a (nice|good|great) (day|night|one|evening)|no thanks?|not interested|nevermind|never mind|gracias|adi[oó]s|que tengas buen d[ií]a|no gracias|ya no)\b[\s!.\u{1F44D}]*$/iu.test(t)
        || /\b(not interested|no longer|found someone|already (booked|hired)|stop messag|unsubscribe|ya no me interesa)\b/i.test(t)) {
      results.push({ id: r.id, skipped: "goodbye" });
      continue;
    }
    if (hitEmergency(t) || hitEscalate(t) || hitReschedule(t)) {
      results.push({ id: r.id, skipped: "needs a person" });
      continue;
    }
    try {
      const history = await sql`select role, text from fadc.messages where lead_id = ${r.id}
                                order by created_at desc limit 30`;
      history.reverse();
      const hist = history as { role: string; text: string }[];
      const special = detectSpecialPricing(hist, t);
      if (special.hit) { results.push({ id: r.id, skipped: "special pricing" }); continue; }
      const [lead] = await sql`select * from fadc.leads where id = ${r.id}`;
      const es = isSpanish(hist.filter((m) => m.role === "customer").map((m) => m.text).join("\n"));
      const sent = hist.filter((m) => m.role === "agent").map((m) => m.text).slice(-3);
      const out = await callClaude(hist,
        buildStateNote(lead as unknown as Record<string, unknown>) +
        "\n\nIMPORTANT: A teammate was helping this customer and has not answered their last message for a while. " +
        "Pick the conversation up naturally as the same team (do not say you are a bot, do not mention a delay at length). " +
        "Answer their last message first, then guide them to booking with one easy choice. Do not repeat anything already sent.");
      let reply = (out.reply ?? "").trim();
      if (!reply || repeatsSent(reply, sent)) {
        results.push({ id: r.id, skipped: "no safe reply" });
        continue;
      }
      // 23 oct (Jill): the resume path told a customer "You're all set, Thursday 10-12" off
      // a slot she had CANCELLED a week earlier, and no ticket existed. Resume never books:
      // a confirmation without a live appointment goes to a person instead.
      const [{ live }] = await sql<{ live: number }[]>`
        select count(*)::int as live from fadc.appointments
        where lead_id = ${r.id} and status in ('pending','confirmed','rescheduled') and slot_date >= current_date`;
      if (live === 0 && (/\b(all set|you're booked|you are booked|booked you|locked in|see you (on )?(mon|tue|wed|thu|fri|sat|sun))/i.test(reply)
          || out.lead?.stage === "ready_to_confirm")) {
        if (!dryRun) {
          await notifyTeamFor(r.id,
            `\u{1F4C5} ${r.first_name ?? "A lead"} wants to book again but has no live appointment. A person must confirm the day.\n` +
            `They wrote: "${t.slice(0, 140)}"\nBot would have said: "${reply.slice(0, 160)}"`, "resume_needs_booking");
          // needs_human takes it out of the resume query, so the alert fires once.
          await sql`update fadc.leads set needs_human = true where id = ${r.id}`;
        }
        results.push({ id: r.id, skipped: "would confirm without a live appointment" });
        continue;
      }
      reply = reply.replace(/\s+[\u2013\u2014]\s+/g, ", ");
      if (dryRun) { results.push({ id: r.id, would_send: reply }); continue; }
      await sendMessengerReply(r.messenger_id, reply);
      await sql`insert into fadc.messages (lead_id, role, text, sent_by) values (${r.id}, 'agent', ${reply}, 'agent')`;
      await sql`update fadc.leads set agent_paused = false, last_outbound_at = now() where id = ${r.id}`;
      await notifyTeam(
        `\u{1F501} ${r.first_name ?? "A lead"} waited 10+ min with no answer after a person stepped in. The bot picked it back up.\n` +
        `They wrote: "${t.slice(0, 140)}"\nBot: "${reply.slice(0, 160)}"`,
        "resumed",
      );
      results.push({ id: r.id, resumed: true });
    } catch (e) {
      results.push({ id: r.id, error: e instanceof Error ? e.message : String(e) });
    }
  }
  return { resumed: results.filter((x) => x.resumed).length, results };
}

// v74: THE UNANSWERED HUMAN QUESTION. A teammate steps in, asks "What day works best for you?"
// or "Can I have your full address?", the customer goes quiet, and because the bot is paused the
// follow-up never runs. Annette, Digna and 08816 all died there on 7 and 8 oct. After 3 hours the
// bot sends ONE friendly nudge that makes the teammate's question easy to answer, inside Meta's
// 24 h window. It stays paused: the chat still belongs to the person who stepped in.
async function runHumanNudge(dryRun: boolean): Promise<Record<string, unknown>> {
  if (!dryRun && !insideFollowupHours()) return { nudged: 0, reason: "outside business hours" };
  const rows = await sql<{ id: string; messenger_id: string; first_name: string | null; human_text: string }[]>`
    select l.id, l.messenger_id, l.first_name, lm.text as human_text
    from fadc.leads l
    join lateral (select role, text, sent_by, created_at from fadc.messages m
                  where m.lead_id = l.id order by created_at desc limit 1) lm on true
    where l.agent_paused = true
      and l.needs_human = false
      and l.stage in ('new', 'qualifying', 'ready_to_confirm')
      and l.messenger_id ~ '^[0-9]{8,}$'
      and lm.sent_by = 'human'
      and lm.text ~ '[?¿]'
      and lm.created_at < now() - interval '3 hours'
      and l.last_inbound_at > now() - interval '22 hours'
      and (l.no_followup_until is null or l.no_followup_until <= now())
    order by lm.created_at asc
    limit 10`;
  const results: Array<Record<string, unknown>> = [];
  for (const r of rows) {
    try {
      const history = await sql`select role, text from fadc.messages where lead_id = ${r.id}
                                order by created_at desc limit 30`;
      history.reverse();
      const hist = history as { role: string; text: string }[];
      const lastCust = [...hist].reverse().find((m) => m.role === "customer")?.text ?? "";
      if (/\b(not interested|no thanks|stop|found someone|already (booked|hired))\b/i.test(lastCust)) {
        results.push({ id: r.id, skipped: "customer declined" });
        continue;
      }
      const sent = hist.filter((m) => m.role === "agent").map((m) => m.text).slice(-3);
      const out = await callClaude([...hist, {
        role: "customer",
        text: `(Internal note, NOT from the customer. Never quote it. A teammate asked them: "${r.human_text.slice(0, 200)}" and they have not answered for a few hours. Write ONE short, warm nudge (max 25 words) from the same team that makes that question easy to answer, ideally with two simple options. Do not repeat the teammate's words, no "just checking in", no pressure. Same JSON format; leave lead fields as they are.)`,
      }]);
      let reply = scrubFillers(stripDashes(out.reply ?? "")).trim();
      if (!reply || repeatsSent(reply, sent) || BANNED_FOLLOWUP_RE.test(reply)) {
        results.push({ id: r.id, skipped: "no clean draft" });
        continue;
      }
      if (dryRun) { results.push({ id: r.id, would_send: reply }); continue; }
      await sql`insert into fadc.messages (lead_id, role, text, sent_by) values (${r.id}, 'agent', ${reply}, 'followup')`;
      await sendMessengerReply(r.messenger_id, reply);
      await sql`update fadc.leads set last_outbound_at = now() where id = ${r.id}`;
      await notifyTeam(
        `\u{1F44B} ${r.first_name ?? "A lead"} never answered your question ("${r.human_text.slice(0, 80)}"). The bot sent one nudge: "${reply.slice(0, 140)}"`,
        "human_nudge",
      );
      results.push({ id: r.id, nudged: true });
    } catch (e) {
      results.push({ id: r.id, error: e instanceof Error ? e.message : String(e) });
    }
  }
  return { nudged: results.filter((x) => x.nudged).length, results };
}

// v81 — THE LEAK. Between 10 and 13 oct the ads produced 19 conversations, $129 of spend and
// ZERO appointments, while the bot ran clean with no errors. The bot was never the problem: it
// cannot close, only a person can. What broke is that nobody SAW the qualified leads. Jill Gray
// handed over her ZIP, her phone and her name at 15:20 on 11 oct; nothing was booked in Supabase,
// Ilay booked her by WhatsApp hours later, and a day after that a teammate was still asking her
// which day worked. Meanwhile 4 special_pricing leads (multi-system, $300+ tickets) went untouched.
//
// So this sweep does one thing: a lead that gave us BOTH a ZIP and a phone and has no appointment
// gets escalated to Telegram once, 4 hours after their last message, with everything a person
// needs to pick up the phone. One alert per lead, ever (the notifications row is the ledger).
// It never messages the customer; the customer side is already covered by runFollowups and
// runHumanNudge. This is purely a nudge at the humans.
async function runLeakAlert(dryRun: boolean): Promise<Record<string, unknown>> {
  const rows = await sql<{
    id: string; first_name: string | null; phone: string; zip: string | null; town: string | null;
    service: string | null; address: string | null; stage: string; ad_title: string | null;
    assigned_to: string | null; hours_cold: number; last_customer: string | null;
  }[]>`
    select l.id, l.first_name, l.phone, l.zip, l.town, l.service, l.address, l.stage,
           l.ad_title, l.assigned_to,
           round(extract(epoch from (now() - l.last_inbound_at)) / 3600, 1)::float as hours_cold,
           (select m.text from fadc.messages m
             where m.lead_id = l.id and m.role = 'customer'
             order by m.created_at desc limit 1) as last_customer
    from fadc.leads l
    where l.phone is not null
      and l.zip is not null
      and l.stage <> 'closed_lost'
      and l.last_inbound_at is not null
      and l.last_inbound_at < now() - interval '4 hours'
      -- A lead that went cold a week ago is history, not a leak worth waking anyone for.
      and l.last_inbound_at > now() - interval '7 days'
      and not exists (
        select 1 from fadc.appointments a
        where a.lead_id = l.id
          and a.status in ('pending', 'confirmed', 'rescheduled', 'completed'))
      and not exists (
        select 1 from fadc.notifications n
        where n.lead_id = l.id and n.kind = 'leak')
    order by l.last_inbound_at desc
    limit 10`;

  const results: Array<Record<string, unknown>> = [];
  for (const r of rows) {
    // "not interested" with a phone on file is a closed door, not a leak. Don't send a
    // teammate to call someone who already said no.
    // Caught by the first dry run: a lead whose last words were "Cancel everything thanks"
    // sailed through, because the list only knew "no thanks". Cancellations and "no longer
    // interested" are the same closed door.
    if (r.last_customer && /\b(not interested|no longer (interested|need)|no thanks|no thank you|stop|found someone|already (booked|hired|done)|unsubscribe|cancel( |l?ed|l?ing)|never ?mind)\b/i.test(r.last_customer)) {
      results.push({ id: r.id, skipped: "customer declined" });
      continue;
    }
    const where = r.address ?? [r.town, r.zip].filter(Boolean).join(" ") ?? "area unknown";
    const text =
      `\u{1F4B8} QUALIFIED LEAD, NOBODY BOOKED IT\n` +
      `${r.first_name ?? "Lead"} — ${r.hours_cold}h since their last message\n` +
      `\u{1F4DE} ${r.phone}\n` +
      `\u{1F4CD} ${where}\n` +
      `\u{1F527} ${r.service ?? "service not stated"}\n` +
      (r.ad_title ? `\u{1F4E3} ${r.ad_title}\n` : "") +
      (r.assigned_to ? `\u{1F464} assigned to ${r.assigned_to}\n` : "") +
      (r.last_customer ? `They wrote: "${r.last_customer.slice(0, 140)}"\n` : "") +
      `→ Call them. There is no appointment in the CRM for this lead.`;
    if (dryRun) { results.push({ id: r.id, would_alert: text }); continue; }
    await notifyTeamFor(r.id, text, "leak");
    results.push({ id: r.id, alerted: true, name: r.first_name, hours_cold: r.hours_cold });
  }
  return { leaks: results.filter((x) => x.alerted).length, results };
}

// ── 15-minute unanswered alert ──────────────────────────────────────
// v85. The leak alert above only fires for a qualified lead (phone + ZIP) after 4 hours cold —
// that is the right bar for "nobody booked this", but it is much too slow for "the bot never
// answered at all" (agent_paused while no human replied, needs_human, or a silent error). Any
// customer message that sits 15+ minutes with no agent reply after it is a dropped conversation
// regardless of what stage or fields the record has, so this checks every lead, not just
// qualified ones. Dedup is per MESSAGE, not per lead: a notification counts as covering this
// message only if it was created after the message's own timestamp, so the same lead can be
// re-alerted if they write again later and are dropped again.
async function runUnansweredAlert(dryRun: boolean): Promise<Record<string, unknown>> {
  const rows = await sql<{
    id: string; first_name: string | null; phone: string | null; zip: string | null;
    town: string | null; last_customer: string | null; minutes_cold: number;
  }[]>`
    select l.id, l.first_name, l.phone, l.zip, l.town,
           (select m.text from fadc.messages m where m.lead_id = l.id and m.role = 'customer'
             order by m.created_at desc limit 1) as last_customer,
           round(extract(epoch from (now() - l.last_inbound_at)) / 60)::int as minutes_cold
    from fadc.leads l
    where l.last_inbound_at is not null
      and l.last_inbound_at < now() - interval '15 minutes'
      and l.last_inbound_at > now() - interval '3 hours'
      and l.stage <> 'closed_lost'
      and not exists (
        select 1 from fadc.messages m
        where m.lead_id = l.id and m.role = 'agent' and m.created_at > l.last_inbound_at)
      and not exists (
        select 1 from fadc.notifications n
        where n.lead_id = l.id and n.kind = 'unanswered15' and n.created_at > l.last_inbound_at)
    order by l.last_inbound_at
    limit 10`;

  const results: Array<Record<string, unknown>> = [];
  for (const r of rows) {
    const where = [r.town, r.zip].filter(Boolean).join(" ") || "area unknown";
    const text =
      `⚠️ NO REPLY IN ${r.minutes_cold} MIN\n` +
      `${r.first_name ?? "A lead"} wrote and nobody — bot or human — answered.\n` +
      (r.phone ? `\u{1F4DE} ${r.phone}\n` : "No phone on file — can only be reached in Messenger.\n") +
      `\u{1F4CD} ${where}\n` +
      (r.last_customer ? `They wrote: "${r.last_customer.slice(0, 140)}"` : "") ;
    if (dryRun) { results.push({ id: r.id, would_alert: text }); continue; }
    await notifyTeamFor(r.id, text, "unanswered15");
    results.push({ id: r.id, alerted: true, name: r.first_name, minutes_cold: r.minutes_cold });
  }
  return { unanswered: results.filter((x) => x.alerted).length, results };
}

// ── Amount chase ─────────────────────────────────────────────────────
// v84 — REVENUE THAT IS NEVER RECORDED CANNOT BE MANAGED. The amount prompt went out exactly
// once, right after the "Done" button was tapped. If nobody replied to it, the job's revenue
// was lost for good: David Spirito's job on 15 oct was closed as done with ticket_amount null
// and never reached closed_jobs, so it is missing from every weekly P&L number. With one
// profitable week out of five, a job we forgot to price is not a rounding error. This asks
// again once a day, up to five times, and then says plainly that the number is gone.
async function runAmountChase(dryRun: boolean): Promise<Record<string, unknown>> {
  if (!TG_TOKEN || !TG_CHAT) return { chased: 0, reason: "telegram not configured" };
  const rows = await sql<{
    id: string; address: string | null; lead_address: string | null;
    first_name: string | null; last_name: string | null; amount_chases: number;
  }[]>`
    select a.id, a.address, l.address as lead_address, l.first_name, l.last_name, a.amount_chases
    from fadc.appointments a join fadc.leads l on l.id = a.lead_id
    where a.ticket_amount is null
      and (a.outcome_reason = 'done' or a.status = 'completed')
      and a.closed_at is not null
      and a.closed_at > now() - interval '30 days'
      and a.amount_chases < 5
      and (a.amount_chased_at is null or a.amount_chased_at < now() - interval '20 hours')
    order by a.closed_at
    limit 5`;
  if (rows.length === 0) return { chased: 0 };

  const results: Record<string, unknown>[] = [];
  for (const r of rows) {
    const who = [r.first_name, r.last_name].filter(Boolean).join(" ") || "this customer";
    const where = r.address ?? r.lead_address ?? "";
    const last = r.amount_chases + 1 >= 5;
    if (dryRun) { results.push({ id: r.id, who, try: r.amount_chases + 1 }); continue; }
    try {
      const p = await tgApi("sendMessage", {
        chat_id: TG_CHAT,
        text: `\u{1F4B5} Still missing the amount for ${who}${where ? ` (${where})` : ""}.\n` +
          `This job is done but it counts as $0 in the weekly P&L until you reply with the number.\n` +
          `Reply to THIS message with just the number (e.g. 249).` +
          (last ? `\n\nThis is the last reminder for this job.` : ""),
        reply_markup: { force_reply: true, selective: true },
      });
      await sql`update fadc.appointments
                set amount_chased_at = now(), amount_chases = amount_chases + 1,
                    amount_prompt_msg_id = ${p?.ok ? p.result.message_id : null},
                    updated_at = now()
                where id = ${r.id}`;
      results.push({ id: r.id, who, try: r.amount_chases + 1, sent: !!p?.ok });
    } catch (e) {
      results.push({ id: r.id, error: e instanceof Error ? e.message : String(e) });
    }
  }
  return { chased: results.filter((x) => x.sent).length, results };
}

// ── Ticket sweep ────────────────────────────────────────────────────
// Catches confirmed appointments that were booked by a human (CRM, Telegram, or
// direct DB) and therefore never went through the bot's ready_to_confirm flow
// that normally builds + sends the Telegram ticket. Runs on every followup sweep.
async function runTicketSweep(dryRun: boolean): Promise<Record<string, unknown>> {
  // Find appointments with status confirmed/pending that have NO matching
  // "appointment" notification — meaning the ticket was never sent to Telegram.
  const rows = await sql<{
    id: string; lead_id: string; service: string | null; address: string | null;
    slot_date: string | null; slot_window: string | null; exact_window: string | null;
    notes: string | null; tech: string | null;
    first_name: string | null; last_name: string | null; phone: string | null;
    lead_address: string | null; lead_zip: string | null; lead_service: string | null; call_after: string | null;
    job_scale: string | null;
  }[]>`
    select a.id, a.lead_id, a.service, a.address, a.slot_date::text as slot_date, a.slot_window,
           a.exact_window, a.notes, a.tech,
           l.first_name, l.last_name, l.phone,
           l.address as lead_address, l.zip as lead_zip, l.service as lead_service, l.call_after, l.job_scale
    from fadc.appointments a
    join fadc.leads l on l.id = a.lead_id
    where a.status in ('confirmed', 'pending')
      and not exists (
        select 1 from fadc.notifications n
        where n.lead_id = a.lead_id and n.kind = 'appointment'
      )
      and a.created_at > now() - interval '7 days'
  `;
  if (rows.length === 0) return { ok: true, sent: 0, reason: "no orphan tickets" };

  const results: Record<string, unknown>[] = [];
  for (const r of rows) {
    const name = [r.first_name, r.last_name].filter(Boolean).join(" ") || null;
    const ticket = buildTicket({
      name,
      jobType: null,
      service: r.service ?? r.lead_service,
      day: r.slot_date ? `${Number(r.slot_date.slice(5, 7))}/${Number(r.slot_date.slice(8, 10))}` : null,
      window: r.exact_window ?? r.slot_window,
      address: r.address ?? r.lead_address,
      zip: r.lead_zip ?? null,
      phone: r.phone,
      context: null,
      callAfter: r.call_after,
      scale: r.job_scale,
    });
    if (dryRun) {
      results.push({ lead_id: r.lead_id, name, would_send: ticket });
      continue;
    }
    // Update the appointment notes with the generated ticket
    await sql`update fadc.appointments set notes = ${ticket}, updated_at = now() where id = ${r.id}`;
    // Send ticket to Telegram
    await notifyTeamFor(r.lead_id,
      ticket + "\n\n" +
      (r.tech ? `Tech: ${r.tech}` : "⚠️ Tech: NOT ASSIGNED") + "\n" +
      "The customer has been CONFIRMED for this slot. Assign a tech, or reschedule them if nobody is free.\n" +
      "(Auto-detected: human-booked appointment without ticket)",
      "appointment",
    );
    results.push({ lead_id: r.lead_id, name, sent: true });
  }
  return { ok: true, sent: results.filter((x: any) => x.sent).length, results };
}

// A booking moved by hand (SQL, the CRM, a person in the Meta inbox) never reached Telegram:
// Sonia was moved from Thu 10/22 to Mon 10/26 and the team only had the old ticket. This
// sweep rebuilds the ticket for every upcoming appointment and compares its first line
// (the "IVY JOB — <day>, <window>" header) with the last ticket the team received for that
// lead. If the header changed, the new ticket goes out as TICKET CHANGED. No schema change:
// the last notification is the record of what the team already knows.
async function runTicketChangeSweep(dryRun: boolean): Promise<Record<string, unknown>> {
  const rows = await sql<{
    id: string; lead_id: string; service: string | null; address: string | null;
    slot_date: string | null; slot_window: string | null; exact_window: string | null;
    status: string; tech: string | null;
    first_name: string | null; last_name: string | null; phone: string | null;
    lead_address: string | null; lead_zip: string | null; lead_service: string | null;
    call_after: string | null; job_scale: string | null; last_text: string | null;
  }[]>`
    select a.id, a.lead_id, a.service, a.address, a.slot_date::text as slot_date, a.slot_window,
           a.exact_window, a.status, a.tech,
           l.first_name, l.last_name, l.phone,
           l.address as lead_address, l.zip as lead_zip, l.service as lead_service, l.call_after, l.job_scale,
           (select n.text from fadc.notifications n
             where n.lead_id = a.lead_id and n.kind in ('appointment', 'ticket_update')
             order by n.created_at desc limit 1) as last_text
    from fadc.appointments a
    join fadc.leads l on l.id = a.lead_id
    where a.status in ('confirmed', 'pending', 'rescheduled')
      and a.slot_date > current_date
  `;
  const results: Record<string, unknown>[] = [];
  for (const r of rows) {
    // No ticket ever sent: that is the orphan sweep's job, not this one.
    if (!r.last_text) continue;
    const name = [r.first_name, r.last_name].filter(Boolean).join(" ") || null;
    const ticket = buildTicket({
      name, jobType: null, service: r.service ?? r.lead_service,
      day: r.slot_date ? `${Number(r.slot_date.slice(5, 7))}/${Number(r.slot_date.slice(8, 10))}` : null,
      window: r.exact_window ?? r.slot_window,
      address: r.address ?? r.lead_address, zip: r.lead_zip ?? null, phone: r.phone,
      context: null, callAfter: r.call_after, scale: r.job_scale,
    });
    const header = ticket.split("\n")[0];
    if (r.last_text.includes(header)) continue;
    if (dryRun) { results.push({ lead_id: r.lead_id, name, would_send: header }); continue; }
    await sql`update fadc.appointments set notes = ${ticket}, updated_at = now() where id = ${r.id}`;
    await notifyTeamFor(r.lead_id,
      "✏️ TICKET CHANGED (new date or time)\n\n" + ticket + "\n\n" +
      (r.tech ? `Tech: ${r.tech}` : "⚠️ Tech: NOT ASSIGNED") + "\n" +
      `Status: ${r.status}\n(Auto-detected: the booking changed after the last ticket went out)`,
      "ticket_update",
    );
    results.push({ lead_id: r.lead_id, name, sent: header });
  }
  return { ok: true, sent: results.filter((x: any) => x.sent).length, results };
}

const REVIEWS_LINK = "https://www.facebook.com/profile.php?id=61593572532326&sk=reviews";
// Someone who had a bad time gets a person, never a review link.
// Someone asking us to stop is the one message we must never answer with another pitch.
// One lead wrote "Stop" on 18 oct, "No" on the 19th, and "Please stop" on the 20th, and the
// followup cron kept going the whole time: nine people had asked, and seven of them got
// another message afterwards. Meta restricts Pages for exactly this, and it only takes one
// spam report. Narrow on purpose: "stop by", "non-stop" and "the dryer stopped" are not
// opt-outs, so the word has to open the message or carry a verb that makes it a request.
const STOP_RE =
  /(^\s*stop\b(?!\s+(by|in|over|at)\b))|(^\s*(unsubscribe|basta)\b)|\b(please stop|stop (messaging|texting|contacting|writing|calling|sending)|unsubscribe|leave me alone|(don'?t|do not|never)\s+(ever\s+|again\s+)?(contact|message|text|msg|dm|write|reach out to|bother) me|(no more (messages|texts|texting|emails))|(quit|stop) (texting|messaging|contacting|bothering)|(remove|take) me off|opt me out)\b|(ya no me (escrib|contact|mand|llam))|(dej(en|a|e)n? de (escribir|mandar|molestar|llamar))|(^\s*no me escrib)/i;

const BAD_EXPERIENCE_RE =
  /\b(terrible|horrible|awful|bad|worst|disgusting|rip.?off|scam|complain|refund|disappointed|angry|furious|never again|no good|do not recommend|don.?t recommend|malo|p[eé]simo|asco|estafa|queja|reembolso|decepcionado|enojado|nunca m[aá]s)\b/i;

// ── Review follow-ups ───────────────────────────────────────────────
// Sends "How did everything go?" to customers whose appointment was
// marked done 2-24h ago AND whose Meta 24h messaging window is still
// open (last_inbound_at < 23h).  Their reply resets the window and
// triggers the review-request intercept in processInbound.
async function runReviewFollowups(dryRun: boolean): Promise<Record<string, unknown>> {
  const rows = await sql<{ id: string; messenger_id: string; first_name: string | null; lead_id: string; phone: string | null; ticket_amount: string | null }[]>`
    select a.id, l.messenger_id, l.first_name, l.id as lead_id, l.phone, a.ticket_amount
    from fadc.appointments a
    join fadc.leads l on l.id = a.lead_id
    where a.outcome_reason = 'done'
      -- WHY THIS WINDOW IS 7 DAYS AND NOT 24 HOURS (21 oct). The old bound was
      -- "closed 2-24h ago", which assumed the job gets closed out the same day it
      -- happens. It does not: close-outs here land days late, so by the time
      -- outcome_reason became 'done' the lead had already aged out and was never
      -- asked. Jose Archila left a $270 tip and was never asked for a review.
      and a.closed_at between now() - interval '7 days' and now() - interval '2 hours'
      and l.review_requested = false
      and l.review_followup_at is null
      -- The 23h "is the Meta window still open?" gate used to live here and was the
      -- real reason this cron almost never fired: a job closed out a day late always
      -- failed it, silently and permanently. We now ATTEMPT the send and handle the
      -- (#10) closed-window error below, which turns an unreachable customer into a
      -- notification Michel can act on by text instead of a lead that vanishes.
      and l.messenger_id is not null
  `;
  if (rows.length === 0) return { ok: true, sent: 0, reason: "no eligible review leads" };

  const results: Record<string, unknown>[] = [];
  for (const r of rows) {
    // Detect language from customer's message history
    const msgs = (await sql`select text from fadc.messages where lead_id = ${r.lead_id} and role = 'customer' order by created_at`).map((m: any) => m.text);
    const es = isSpanish(msgs.join("\n"));
    // One message, not two. The old flow asked "how did it go?" and only sent the link if
    // they replied — of the one lead that ever got here, nobody replied, so the link never
    // went out. The link ships with the ask now, so a reply is a bonus, not a requirement.
    // The "if something was off, tell me here" line is deliberate: it routes a bad job to
    // us instead of to the public page.
    const msg = es
      ? `¡Hola${r.first_name ? ` ${r.first_name}` : ""}! ¿Cómo te fue con el servicio? Si el equipo te atendió bien, una reseña nos ayudaría mucho — así otras familias nos encuentran. Y te damos $20 de descuento en tu próximo servicio.\n\n${REVIEWS_LINK}\n\nY si algo no salió bien, dímelo por aquí y lo resuelvo.`
      : `Hi${r.first_name ? ` ${r.first_name}` : ""}! How did everything go with the service? If the team took good care of you, a quick review would help us a lot — it's how other families find us. And it gets you $20 off your next service.\n\n${REVIEWS_LINK}\n\nAnd if anything wasn't right, just tell me here and I'll make it right.`;

    if (dryRun) {
      results.push({ lead_id: r.lead_id, name: r.first_name, would_send: msg });
      continue;
    }
    const [reviewMsgRow] = await sql<{ id: string }[]>`
      insert into fadc.messages (lead_id, role, text, sent_by)
      values (${r.lead_id}, 'agent', ${msg}, 'review_followup') returning id`;
    try {
      await sendMessengerReply(r.messenger_id, msg);
      await sql`update fadc.leads set review_followup_at = now(), review_requested = true where id = ${r.lead_id}`;
      results.push({ lead_id: r.lead_id, name: r.first_name, sent: true });
    } catch (e) {
      // The customer's 24h Meta window shut before we closed the job out. The ask is still
      // worth making, it just has to leave by text instead, so this becomes a notification
      // with the number in it rather than a review we quietly never requested. review_requested
      // is still set: without it this lead comes back every single cron run and re-notifies.
      const em = String((e as Error)?.message ?? e);
      const windowClosed = /outside of allowed window|\(#10\)|24.?hour/i.test(em);
      await sql`update fadc.messages set sent_by = 'review_failed', text = '[NOT SENT] ' || text
                 where id = ${reviewMsgRow?.id ?? null}`;
      await sql`update fadc.leads set review_followup_at = now(), review_requested = true where id = ${r.lead_id}`;
      if (windowClosed) {
        const paid = r.ticket_amount ? ` (paid $${r.ticket_amount})` : "";
        await notifyTeam(
          `⭐ ${r.first_name ?? "A customer"}${paid} is due a review ask, but their Messenger window closed.\n` +
          `${r.phone ? `Text them: ${r.phone}` : "No phone on file, check the thread."}\n\n` +
          `Suggested: "Hi${r.first_name ? ` ${r.first_name}` : ""}! How did everything go? If the team took good care of you, a quick review helps a lot, and it gets you $20 off next time: ${REVIEWS_LINK}"`,
          "review_window_closed",
        );
      }
      results.push({ lead_id: r.lead_id, name: r.first_name, sent: false, error: em.slice(0, 120) });
    }
  }
  return { ok: true, sent: results.filter((x: any) => x.sent).length, results };
}

// v87 — THE APPOINTMENT THAT WAS NEVER AN APPOINTMENT (found 21 oct). runLeakAlert watches
// leads with NO appointment. The moment an appointment row exists, every watcher in this file
// stops looking — and that is exactly where ten customers died. On 21 oct all eleven open
// appointments had no technician assigned and nine had no date at all: Digna sat 13 days,
// Mike 14, Ramiro 15, Duan 15.8. Every one of them was paused because a teammate had replied
// once from the inbox and never came back, so no nudge could run either.
//
// The numbers that forced this: appointments WITH a tech assigned closed and got paid 2 of 3
// times; appointments WITHOUT one did so 2 of 23. Assignment predicts revenue better than
// zone, ad, service or price. Nothing was watching the single strongest predictor in the data.
//
// Two different failures live in one row, and they need different owners:
//   - no DATE   → the customer can fix it, so ask them, but only while Meta's window is open.
//   - no TECH   → only Michel can fix it; the customer must never be asked about dispatch.
// A closed window means the customer is unreachable by Messenger forever, so that becomes a
// phone number in a notification instead of a message that silently fails.
async function runStaleAppointments(dryRun: boolean): Promise<Record<string, unknown>> {
  const rows = await sql<{
    id: string; lead_id: string; messenger_id: string | null; first_name: string | null;
    phone: string | null; town: string | null; zip: string | null; service: string | null;
    scheduled_at: string | null; slot_date: string | null; tech: string | null; status: string;
    date_chases: number; date_chased_at: string | null; dispatch_alerted_at: string | null;
    declined_slots: string[] | null; preferred_window: string | null;
    window_open: boolean; silence_hours: number;
  }[]>`
    select a.id, a.scheduled_at, a.slot_date, a.tech, a.status, a.date_chases, a.date_chased_at,
           a.dispatch_alerted_at,
           l.id as lead_id, l.messenger_id, l.first_name, l.phone, l.town, l.zip, l.service,
           l.declined_slots, l.preferred_window,
           (l.last_inbound_at > now() - interval '22 hours') as window_open,
           round(extract(epoch from (now() - l.last_inbound_at)) / 3600, 1)::float as silence_hours
    from fadc.appointments a
    join fadc.leads l on l.id = a.lead_id
    where a.status in ('pending', 'rescheduled')
      -- A real date can live in either column: the normal booking flow only ever writes
      -- slot_date, so scheduled_at being null does NOT mean the date is unconfirmed.
      and ((a.scheduled_at is null and a.slot_date is null) or a.tech is null)
      -- A booking still being written by the live chat is not stale. Two hours is long
      -- enough that the conversation has ended one way or the other.
      and a.created_at < now() - interval '2 hours'
      and l.stage <> 'closed_lost'
      and (l.no_followup_until is null or l.no_followup_until <= now())
      -- A pause means "a person is handling this". It stops meaning that once the person
      -- has been silent for six hours; at that point the pause is just where the lead died.
      and (
        l.agent_paused = false
        or not exists (
          select 1 from fadc.messages h
           where h.lead_id = l.id and h.sent_by = 'human'
             and h.created_at > now() - interval '6 hours'
        )
      )
    order by a.created_at asc
    limit 20`;

  const results: Array<Record<string, unknown>> = [];

  for (const r of rows) {
    try {
      const needsDate = !r.scheduled_at && !r.slot_date;
      const needsTech = !r.tech;
      const who = r.first_name ?? "A customer";
      const where = [r.town, r.zip].filter(Boolean).join(" ");

      // ── The customer can only help with the date, and only if we can still reach them.
      if (needsDate && r.window_open && r.messenger_id && /^[0-9]{8,}$/.test(r.messenger_id)) {
        // Two asks, then it belongs to a human. A third message about the same missing date
        // is how a booking turns into an opt-out.
        if (r.date_chases >= 2) {
          results.push({ appointment: r.id, name: r.first_name, skipped: "already asked twice" });
        } else if (r.date_chased_at && Date.parse(r.date_chased_at) > Date.now() - 20 * 3600 * 1000) {
          results.push({ appointment: r.id, name: r.first_name, skipped: "asked within 20h" });
        } else if (!insideFollowupHours() && !dryRun) {
          results.push({ appointment: r.id, name: r.first_name, skipped: "outside business hours" });
        } else {
          const msgs = (await sql`select text from fadc.messages
                                   where lead_id = ${r.lead_id} and role = 'customer'
                                   order by created_at`).map((m: any) => m.text);
          const es = isSpanish(msgs.join("\n"));
          const slotObj = await nextSlot(es, r.declined_slots ?? [], r.preferred_window ?? null);
          const slot = slotObj?.phrase ?? null;
          const svc = r.service ?? (es ? "el servicio" : "the service");

          // Deliberately an offer, never "what day works for you". That open question is the
          // one this codebase has lost leads to over and over; a yes/no is a smaller ask.
          const msg = slot
            ? (es
              ? `¡Hola${r.first_name ? ` ${r.first_name}` : ""}! Tengo tu ${svc} apartado pero sin día confirmado. ¿Te queda ${slot}?`
              : `Hi${r.first_name ? ` ${r.first_name}` : ""}! I have your ${svc} held but no day locked in yet. Does ${slot} work?`)
            : (es
              ? `¡Hola${r.first_name ? ` ${r.first_name}` : ""}! Tengo tu ${svc} apartado pero sin día confirmado. ¿Esta semana o la próxima?`
              : `Hi${r.first_name ? ` ${r.first_name}` : ""}! I have your ${svc} held but no day locked in yet. This week or next?`);

          if (dryRun) {
            results.push({ appointment: r.id, name: r.first_name, would_send: msg });
          } else {
            await sql`insert into fadc.messages (lead_id, role, text, sent_by)
                      values (${r.lead_id}, 'agent', ${msg}, 'date_chase')`;
            await sendMessengerReply(r.messenger_id, msg);
            await sql`update fadc.appointments
                         set date_chased_at = now(), date_chases = date_chases + 1
                       where id = ${r.id}`;
            await sql`update fadc.leads set last_outbound_at = now() where id = ${r.lead_id}`;
            results.push({ appointment: r.id, name: r.first_name, asked_for_date: true });
          }
        }
      }

      // ── Everything the customer cannot fix becomes ONE message to the team, never two.
      // Missing date and missing tech almost always occur together (7 of the 9 found on
      // 21 oct had both), so alerting per-problem would have doubled the noise on the very
      // first run and taught Michel to ignore the channel.
      const unreachable = needsDate && !r.window_open;
      if ((unreachable || needsTech) && !r.dispatch_alerted_at) {
        const missing: string[] = [];
        if (needsDate) missing.push("no date");
        if (needsTech) missing.push("no technician");
        const days = Math.max(1, Math.round(r.silence_hours / 24));

        const body =
          `\u{1F527} ${who}${where ? ` (${where})` : ""} — ${r.service ?? "job"} booked with ${missing.join(" and ")}.\n` +
          (unreachable
            ? `Their Messenger window closed, silent ${days} day${days === 1 ? "" : "s"}, so this has to be a call.\n`
            : needsDate
            ? `Still reachable on Messenger; the bot is asking them for a day.\n`
            : `Date is set${(r.scheduled_at ?? r.slot_date) ? ` for ${String(r.scheduled_at ?? r.slot_date).slice(0, 10)}` : ""} — it just needs a tech.\n`) +
          (r.phone ? `${unreachable ? "Call them" : "Customer"}: ${r.phone}` : "No phone on file — check the thread.");

        if (!dryRun) {
          await notifyTeam(body, "appointment_stuck");
          await sql`update fadc.appointments set dispatch_alerted_at = now() where id = ${r.id}`;
        }
        results.push({ appointment: r.id, name: r.first_name, alerted: missing.join("+"), would_send: dryRun ? body : undefined });
      }
    } catch (e) {
      results.push({ appointment: r.id, error: e instanceof Error ? e.message : String(e) });
    }
  }

  return {
    checked: rows.length,
    asked: results.filter((x) => x.asked_for_date).length,
    alerted: results.filter((x) => x.alerted).length,
    results,
  };
}

// v130: cold SMS outreach to technician_pipeline (fadc.technicians) prospects — daily, capped,
// B2B-hours-only, and intentionally NOT auto-sent to customers (see runStaleAppointments /
// runAmountChase for the customer-facing watchers, which still go through Messenger). Michel
// asked for a system that texts technician prospects for him instead of him sending by hand;
// this is scoped to technicians only because a cold text to a customer carries brand risk a
// text to a prospective subcontractor doesn't.
function insideOutreachHours(): boolean {
  const { weekday, hour } = bostonNow();
  const isWeekday = !["Sat", "Sun"].includes(weekday);
  // Narrower than customer follow-up hours on purpose: a B2B cold text at 9:05am or 6:45pm
  // reads as a telemarketer. 10am-5pm Mon-Fri only.
  return isWeekday && hour >= 10 && hour < 17;
}

async function runTechnicianOutreach(dryRun: boolean): Promise<Record<string, unknown>> {
  if (!dryRun && !insideOutreachHours()) {
    return { ok: true, sent: 0, reason: "outside outreach hours", boston: bostonNow() };
  }
  // Daily cap lives in fadc.settings so it can be ramped (20 → 30 …) without a redeploy.
  // Env var stays as the fallback; hard ceiling of 60 so a typo can't blast the list.
  const [capRow] = await sql<{ value: string }[]>`
    select value from fadc.settings where key = 'tech_outreach_daily_cap'`;
  const capSetting = Number(capRow?.value);
  const dailyCap = Math.min(60, Number.isFinite(capSetting) && capSetting > 0 ? capSetting : TECH_OUTREACH_DAILY_CAP);
  const rows = await sql<{ id: string; name: string; base_town: string | null; phone: string | null }[]>`
    select id, name, base_town, phone
    from fadc.technicians
    where stage = 'prospect'
      and contacted_at is null
      and opted_out = false
      and phone is not null
      and coalesce(zone, '') <> 'nj'
      -- 8xx = central de franquicia: no recibe SMS y no es el técnico
      and right(regexp_replace(phone, '\\D', '', 'g'), 10) !~ '^8(00|33|44|55|66|77|88)'
    -- Zonas hueco primero (donde Afek e Ilay no llegan: sur de NH, Merrimack
    -- Valley, North Shore, SE MA/Cape, Springfield — zone 'other'), sur de NH
    -- al frente; luego Worcester/Providence, y al final Hartford/Boston.
    order by case
               when zone = 'other' and base_town ~* '\\mNH\\M' then 0
               when zone = 'other' then 1
               when zone in ('worcester','providence') then 2
               when zone = 'hartford' then 3
               else 4
             end,
             created_at desc
    limit ${dailyCap}
  `;

  const results: Array<Record<string, unknown>> = [];
  for (const r of rows) {
    const town = r.base_town ? ` in the ${r.base_town} area` : "";
    const msg = `Hi, this is Michel with Vonkam Home Services (air duct/dryer vent, MA/NH/CT/RI). `
      + `We have duct & vent jobs already sold and scheduled${town} and need a sub to run them `
      + `-- customer pays you on site, same day, you keep 60%. Interested? Reply YES and I'll call you. `
      + `Not interested? Reply STOP and we won't text you again.`;
    if (dryRun) {
      results.push({ id: r.id, name: r.name, phone: r.phone, would_send: msg });
      continue;
    }
    const send = await sendSMS(r.phone!, msg);
    if (send.ok) {
      await sql`update fadc.technicians
                 set contacted_at = now(), sms_sent_at = now(), sms_attempts = sms_attempts + 1
                 where id = ${r.id}`;
    }
    results.push({ id: r.id, name: r.name, phone: r.phone, sent: send.ok, error: send.error });
  }

  const sentOk = results.filter((x) => x.sent);
  if (sentOk.length > 0) {
    const names = sentOk.map((x) => x.name).join(", ");
    await notifyTeam(`📞 Texted ${sentOk.length} technician prospect${sentOk.length === 1 ? "" : "s"} today: ${names}`, "tech_outreach");
  }
  const failed = results.filter((x) => x.sent === false);
  if (failed.length > 0) {
    await notifyTeam(`⚠️ ${failed.length} technician text(s) failed to send: ${failed.map((x) => `${x.name} (${x.error})`).join("; ")}`, "tech_outreach_error");
  }
  return { ok: true, candidates: rows.length, sent: sentOk.length, dry_run: dryRun, results };
}

async function runFollowups(dryRun: boolean): Promise<Record<string, unknown>> {
  if (!dryRun && !insideFollowupHours()) {
    return { ok: true, sent: 0, reason: "outside business hours", boston: bostonNow() };
  }
  const rows = await sql<FollowupRow[]>`
    select id, messenger_id, first_name, stage, followup_count,
           service, zip, town, phone, preferred_day, preferred_window, address, declined_slots,
           round(extract(epoch from (now() - last_inbound_at)) / 3600, 1)::float as silence_hours
    from fadc.leads
    -- THE READY_TO_CONFIRM HOLE (found 21 oct). The nudge cron only ever covered 'new' and
    -- 'qualifying', so the leads CLOSEST to booking — the ones who picked a service, gave a
    -- ZIP and had a slot offered, and only had to say yes — got nothing. 12 of them were
    -- sitting untouched when this was found, against 39 in 'qualifying' that were being
    -- nudged. They are the cheapest bookings on the board and they were the only stage with
    -- no follow-up at all. FOLLOWUP_INSTRUCTION already handles them: with every field
    -- filled, "missing" comes back empty and the prompt asks them to confirm the slot.
    where stage in ('new', 'qualifying', 'ready_to_confirm')
      and agent_paused = false
      and last_outbound_at is not null
      and last_inbound_at is not null
      and (
        last_outbound_at > last_inbound_at
        -- A trailing "ok", "thanks" or a thumbs up seconds after our reply used to disqualify
        -- the lead forever, because the rule read it as "they are waiting on us". It is not:
        -- it is the end of their turn. A real unanswered question shows up as a long gap, and
        -- those still stay out, because there we owe them an answer, not a nudge.
        or last_inbound_at - last_outbound_at < interval '5 minutes'
      )
      and (no_followup_until is null or no_followup_until <= now())
      and last_inbound_at > now() - interval '72 hours'
      and messenger_id ~ '^[0-9]{8,}$'
      and (
        (followup_count = 0 and last_inbound_at < now() - interval '3 hours')
        or (followup_count = 1 and last_inbound_at < now() - interval '20 hours')
        or (followup_count = 2 and last_inbound_at < now() - interval '48 hours')
      )
    order by last_inbound_at asc
    limit 20`;

  const results: Array<Record<string, unknown>> = [];
  for (const r of rows) {
    const t0 = Date.now();
    // Declared here (not inside the try) so the catch block below can still reach the row id
    // if sendMessengerReply() throws after the insert — see the undeliverable-lead handling.
    let fuMsgRow: { id: string } | undefined;
    try {
      const history = await sql`
        select role, text from fadc.messages where lead_id = ${r.id}
        order by created_at desc limit 30`;
      history.reverse();
      const hist = history as { role: string; text: string }[];
      const fuEs = isSpanish(hist.filter((m) => m.role === "customer").map((m) => m.text).join("\n"));
      const fuSlotObj = r.preferred_day ? null : await nextSlot(fuEs, r.declined_slots ?? [], r.preferred_window ?? null);
      const fuSlot = fuSlotObj?.phrase ?? null;
      const convo = [...hist,
        { role: "customer", text: FOLLOWUP_INSTRUCTION(r, Math.round(r.silence_hours), fuSlot) }];
      const out = await callClaude(convo);
      let reply = scrubFillers(stripDashes(out.reply));
      let rewritten = false;

      // Guard: if the draft used a banned filler, call it out and ask for one rewrite.
      if (reply && BANNED_FOLLOWUP_RE.test(reply)) {
        const offender = reply.match(BANNED_FOLLOWUP_RE)?.[0] ?? "a filler phrase";
        const retry = await callClaude([...convo, {
          role: "customer",
          text: `(Your draft was: "${reply}". It contains "${offender}", which is banned, that phrasing is the giveaway that a message is automated. Rewrite the same follow-up without any form of "checking in", "following up", "circling back", "touching base" or "hope you're well". Keep the first name and the concrete detail. Same JSON format.)`,
        }]);
        if (retry.reply && !BANNED_FOLLOWUP_RE.test(retry.reply)) {
          reply = scrubFillers(stripDashes(retry.reply));
          rewritten = true;
        } else {
          // Still bad. Skip this lead WITHOUT burning its nudge, so the next cron run
          // (30 min later) gets a fresh attempt instead of sending something robotic.
          results.push({ lead: r.first_name ?? r.messenger_id, skipped: "draft kept banned phrasing, will retry next run", draft: reply });
          continue;
        }
      }

      // v74: a nudge may never repeat what we already sent (9 oct: the same "Still interested in
      // that air duct cleaning? I can hold Monday 9 to 11" went out at 9:00 and again at 12:00).
      const fuSent = hist.filter((m) => m.role === "agent").map((m) => m.text).slice(-3);
      if (reply && repeatsSent(reply, fuSent)) {
        const retry2 = await callClaude([...convo, {
          role: "customer",
          text: `(Your draft repeats a message already sent: "${reply}". Write a DIFFERENT, warmer second nudge with a new angle: offer a choice like "mornings or afternoons?" or "this week or next?", or mention no deposit. Do not offer the same day again. Same JSON format.)`,
        }]);
        const r2 = retry2.reply ? scrubFillers(stripDashes(retry2.reply)) : "";
        if (r2 && !repeatsSent(r2, fuSent) && !BANNED_FOLLOWUP_RE.test(r2)) {
          reply = r2;
          rewritten = true;
        } else {
          results.push({ lead: r.first_name ?? r.messenger_id, skipped: "draft repeated an earlier message, will retry next run" });
          continue;
        }
      }

      // The same net as the live chat, applied to the nudge. A follow-up that asks "what day
      // works for you?" is the message that already failed once with this exact person, so
      // it never goes out a second time: it becomes an offer they can answer with one word.
      // A draft that already names a concrete day is left exactly as it is.
      if (fuSlot && reply && !fuSent.some((x) => x.includes(fuSlot))) {
        if (OPEN_DAY_RE.test(reply)) {
          reply = reply.replace(OPEN_DAY_RE, cap(holdLine(fuSlot, fuEs)));
        } else if (!DAY_WORD_RE.test(reply)) {
          reply = reply.replace(/\s*$/, "") + " " + cap(holdLine(fuSlot, fuEs));
        }
      }

      if (dryRun) {
        results.push({ lead: r.first_name ?? r.messenger_id, nudge: r.followup_count + 1, silence_hours: r.silence_hours, rewritten, would_send: reply });
        continue;
      }
      if (!reply) {
        await sql`update fadc.leads set followup_count = followup_count + 1 where id = ${r.id}`;
        results.push({ lead: r.first_name ?? r.messenger_id, skipped: "agent returned empty" });
        continue;
      }
      // Row first, send second, so the echo of this very message always finds its own record.
      [fuMsgRow] = await sql`insert into fadc.messages (lead_id, role, text, sent_by) values (${r.id}, 'agent', ${reply}, 'followup') returning id`;
      await sendMessengerReply(r.messenger_id, reply);
      // A nudge that offers a slot records it, so a one-word yes books THAT slot. On 6 oct the
      // 4pm nudge to 08816 offered Wednesday while the lead still held Tuesday from an older offer.
      const offered = !!(fuSlotObj && reply && reply.includes(fuSlotObj.phrase));
      await sql`update fadc.leads set followup_count = followup_count + 1, last_outbound_at = now(),
        offered_day = case when ${offered} then ${fuSlotObj?.day ?? null} else offered_day end,
        offered_window = case when ${offered} then ${fuSlotObj?.window ?? null} else offered_window end
        where id = ${r.id}`;
      await sql`insert into fadc.agent_runs (lead_id, inbound_text, model, raw_output, reply_sent, latency_ms)
        values (${r.id}, ${"[followup " + (r.followup_count + 1) + "]"}, ${MODEL}, ${sql.json(out as unknown as Record<string, unknown>)}, ${reply}, ${Date.now() - t0})`;
      results.push({ lead: r.first_name ?? r.messenger_id, nudge: r.followup_count + 1, sent: reply });
      // v74: Messenger closes the door 24 h after their last message. After the final nudge the
      // only channel left is the phone, so a lead with a number gets handed to a person to call.
      if (r.followup_count + 1 >= 3 && r.phone) {
        await notifyTeam(
          `\u{1F4DE} CALL ${r.first_name ?? "this lead"} at ${r.phone}. Third and final nudge sent, 48h since last reply. ` +
          `${[r.service, r.town, r.zip].filter(Boolean).join(", ")}`,
          "window_closing",
        );
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      try {
        await sql`insert into fadc.agent_runs (lead_id, inbound_text, model, error, latency_ms)
          values (${r.id}, '[followup]', ${MODEL}, ${msg}, ${Date.now() - t0})`;
      } catch (_) { /* ignore */ }
      // Meta refuses some recipients permanently. It says so in two different ways and we have
      // now been bitten by both: "(#100) cannot send messages to this id", and "(#551) This
      // person isn't available right now" — the one a blocked or restricted Page gets. Only the
      // first was matched here, so a 551 lead matched the followup query again every run
      // forever: one unnamed lead burned 21 retries in 24 hours on 19 oct, and those errors
      // drowned the real ones in the error rate. The message row inserted above never went out,
      // so mark it failed rather than leaving a ghost "sent" record, then stop and hand it to a
      // person once. (Jennifer Judge gave 551 with a different subcode the same day — the
      // subcodes vary, the 551 does not, so match on the code and the sentence, not the subcode.)
      // THE 24-HOUR WINDOW (found 20 oct). "(#10) This message is sent outside of allowed
      // window" was missing from the test below, so those leads matched the query again every
      // single run: lead 6c116b6a took 19 retries in 38 hours, 18 of them today, one every ten
      // minutes, each one paying for a model call before Meta refused it. Nothing reached the
      // customer. Two other code paths already knew about this error; this one did not.
      //
      // #10 is NOT permanent the way #100 and #551 are — the window reopens the moment the
      // customer writes again. So it stops the nudges and hands the lead to a person, but
      // leaves the agent live so a reply still gets answered.
      if (/outside of allowed window|\(#10\)|24.?hour/i.test(msg)) {
        try {
          await sql`update fadc.messages set sent_by = 'followup_failed', text = '[NOT SENT] ' || text where id = ${fuMsgRow?.id ?? null}`;
        } catch (_) { /* ignore */ }
        const [lead10] = await sql`select needs_human, no_followup_until from fadc.leads where id = ${r.id}`;
        await sql`update fadc.leads
                    set needs_human = true,
                        no_followup_until = now() + interval '30 days',
                        updated_at = now()
                  where id = ${r.id}`;
        if (!lead10?.needs_human && !lead10?.no_followup_until) {
          await notifyTeam(
            `\u{23F0} The Messenger window closed on ${r.first_name ?? "this lead"} — 24h since they last wrote, ` +
            `so Meta refuses any new message. Nudges stopped; the bot still answers if they write back.\n` +
            (r.phone ? `\u{1F4DE} The phone is the only way in: ${r.phone}.` : "No phone on file — no other way to reach them.") + "\n" +
            `${[r.service, r.town, r.zip].filter(Boolean).join(", ")}`,
            "window_closed",
          );
        }
      } else if (/cannot send messages to this id|isn.?t available right now|\(#551\)/i.test(msg)) {
        try {
          await sql`update fadc.messages set sent_by = 'followup_failed', text = '[NOT SENT] ' || text where id = ${fuMsgRow?.id ?? null}`;
        } catch (_) { /* ignore */ }
        const [lead] = await sql`select needs_human from fadc.leads where id = ${r.id}`;
        await sql`update fadc.leads set needs_human = true, agent_paused = true, updated_at = now() where id = ${r.id}`;
        if (!lead?.needs_human) {
          await notifyTeam(
            `\u{1F6AB} Can't reach ${r.first_name ?? "this lead"} on Messenger — Meta is permanently refusing delivery ` +
            `(blocked, unsubscribed, or a stale conversation). Bot has stopped retrying.\n` +
            (r.phone ? `\u{1F4DE} Call them at ${r.phone}.` : "No phone on file — no other way to reach them.") + "\n" +
            `${[r.service, r.town, r.zip].filter(Boolean).join(", ")}`,
            "undeliverable",
          );
        }
      }
      results.push({ lead: r.first_name ?? r.messenger_id, error: msg.slice(0, 200) });
    }
  }
  const sent = results.filter((x) => x.sent).length;
  if (sent > 0) await notifyTeam(`🔁 Sent ${sent} follow-up${sent === 1 ? "" : "s"} to quiet leads.`, "followup");
  return { ok: true, candidates: rows.length, sent, dry_run: dryRun, results };
}

async function cronSecretOk(header: string | null): Promise<boolean> {
  if (!header) return false;
  const [row] = await sql`select value from fadc.settings where key = 'cron_secret'`;
  return !!row && row.value === header;
}

// ---------------------------------------------------------------------------------------
// CRM. Michel, Afek and Ilay run 100+ leads from their phones. Reads and edits go straight
// to the database through public.crm_* functions that check the login. Two things cannot live
// in the database because they need secrets this function already holds: re-sending a changed
// ticket to Telegram, and writing to a customer on Messenger as a person.
const CORS: Record<string, string> = {
  "access-control-allow-origin": "*",
  "access-control-allow-headers": "authorization, content-type, apikey, x-client-info",
  "access-control-allow-methods": "POST, OPTIONS",
};
const SB_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SB_ANON = Deno.env.get("SUPABASE_ANON_KEY") ?? "";

async function crmUser(auth: string | null): Promise<{ email: string; name: string } | null> {
  if (!auth || !/^Bearer /i.test(auth) || !SB_URL || !SB_ANON) return null;
  try {
    const r = await fetch(`${SB_URL}/auth/v1/user`, { headers: { apikey: SB_ANON, authorization: auth } });
    if (!r.ok) return null;
    const u = await r.json();
    const email = String(u?.email ?? "").toLowerCase();
    if (!email) return null;
    const [row] = await sql`select email, name from fadc.crm_users where lower(email) = ${email}`;
    return row ? { email: String(row.email), name: String(row.name) } : null;
  } catch (_) { return null; }
}

function isoToMd(d: unknown): string | null {
  if (!d) return null;
  const s = d instanceof Date ? d.toISOString().slice(0, 10) : String(d).slice(0, 10);
  const m = s.match(/^\d{4}-(\d{2})-(\d{2})$/);
  return m ? `${Number(m[1])}/${Number(m[2])}` : null;
}

async function handleCrm(auth: string | null, b: Record<string, any>): Promise<{ status: number; body: unknown }> {
  const me = await crmUser(auth);
  if (!me) return { status: 401, body: { error: "not authorized" } };

  if (b.action === "ticket") {
    const [a] = await sql`
      select a.*, l.first_name, l.last_name, l.phone, l.address as lead_address, l.zip as lead_zip, l.service as lead_service, l.call_after, l.job_scale
      from fadc.appointments a join fadc.leads l on l.id = a.lead_id where a.id = ${String(b.appointment_id ?? "")}::uuid`;
    if (!a) return { status: 404, body: { error: "no ticket" } };
    setCtxLead(a.lead_id);
    const name = [a.first_name, a.last_name].filter(Boolean).join(" ") || null;
    const oldContext = String(a.notes ?? "").split("\n\n").slice(2).join("\n\n").trim() || null;
    const ticket = buildTicket({
      name, jobType: null, service: a.service ?? a.lead_service, day: isoToMd(a.slot_date),
      window: a.exact_window ?? a.slot_window, address: a.address ?? a.lead_address, phone: a.phone, zip: a.lead_zip ?? null,
      context: oldContext, callAfter: null, scale: a.job_scale ?? null,
    });
    await sql`update fadc.appointments set notes = ${ticket}, updated_at = now() where id = ${a.id}`;
    const status = String(a.status);
    const outcome = String(a.outcome_reason ?? "");
    const head = /no_show/.test(outcome) ? "⚠️ NO-SHOW"
      : status === "cancelled" ? "❌ TICKET CANCELLED"
      : status === "completed" ? "✅ JOB DONE"
      : "✏️ TICKET CHANGED";
    const extra = [a.tech ? `Tech: ${a.tech}` : "Tech: NOT ASSIGNED", `Status: ${status}`, b.note ? String(b.note) : ""].filter(Boolean).join("\n");
    await notifyTeam(`${head} (by ${me.name})\n\n${ticket}\n\n${extra}`, "ticket_update");
    return { status: 200, body: { ok: true, ticket } };
  }

  if (b.action === "send") {
    const text = String(b.text ?? "").trim();
    if (!text || text.length > 1900) return { status: 400, body: { error: "empty or too long" } };
    const [lead] = await sql`select id, messenger_id from fadc.leads where id = ${String(b.lead_id ?? "")}::uuid`;
    if (!lead?.messenger_id) return { status: 404, body: { error: "no lead" } };
    setCtxLead(lead.id);
    if (!META_PAGE_TOKEN) return { status: 500, body: { error: "page token missing" } };
    // Paused BEFORE sending: a person is now on this conversation and the bot must not answer
    // over them. The row is written first so the echo is recognised as ours.
    await sql`update fadc.leads set agent_paused = true, assigned_to = coalesce(assigned_to, ${me.name}), taken_at = coalesce(taken_at, now()) where id = ${lead.id}`;
    const [row] = await sql`insert into fadc.messages (lead_id, role, text, sent_by) values (${lead.id}, 'agent', ${text}, 'crm') returning id`;
    const res = await fetch(`https://graph.facebook.com/${GRAPH_VERSION}/me/messages?access_token=${META_PAGE_TOKEN}`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ recipient: { id: lead.messenger_id }, messaging_type: "RESPONSE", message: { text } }),
    });
    const out = await res.json().catch(() => ({}));
    if (!res.ok) {
      await sql`update fadc.messages set sent_by = 'crm_failed', text = '[NOT SENT] ' || text where id = ${row.id}`;
      const err = String(out?.error?.message ?? res.status);
      const outside = /outside of allowed window|24 hour/i.test(err);
      return { status: 400, body: { error: outside ? "outside_window" : err } };
    }
    await sql`update fadc.messages set meta_mid = ${out?.message_id ?? null} where id = ${row.id}`;
    await sql`update fadc.leads set last_outbound_at = now(), updated_at = now() where id = ${lead.id}`;
    await sql`insert into fadc.crm_log (actor, action, lead_id, detail) values (${me.name}, 'send', ${lead.id}, ${JSON.stringify({ text })}::jsonb)`;
    return { status: 200, body: { ok: true } };
  }

  return { status: 400, body: { error: "unknown action" } };
}

// ---------------------------------------------------------------------------------------
// v75 — Appointment closeout.
// v77 — Phone earlier. Moved phone from position 5 to position 3 in ASK_LINES (after service + zip, before day/window).
//        Aligns with system prompt order. 20 leads in last 7 days had no phone despite 5-10+ messages — they drop off before reaching phone ask.
//        Also aligned buildStateNote order to match.
//
// Every booked appointment must end its day with a recorded outcome. Until now all of them
// stayed "pending" forever, so nobody could say how many jobs were lost because a tech never
// showed up — which is exactly the number Michel needs next to Afek and Ilay's Wednesday balance.
//
// Flow: at 5pm ET the team's Telegram group gets ONE message per open appointment with
// buttons. A tap records the outcome and edits the message so everyone sees it is closed.
// "Done" asks for the amount charged as a reply. Open jobs get one reminder around 8pm.
// Wednesday morning a one-message summary of the week goes out.
// ---------------------------------------------------------------------------------------

type CloseoutOutcome = "done" | "tech_no_show" | "customer_no_show" | "customer_cancelled" | "rescheduled";

const CLOSEOUT_LABELS: Record<CloseoutOutcome, string> = {
  done: "✅ Done",
  tech_no_show: "\u{1F6AB} Tech didn't show",
  customer_no_show: "\u{1F3E0} Customer not home",
  customer_cancelled: "❌ Customer cancelled",
  rescheduled: "\u{1F501} Rescheduled",
};

// fadc.appointments has a check constraint allowing only:
// pending / confirmed / completed / cancelled / rescheduled — there is no "done" or
// "no_show" value, so those used to throw a constraint violation and silently fail
// to close the appointment. outcome_reason carries the finer distinction instead.
const CLOSEOUT_STATUS: Record<CloseoutOutcome, string> = {
  done: "completed",
  tech_no_show: "cancelled",
  customer_no_show: "cancelled",
  customer_cancelled: "cancelled",
  rescheduled: "rescheduled",
};

const CLOSEOUT_RE = /^co:([0-9a-f-]{36}):(done|tech_no_show|customer_no_show|customer_cancelled|rescheduled)$/;

async function tgApi(method: string, body: Record<string, unknown>): Promise<any> {
  if (!TG_TOKEN) return { ok: false, description: "TELEGRAM_BOT_TOKEN not set" };
  try {
    const r = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/${method}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    return await r.json();
  } catch (e) {
    return { ok: false, description: e instanceof Error ? e.message : String(e) };
  }
}

// Send a WhatsApp message via Twilio to Afek.
// Silently skips if creds are not set — Telegram is the primary channel.
async function sendWhatsApp(body: string): Promise<{ ok: boolean; sid?: string; error?: string }> {
  if (!TWILIO_SID || !TWILIO_TOKEN) return { ok: false, error: "twilio creds not set" };
  try {
    const params = new URLSearchParams({ From: TWILIO_FROM, To: AFEK_WA, Body: body });
    const res = await fetch(
      `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_SID}/Messages.json`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization: `Basic ${btoa(`${TWILIO_SID}:${TWILIO_TOKEN}`)}`,
        },
        body: params.toString(),
      },
    );
    const json = await res.json();
    if (res.ok) return { ok: true, sid: json.sid };
    return { ok: false, error: json.message ?? `HTTP ${res.status}` };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  }
}

// Send a plain SMS via Twilio from Michel's own 617 recruiting line.
// Silently skips if creds are not set — same guard as sendWhatsApp above.
async function sendSMS(to: string, body: string): Promise<{ ok: boolean; sid?: string; error?: string }> {
  if (!TWILIO_SID || !TWILIO_TOKEN) return { ok: false, error: "twilio creds not set" };
  try {
    const params = new URLSearchParams({ From: TWILIO_SMS_FROM, To: to, Body: body });
    const res = await fetch(
      `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_SID}/Messages.json`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization: `Basic ${btoa(`${TWILIO_SID}:${TWILIO_TOKEN}`)}`,
        },
        body: params.toString(),
      },
    );
    const json = await res.json();
    if (res.ok) return { ok: true, sid: json.sid };
    return { ok: false, error: json.message ?? `HTTP ${res.status}` };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  }
}

async function tgWebhookSecret(): Promise<string> {
  const [row] = await sql`select value from fadc.settings where key = 'tg_webhook_secret'`;
  return row?.value ? String(row.value) : "";
}

function closeoutText(a: Record<string, any>, head: string): string {
  const name = [a.first_name, a.last_name].filter(Boolean).join(" ") || "(no name)";
  const win = a.exact_window ?? a.slot_window ?? "";
  const lines = [
    head,
    "",
    name,
    `${isoToMd(a.slot_date) ?? ""} ${win} — ${a.service ?? a.lead_service ?? "service?"}`.trim(),
    a.address ?? a.lead_address ?? "(no address)",
    a.phone ?? "",
    a.tech ? `Tech: ${a.tech}` : "",
  ];
  return lines.filter((l, i) => i < 2 || l !== "").join("\n");
}

// First ask at 5pm ET, one reminder around 8pm ET. Idempotent: each appointment is asked
// once (closeout_msg_id) and reminded once (closeout_reminded_at); a tap on either closes it.
async function runCloseout(remind: boolean, dryRun: boolean): Promise<Record<string, unknown>> {
  const today = bostonToday().toISOString().slice(0, 10);
  // v80: use <= today so past appointments that were never closed out are also caught.
  // Previously slot_date = today meant citas from Oct 6-8 were silently ignored every day.
  // v85: that fix still asked a stale appointment only ONCE + reminded ONCE, ever — Ramiro,
  // Duan, Mike, Oscar, Chris and Digna (all slot_date 6-8 oct) got both messages back on 14-15
  // oct and then nothing again, because closeout_reminded_at was already set. A past-dated
  // appointment nobody tapped a button on is not resolved by going quiet about it, so a
  // PAST-dated one (slot_date < today) that was already reminded 20+ hours ago is asked again
  // on this sweep, every day, until someone taps an outcome. Today's own appointment still
  // gets only the one evening reminder.
  const open = remind
    ? await sql`
        select a.*, l.first_name, l.last_name, l.phone, l.address as lead_address, l.service as lead_service
        from fadc.appointments a join fadc.leads l on l.id = a.lead_id
        where a.slot_date <= ${today}::date
          and a.status in ('pending','confirmed','rescheduled')
          and a.closed_at is null
          and a.closeout_msg_id is not null
          and (
            a.closeout_reminded_at is null
            or (a.slot_date < ${today}::date and a.closeout_reminded_at < now() - interval '20 hours')
          )
        order by a.slot_date, a.slot_window, a.created_at`
    : await sql`
        select a.*, l.first_name, l.last_name, l.phone, l.address as lead_address, l.service as lead_service
        from fadc.appointments a join fadc.leads l on l.id = a.lead_id
        where a.slot_date <= ${today}::date
          and a.status in ('pending','confirmed','rescheduled')
          and a.closed_at is null
          and a.closeout_msg_id is null
        order by a.slot_date, a.slot_window, a.created_at`;

  if (open.length === 0) return { ok: true, today, remind, open: 0, sent: 0 };
  if (!TG_TOKEN || !TG_CHAT) return { ok: false, today, remind, open: open.length, error: "telegram not configured" };

  let sent = 0;
  const errors: string[] = [];
  for (const a of open) {
    const head = remind
      ? "⏰ STILL OPEN — what happened with this job?"
      : "\u{1F4CB} END OF DAY — what happened with this job?";
    const text = closeoutText(a, head);
    const reply_markup = {
      inline_keyboard: [
        [{ text: CLOSEOUT_LABELS.done, callback_data: `co:${a.id}:done` }],
        [{ text: CLOSEOUT_LABELS.tech_no_show, callback_data: `co:${a.id}:tech_no_show` }],
        [
          { text: CLOSEOUT_LABELS.customer_no_show, callback_data: `co:${a.id}:customer_no_show` },
          { text: CLOSEOUT_LABELS.customer_cancelled, callback_data: `co:${a.id}:customer_cancelled` },
        ],
        [{ text: CLOSEOUT_LABELS.rescheduled, callback_data: `co:${a.id}:rescheduled` }],
      ],
    };
    if (dryRun) { sent++; continue; }
    const r = await tgApi("sendMessage", { chat_id: TG_CHAT, text, reply_markup });
    if (r?.ok) {
      sent++;
      if (remind) {
        await sql`update fadc.appointments set closeout_reminded_at = now() where id = ${a.id}`;
      } else {
        await sql`update fadc.appointments set closeout_msg_id = ${r.result.message_id} where id = ${a.id}`;
      }
      // Mirror to Afek on WhatsApp — just the job info, no buttons (WhatsApp doesn't support them).
      const customerName = [a.first_name, a.last_name].filter(Boolean).join(" ") || "(no name)";
      const win = a.exact_window ?? a.slot_window ?? "";
      const waText = remind
        ? `⏰ STILL OPEN — ${customerName}\n${isoToMd(a.slot_date) ?? ""} ${win}\n${a.address ?? a.lead_address ?? ""}\nWhat happened? Reply here.`
        : `📋 END OF DAY — ${customerName}\n${isoToMd(a.slot_date) ?? ""} ${win}\n${a.address ?? a.lead_address ?? ""}\nWhat happened with this job?`;
      await sendWhatsApp(waText);
    } else {
      errors.push(String(r?.description ?? "send failed"));
    }
  }
  return { ok: errors.length === 0, today, remind, open: open.length, sent, errors };
}

// A Telegram update: a button tap (callback_query) or a reply carrying the amount charged.
async function handleTelegramUpdate(u: Record<string, any>): Promise<Record<string, unknown>> {
  const cq = u.callback_query;
  if (cq) {
    const chatId = String(cq.message?.chat?.id ?? "");
    const ack = (text?: string) => tgApi("answerCallbackQuery", { callback_query_id: cq.id, ...(text ? { text } : {}) });

    if (TG_CHAT && chatId !== String(TG_CHAT)) { await ack(); return { ignored: "wrong chat" }; }
    const m = String(cq.data ?? "").match(CLOSEOUT_RE);
    if (!m) { await ack("Unknown button"); return { ignored: "bad callback data" }; }
    const apptId = m[1];
    const outcome = m[2] as CloseoutOutcome;
    const who = [cq.from?.first_name, cq.from?.last_name].filter(Boolean).join(" ")
      || (cq.from?.username ? `@${cq.from.username}` : "someone");

    const [a] = await sql`
      select a.*, l.first_name, l.last_name, l.phone, l.address as lead_address, l.service as lead_service
      from fadc.appointments a join fadc.leads l on l.id = a.lead_id
      where a.id = ${apptId}::uuid`;
    if (!a) { await ack("Appointment not found"); return { ignored: "no appointment" }; }
    setCtxLead(a.lead_id);

    if (a.closed_at) {
      await ack(`Already closed: ${a.outcome_reason ?? a.status} by ${a.closed_by ?? "someone"}`);
      return { ignored: "already closed", appointment_id: apptId };
    }

    if (outcome === "rescheduled") {
      // Stays open: the bot or the CRM will set the new date and it gets asked again that day.
      await sql`
        update fadc.appointments
        set status = 'rescheduled', outcome_reason = 'rescheduled', closed_by = ${who},
            closeout_msg_id = null, closeout_reminded_at = null, updated_at = now()
        where id = ${apptId}::uuid`;
    } else {
      await sql`
        update fadc.appointments
        set status = ${CLOSEOUT_STATUS[outcome]}, outcome_reason = ${outcome},
            closed_at = now(), closed_by = ${who}, updated_at = now()
        where id = ${apptId}::uuid`;
    }
    await sql`
      insert into fadc.crm_log (actor, action, lead_id, appointment_id, detail)
      values (${who}, 'closeout', ${a.lead_id}, ${apptId}::uuid, ${JSON.stringify({ outcome })}::jsonb)`;

    await ack(`Saved: ${CLOSEOUT_LABELS[outcome]}`);
    await tgApi("editMessageText", {
      chat_id: chatId,
      message_id: cq.message.message_id,
      text: closeoutText(a, `${CLOSEOUT_LABELS[outcome]} — recorded by ${who}`),
    });

    if (outcome === "done") {
      const p = await tgApi("sendMessage", {
        chat_id: chatId,
        text: `\u{1F4B5} How much was charged at ${a.address ?? a.lead_address ?? "this job"}?\nReply to THIS message with just the number (e.g. 249).`,
        reply_markup: { force_reply: true, selective: true },
      });
      if (p?.ok) await sql`update fadc.appointments set amount_prompt_msg_id = ${p.result.message_id} where id = ${apptId}::uuid`;
      // Also notify Afek directly on WhatsApp so he doesn't have to check Telegram.
      const customerName = [a.first_name, a.last_name].filter(Boolean).join(" ") || "the customer";
      await sendWhatsApp(
        `✅ Job done — ${customerName} (${a.address ?? a.lead_address ?? ""})\nHow much did you charge? Reply here with the amount (e.g. 249).`,
      );
    }
    if (outcome === "tech_no_show") {
      await notifyTeam(
        `\u{1F6A8} TECH NO-SHOW recorded by ${who}\n${a.address ?? a.lead_address ?? ""}\n${a.phone ?? ""}\nThis customer needs a call and a new slot.`,
        "tech_no_show",
      );
    }
    return { ok: true, outcome, appointment_id: apptId, by: who };
  }

  const msg = u.message;
  if (msg?.reply_to_message?.message_id && typeof msg.text === "string") {
    const chatId = String(msg.chat?.id ?? "");
    if (TG_CHAT && chatId !== String(TG_CHAT)) return { ignored: "wrong chat" };
    const [a] = await sql`
      select id, lead_id, address from fadc.appointments
      where amount_prompt_msg_id = ${Number(msg.reply_to_message.message_id)}`;
    if (!a) return { ignored: "not a reply to an amount prompt" };
    setCtxLead(a.lead_id);
    const num = Number(String(msg.text).replace(/[^0-9.]/g, ""));
    if (!isFinite(num) || num <= 0 || num > 20000) {
      await tgApi("sendMessage", {
        chat_id: chatId, reply_to_message_id: msg.message_id,
        text: "Just the number please, like 249 — reply to the amount question again.",
      });
      return { ignored: "bad amount" };
    }
    const who = [msg.from?.first_name, msg.from?.last_name].filter(Boolean).join(" ") || "someone";
    await sql`update fadc.appointments set ticket_amount = ${num}, amount_prompt_msg_id = null, updated_at = now() where id = ${a.id}`;
    await sql`
      insert into fadc.crm_log (actor, action, lead_id, appointment_id, detail)
      values (${who}, 'ticket_amount', ${a.lead_id}, ${a.id}, ${JSON.stringify({ amount: num })}::jsonb)`;
    await tgApi("sendMessage", {
      chat_id: chatId, reply_to_message_id: msg.message_id,
      text: `\u{1F4B5} $${num.toFixed(2)} saved for ${a.address ?? "the job"}. Thanks ${who}.`,
    });
    return { ok: true, amount: num, appointment_id: a.id };
  }

  return { ignored: "not handled" };
}

// Wednesday morning: last 7 days in one message, built to sit next to Afek and Ilay's balance.
async function runWeeklySummary(dryRun: boolean): Promise<Record<string, unknown>> {
  const rows = await sql`
    select a.slot_date, a.slot_window, a.status, a.outcome_reason, a.ticket_amount, a.tech,
           a.address, l.address as lead_address, l.town, l.zip
    from fadc.appointments a join fadc.leads l on l.id = a.lead_id
    where a.slot_date >= (current_date - interval '7 days') and a.slot_date < current_date
      and coalesce(l.phone, '') not like '%555-0%'
    order by a.slot_date, a.slot_window`;

  const by: Record<string, number> = {};
  let revenue = 0;
  const techNoShows: string[] = [];
  for (const r of rows) {
    const k = r.outcome_reason
      ?? ((r.status === "pending" || r.status === "confirmed") ? "unrecorded" : String(r.status));
    by[k] = (by[k] ?? 0) + 1;
    if (r.ticket_amount) revenue += Number(r.ticket_amount);
    if (k === "tech_no_show") {
      techNoShows.push(`${isoToMd(r.slot_date) ?? ""} ${r.slot_window ?? ""} — ${r.address ?? r.lead_address ?? r.town ?? r.zip ?? ""}`.trim());
    }
  }
  const line = (k: string, label: string) => by[k] ? `${label}: ${by[k]}` : null;
  const parts: Array<string | null> = [
    `\u{1F4CA} WEEK IN REVIEW — ${rows.length} appointment${rows.length === 1 ? "" : "s"} (last 7 days)`,
    "",
    line("done", "✅ Completed"),
    line("tech_no_show", "\u{1F6AB} Tech didn't show"),
    line("customer_no_show", "\u{1F3E0} Customer not home"),
    line("customer_cancelled", "❌ Customer cancelled"),
    line("rescheduled", "\u{1F501} Rescheduled"),
    by.unrecorded ? `⚠️ No outcome recorded: ${by.unrecorded}` : null,
    "",
    revenue ? `\u{1F4B5} Revenue recorded: $${revenue.toFixed(2)}` : "\u{1F4B5} No ticket amounts recorded yet",
    techNoShows.length ? `\nTech no-shows:\n${techNoShows.map((s) => "• " + s).join("\n")}` : null,
    "",
    "Lay this next to the Wednesday balance: everything marked Completed here should appear there.",
  ];
  const text = parts.filter((x): x is string => x !== null).join("\n");
  if (!dryRun) await notifyTeam(text, "weekly_summary");
  return { ok: true, appointments: rows.length, by, revenue, text };
}

Deno.serve((req: Request) => leadCtx.run({ leadId: null }, () => handle(req)));

async function handle(req: Request): Promise<Response> {
  const url = new URL(req.url);

  // --- CRM (the phone app). Browser calls, so CORS, and the caller proves who they are with
  // their Supabase login, checked against fadc.crm_users. See handleCrm. ---
  if (url.searchParams.get("crm")) {
    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
    if (req.method !== "POST") return new Response("POST only", { status: 405, headers: CORS });
    let b: Record<string, any> = {};
    try { b = await req.json(); } catch { /* empty body */ }
    const r = await handleCrm(req.headers.get("authorization"), b);
    return new Response(JSON.stringify(r.body), { status: r.status, headers: { ...CORS, "content-type": "application/json" } });
  }

  // --- Diagnostic (GET ?diag=<webhook secret>): report token/page/subscribed-apps. Safe: no secrets returned. ---
  if (req.method === "GET" && url.searchParams.get("diag")) {
    if (!WEBHOOK_SECRET || url.searchParams.get("diag") !== WEBHOOK_SECRET) {
      return json(403, { error: "forbidden" });
    }
    const out: Record<string, unknown> = {
      token_present: !!META_PAGE_TOKEN,
      token_prefix: META_PAGE_TOKEN ? META_PAGE_TOKEN.slice(0, 8) : null,
      verify_token: META_VERIFY_TOKEN,
      app_secret_set: !!META_APP_SECRET,
      graph_version: GRAPH_VERSION,
      live_mode: BOT_LIVE_MODE,
      telegram_configured: !!(TG_TOKEN && TG_CHAT),
    };
    if (META_PAGE_TOKEN) {
      try {
        const meRes = await fetch(`https://graph.facebook.com/${GRAPH_VERSION}/me?fields=id,name&access_token=${META_PAGE_TOKEN}`);
        out.me = await meRes.json();
      } catch (e) { out.me_error = String(e); }
      try {
        const subRes = await fetch(`https://graph.facebook.com/${GRAPH_VERSION}/me/subscribed_apps?access_token=${META_PAGE_TOKEN}`);
        out.subscribed_apps = await subRes.json();
      } catch (e) { out.subscribed_apps_error = String(e); }
    }
    return json(200, out);
  }

  // --- Meta webhook verification (GET) ---
  if (req.method === "GET") {
    const mode = url.searchParams.get("hub.mode");
    const token = url.searchParams.get("hub.verify_token");
    const challenge = url.searchParams.get("hub.challenge");
    if (mode === "subscribe" && token === META_VERIFY_TOKEN) {
      return new Response(challenge ?? "", { status: 200 });
    }
    return new Response("forbidden", { status: 403 });
  }

  if (req.method !== "POST") return json(405, { error: "POST only" });

  const rawBody = await req.text();
  let body: Record<string, unknown>;
  try { body = JSON.parse(rawBody); } catch { return json(400, { error: "bad json" }); }

  // --- Telegram webhook (v75): button taps and amount replies. Telegram proves itself with the
  // secret_token registered in setWebhook, echoed in this header on every update. Always 200
  // after auth so Telegram never retries the same tap forever. ---
  if (typeof body.update_id === "number") {
    const want = await tgWebhookSecret();
    const got = req.headers.get("x-telegram-bot-api-secret-token") ?? "";
    if (!want || got !== want) return json(403, { error: "forbidden" });
    try { return json(200, await handleTelegramUpdate(body as Record<string, any>)); }
    catch (e) { return json(200, { ok: false, error: e instanceof Error ? e.message : String(e) }); }
  }

  // --- Meta webhook events (POST with object === "page") ---
  if (body.object === "page") {
    const okSig = await verifyMetaSignature(rawBody, req.headers.get("x-hub-signature-256"));
    if (!okSig) {
      // A wrong or stale META_APP_SECRET makes the bot silently deaf: Meta keeps delivering,
      // we keep answering 403, and nobody finds out until a lead goes unanswered. Say it out
      // loud, throttled to once every 10 minutes so a flood of bad POSTs cannot spam Telegram.
      const now = Date.now();
      if (now - lastBadSigAlert > 600_000) {
        lastBadSigAlert = now;
        await notifyTeam(
          "\u{1F6D1} A Messenger event was REJECTED for a bad signature, so the bot is not answering anyone right now.\n" +
          "META_APP_SECRET in Supabase does not match the App Secret in the Meta App Dashboard. Fix it or delete the secret to go back to accepting events.",
          "bad_signature",
        );
      }
      return json(403, { error: "bad signature" });
    }

    // Process in the background and ack Meta immediately, so a cold start never makes Meta
    // time out (~20s) and retry minutes later. EdgeRuntime.waitUntil keeps the worker alive until done.
    const work = (async () => {
      const entries = (body.entry ?? []) as Array<Record<string, unknown>>;
      for (const entry of entries) {
        // Comments arrive on entry.changes with field "feed", not on entry.messaging.
        const changes = (entry.changes ?? []) as Array<Record<string, any>>;
        for (const ch of changes) {
          if (ch?.field !== "feed") continue;
          try { await handleComment(ch.value ?? {}); } catch (_) { /* best-effort, never block messages */ }
        }
        const events = (entry.messaging ?? []) as Array<Record<string, any>>;
        for (const ev of events) {
          const msg = ev.message;
          // v73: which ad brought this person. Click-to-Messenger sends the referral on the first
          // message (message.referral) or as its own event (ev.referral) when the thread exists.
          const ref = (msg?.referral ?? ev.referral ?? ev.postback?.referral) as Record<string, any> | undefined;
          const refAdId = ref?.ad_id ? String(ref.ad_id) : null;
          const refTitle = ref?.ads_context_data?.ad_title ? String(ref.ads_context_data.ad_title).slice(0, 200) : null;
          const refSender = String(ev.sender?.id ?? "").trim();
          if (refAdId && refSender && !msg) {
            try { await saveAdRef(refSender, refAdId, refTitle); } catch (_) { /* best-effort */ }
          }
          if (!msg) continue;

          // Echo event = someone sent a message FROM the page (Michel from inbox, or the bot via API).
          // handleEcho distinguishes human vs bot echoes and pauses the agent when Michel replies manually.
          if (msg.is_echo) {
            try { await handleEcho(ev); } catch (_) { /* best-effort */ }
            continue;
          }

          // Text, attachments, or both. Only delivery/read receipts and postbacks are skipped.
          const hasText0 = typeof msg.text === "string" && msg.text.trim().length > 0;
          const stickerId = pickStickerId(msg);
          if (!hasText0 && stickerId) {
            // A sticker is a reaction, not a file: never "got the photo", never an attachment alert.
            // An unrecognised sticker is NOT read as a yes: only the three like ids are.
            msg.text = LIKE_STICKERS.has(stickerId) ? "\u{1F44D}" : "[sent a sticker]";
            msg.attachments = [];
          }
          const att = describeAttachments(msg);
          const hasText = typeof msg.text === "string" && msg.text.trim().length > 0;
          if (!hasText && att.kinds.length === 0) continue;
          const senderId = String(ev.sender?.id ?? "").trim();
          if (!senderId) continue;
          const mid = msg.mid ? String(msg.mid) : null;
          try {
            const profile = await getMessengerProfile(senderId);
            // Attachments with no text never reach the model: handled in code.
            if (!hasText) {
              const r = await processAttachment({
                messengerId: senderId, att,
                firstName: profile.first_name, lastName: profile.last_name, metaMid: mid,
              });
              if (r.reply) await sendMessengerReply(senderId, r.reply);
              if (refAdId) { try { await saveAdRef(senderId, refAdId, refTitle); } catch (_) { /* best-effort */ } }
              continue;
            }
            const result = await processInbound({
              messengerId: senderId,
              // A photo sent alongside text is named in the text so the history stays truthful.
              text: att.kinds.length ? `${String(msg.text).trim()} ${att.label}` : String(msg.text).trim(),
              firstName: profile.first_name,
              lastName: profile.last_name,
              metaMid: mid,
              attachNote: att.kinds.length
                ? `(Internal note: they also attached ${att.kinds.join(", ")}. A person on the team can open it in the inbox, so NEVER say you cannot see images or files, and never ask them to describe it. Acknowledge it in three or four words, say the tech will look at it properly on site, and then ask the next thing you still need.)`
                : null,
            });
            if (result.reply && !result.skipped && !result.paused) {
              await sendMessengerReply(senderId, result.reply);
            }
            if (refAdId) { try { await saveAdRef(senderId, refAdId, refTitle); } catch (_) { /* best-effort */ } }
          } catch (_) {
            // team already notified inside processInbound; ack Meta so it doesn't hammer retries.
          }
        }
      }
    })();
    const er = (globalThis as any).EdgeRuntime;
    if (er && typeof er.waitUntil === "function") er.waitUntil(work);
    else await work;
    return new Response("EVENT_RECEIVED", { status: 200 });
  }

  // --- v75 closeout / weekly summary / one-time Telegram webhook setup (pg_cron or Michel) ---
  if (body.action === "closeout" || body.action === "weekly_summary" || body.action === "tg_setup") {
    const bySecret = !!WEBHOOK_SECRET && req.headers.get("x-fadc-secret") === WEBHOOK_SECRET;
    const byCron = await cronSecretOk(req.headers.get("x-fadc-cron"));
    if (!bySecret && !byCron) return json(401, { error: "unauthorized" });
    try {
      if (body.action === "closeout") {
        const remind = body.remind === true;
        const result = await runCloseout(remind, body.dry_run === true);
        await heartbeat(remind ? "fadc_closeout_remind" : "fadc_closeout_ask", true);
        return json(200, result);
      }
      if (body.action === "weekly_summary") {
        const result = await runWeeklySummary(body.dry_run === true);
        await heartbeat("fadc_weekly_summary", true);
        return json(200, result);
      }
      // tg_setup: point Telegram at this function. Idempotent, safe to re-run.
      const secret = await tgWebhookSecret();
      if (!secret) return json(500, { error: "tg_webhook_secret missing in fadc.settings" });
      if (!SB_URL) return json(500, { error: "SUPABASE_URL not set" });
      const setRes = await tgApi("setWebhook", {
        url: `${SB_URL}/functions/v1/fadc-agent`,
        secret_token: secret,
        allowed_updates: ["message", "callback_query"],
        drop_pending_updates: true,
      });
      const info = await tgApi("getWebhookInfo", {});
      return json(200, {
        setWebhook: setRes,
        webhook: info?.result
          ? { url: info.result.url, pending: info.result.pending_update_count, last_error: info.result.last_error_message ?? null }
          : info,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (body.action === "closeout") await heartbeat(body.remind === true ? "fadc_closeout_remind" : "fadc_closeout_ask", false, msg);
      if (body.action === "weekly_summary") await heartbeat("fadc_weekly_summary", false, msg);
      return json(500, { ok: false, error: msg });
    }
  }

  // --- Manual send (Michel, via x-fadc-cron — used when the CRM phone app isn't handy).
  // Sends as the Page, pauses the bot on that lead (so it doesn't talk over a human), and
  // logs the message as sent_by 'human' exactly like the CRM "send" action does. ---
  if (body.action === "manual_send") {
    const byCron = await cronSecretOk(req.headers.get("x-fadc-cron"));
    if (!byCron) return json(401, { error: "unauthorized" });
    const text = String(body.text ?? "").trim();
    const leadId = String(body.lead_id ?? "");
    if (!text || text.length > 1900 || !leadId) return json(400, { error: "lead_id and text required" });
    const [lead] = await sql`select id, messenger_id, first_name from fadc.leads where id = ${leadId}::uuid`;
    if (!lead?.messenger_id) return json(404, { error: "no lead or no messenger_id" });
    setCtxLead(lead.id);
    try {
      await sql`update fadc.leads set agent_paused = true, assigned_to = coalesce(assigned_to, 'Michel'), taken_at = coalesce(taken_at, now()) where id = ${lead.id}`;
      await sql`insert into fadc.messages (lead_id, role, text, sent_by) values (${lead.id}, 'agent', ${text}, 'human')`;
      await sendAppointmentUpdate(lead.messenger_id, text);
      await sql`update fadc.leads set last_outbound_at = now(), updated_at = now() where id = ${lead.id}`;
      return json(200, { ok: true, lead: lead.first_name });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return json(500, { ok: false, error: msg });
    }
  }

  // --- Follow-up runner (pg_cron via x-fadc-cron, or Michel via x-fadc-secret) ---
  if (body.action === "technician_outreach") {
    const bySecret = !!WEBHOOK_SECRET && req.headers.get("x-fadc-secret") === WEBHOOK_SECRET;
    const byCron = await cronSecretOk(req.headers.get("x-fadc-cron"));
    if (!bySecret && !byCron) return json(401, { error: "unauthorized" });
    try {
      const result = await runTechnicianOutreach(body.dry_run === true);
      await heartbeat("fadc_technician_outreach", true);
      return json(200, result);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      await heartbeat("fadc_technician_outreach", false, msg);
      return json(500, { ok: false, error: msg });
    }
  }

  if (body.action === "followups") {
    const bySecret = !!WEBHOOK_SECRET && req.headers.get("x-fadc-secret") === WEBHOOK_SECRET;
    const byCron = await cronSecretOk(req.headers.get("x-fadc-cron"));
    if (!bySecret && !byCron) return json(401, { error: "unauthorized" });
    try {
      let resume: Record<string, unknown> = {};
      try { resume = await runResume(body.dry_run === true); } catch (e) { resume = { error: e instanceof Error ? e.message : String(e) }; }
      let humanNudge: Record<string, unknown> = {};
      try { humanNudge = await runHumanNudge(body.dry_run === true); } catch (e) { humanNudge = { error: e instanceof Error ? e.message : String(e) }; }
      // v81: runs on every sweep, and unlike the others it is NOT gated on business hours —
      // a lead that qualified at 8pm should be on someone's phone that night, not at 9am.
      let leaks: Record<string, unknown> = {};
      try { leaks = await runLeakAlert(body.dry_run === true); } catch (e) { leaks = { error: e instanceof Error ? e.message : String(e) }; }
      // v85: also not gated on business hours — a customer dropped at 9pm needs the same
      // 15-minute net as one dropped at 9am, independent of stage, phone or ZIP.
      let unanswered: Record<string, unknown> = {};
      try { unanswered = await runUnansweredAlert(body.dry_run === true); } catch (e) { unanswered = { error: e instanceof Error ? e.message : String(e) }; }
      let ticketSweep: Record<string, unknown> = {};
      try { ticketSweep = await runTicketSweep(body.dry_run === true); } catch (e) { ticketSweep = { error: e instanceof Error ? e.message : String(e) }; }
      let ticketChanges: Record<string, unknown> = {};
      try { ticketChanges = await runTicketChangeSweep(body.dry_run === true); } catch (e) { ticketChanges = { error: e instanceof Error ? e.message : String(e) }; }
      let reviewFollowups: Record<string, unknown> = {};
      try { reviewFollowups = await runReviewFollowups(body.dry_run === true); } catch (e) { reviewFollowups = { error: e instanceof Error ? e.message : String(e) }; }
      let amountChase: Record<string, unknown> = {};
      try { amountChase = await runAmountChase(body.dry_run === true); } catch (e) { amountChase = { error: e instanceof Error ? e.message : String(e) }; }
      // v87: the booking itself, once it exists, is the thing nothing was watching.
      let staleAppts: Record<string, unknown> = {};
      try { staleAppts = await runStaleAppointments(body.dry_run === true); } catch (e) { staleAppts = { error: e instanceof Error ? e.message : String(e) }; }
      const fu = await runFollowups(body.dry_run === true);
      await heartbeat("fadc_followups", true);
      return json(200, { ...fu, resume, humanNudge, leaks, unanswered, ticketSweep, ticketChanges, reviewFollowups, amountChase, staleAppts });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      await heartbeat("fadc_followups", false, msg);
      return json(500, { ok: false, error: msg });
    }
  }

  // --- Direct JSON path (curl tests / ManyChat) — requires shared secret ---
  if (!WEBHOOK_SECRET || req.headers.get("x-fadc-secret") !== WEBHOOK_SECRET) {
    return json(401, { error: "unauthorized" });
  }
  const messengerId = String(body.subscriber_id ?? body.messenger_id ?? "").trim();
  const text = String(body.text ?? body.last_input_text ?? "").trim();
  const firstName = body.first_name ? String(body.first_name) : null;
  const lastName = body.last_name ? String(body.last_name) : null;
  if (!messengerId) return json(400, { error: "subscriber_id required" });

  try {
    const result = await processInbound({ messengerId, text, firstName, lastName });
    return json(200, { reply: result.reply, stage: result.stage, needs_human: result.needs_human });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return json(200, { reply: "", stage: "needs_human", error: msg });
  }
}
