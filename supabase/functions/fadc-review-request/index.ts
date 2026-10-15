import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const REVIEWS_LINK = "https://www.facebook.com/profile.php?id=61593572532326&sk=reviews";

serve(async (req) => {
  try {
    const { recipients, cron_secret } = await req.json();

    // Auth check
    const expected = Deno.env.get("CRON_SECRET");
    if (!expected || cron_secret !== expected) {
      return new Response(JSON.stringify({ error: "unauthorized" }), { status: 401 });
    }

    const PAGE_TOKEN = Deno.env.get("META_PAGE_TOKEN");
    if (!PAGE_TOKEN) {
      return new Response(JSON.stringify({ error: "no page token" }), { status: 500 });
    }

    const results: { psid: string; name: string; ok: boolean; error?: string }[] = [];

    for (const r of recipients) {
      const message = r.message || buildDefaultMessage(r.name);
      try {
        const res = await fetch(
          `https://graph.facebook.com/v21.0/me/messages?access_token=${PAGE_TOKEN}`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              recipient: { id: r.psid },
              message: { text: message },
              messaging_type: "MESSAGE_TAG",
              tag: "CONFIRMED_EVENT_UPDATE",
            }),
          },
        );
        const data = await res.json();
        if (data.error) {
          results.push({ psid: r.psid, name: r.name, ok: false, error: data.error.message });
        } else {
          results.push({ psid: r.psid, name: r.name, ok: true });
        }
      } catch (e) {
        results.push({ psid: r.psid, name: r.name, ok: false, error: String(e) });
      }
    }

    return new Response(JSON.stringify({ results }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: String(e) }), { status: 500 });
  }
});

function buildDefaultMessage(name: string): string {
  return `Hi ${name}! 😊 Thank you so much for choosing First Air Duct Cleaning — we really appreciate your trust in our team.

We just launched our Facebook page and we'd love your support! Would you mind leaving us a quick review? It takes about 30 seconds and really helps a small family business like ours grow.

Here's the link: ${REVIEWS_LINK}

Thank you so much! 🙏`;
}
