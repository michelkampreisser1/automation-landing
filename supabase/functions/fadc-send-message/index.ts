import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

serve(async (req) => {
  try {
    const { psid, message, cron_secret } = await req.json();

    // Auth: match CRON_SECRET env var
    const expected = Deno.env.get("CRON_SECRET");
    if (!expected || cron_secret !== expected) {
      return new Response(JSON.stringify({ error: "unauthorized" }), { status: 401 });
    }

    const PAGE_TOKEN = Deno.env.get("META_PAGE_TOKEN");
    if (!PAGE_TOKEN) {
      return new Response(JSON.stringify({ error: "no page token configured" }), { status: 500 });
    }

    const res = await fetch(
      `https://graph.facebook.com/v21.0/me/messages?access_token=${PAGE_TOKEN}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          recipient: { id: psid },
          message: { text: message },
          messaging_type: "RESPONSE",
        }),
      },
    );

    const data = await res.json();
    return new Response(JSON.stringify(data), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: String(e) }), { status: 500 });
  }
});
