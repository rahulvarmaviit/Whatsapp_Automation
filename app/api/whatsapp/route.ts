import { NextRequest } from "next/server";

function escapeXml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function toTwiml(message: string): string {
  const safeMessage = escapeXml(message);
  return `<?xml version="1.0" encoding="UTF-8"?><Response><Message>${safeMessage}</Message></Response>`;
}

async function parseTwilioInbound(request: NextRequest): Promise<{ body: string; from: string; to: string }> {
  const contentType = request.headers.get("content-type") || "";

  if (contentType.includes("application/x-www-form-urlencoded") || contentType.includes("multipart/form-data")) {
    const form = await request.formData();
    return {
      body: String(form.get("Body") || "").trim(),
      from: String(form.get("From") || "").trim(),
      to: String(form.get("To") || "").trim(),
    };
  }

  const raw = await request.text();
  const params = new URLSearchParams(raw);
  return {
    body: String(params.get("Body") || "").trim(),
    from: String(params.get("From") || "").trim(),
    to: String(params.get("To") || "").trim(),
  };
}

function toSessionId(from: string): string {
  const normalized = from.replace(/^whatsapp:/i, "").trim();
  return normalized || "whatsapp-default";
}

function getChatApiBaseUrl(request: NextRequest): string {
  if (process.env.INTERNAL_BASE_URL) {
    return process.env.INTERNAL_BASE_URL;
  }

  // Avoid ngrok-to-ngrok loop in local development.
  if (process.env.NODE_ENV !== "production") {
    const port = process.env.PORT || "3000";
    return `http://127.0.0.1:${port}`;
  }

  return request.nextUrl.origin;
}

export async function POST(request: NextRequest) {
  try {
    const inbound = await parseTwilioInbound(request);

    if (!inbound.body) {
      return new Response(toTwiml("I did not receive any message body."), {
        status: 200,
        headers: { "Content-Type": "text/xml; charset=utf-8" },
      });
    }

    const chatResponse = await fetch(new URL("/api/chat", getChatApiBaseUrl(request)), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        message: inbound.body,
        sessionId: toSessionId(inbound.from),
      }),
      cache: "no-store",
    });

    let reply = "Sorry, I could not process your message right now.";
    try {
      const payload = (await chatResponse.json()) as { reply?: string };
      if (payload?.reply && payload.reply.trim()) {
        reply = payload.reply;
      }
    } catch {
      // Keep a safe fallback reply for Twilio.
    }

    return new Response(toTwiml(reply), {
      status: 200,
      headers: { "Content-Type": "text/xml; charset=utf-8" },
    });
  } catch (error) {
    console.error("WhatsApp webhook failed:", error);
    return new Response(toTwiml("Temporary error. Please try again."), {
      status: 200,
      headers: { "Content-Type": "text/xml; charset=utf-8" },
    });
  }
}

export async function GET() {
  return new Response(toTwiml("WhatsApp webhook is running."), {
    status: 200,
    headers: { "Content-Type": "text/xml; charset=utf-8" },
  });
}