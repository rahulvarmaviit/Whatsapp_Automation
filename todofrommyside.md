# WhatsApp Sandbox Setup - What You Need To Do

The code setup is done on the app side. Complete the steps below from your side in order.

## 1) Run your Next.js app

- Start the app:

```bash
npm run dev
```

- Confirm this endpoint is reachable locally:

`http://localhost:3000/api/whatsapp`

## 2) Expose your local app to the internet

- ngrok now requires account verification + authtoken. Do this once:

```bash
# 1) Create/verify ngrok account
# https://dashboard.ngrok.com/signup

# 2) Install authtoken from your dashboard
ngrok config add-authtoken <YOUR_NGROK_AUTHTOKEN>
```

- Then in a second terminal, run:

```bash
ngrok http 3000
```

- Copy your HTTPS forwarding URL (example: `https://abc123.ngrok-free.app`).

If ngrok still fails, quick fallback:

```bash
npx localtunnel --port 3000
```

Use the HTTPS URL returned by localtunnel in Twilio webhook settings.

## 3) Join Twilio WhatsApp Sandbox

- Open Twilio Console -> Messaging -> Try it out -> Send a WhatsApp message -> Sandbox.
- From your personal WhatsApp number, send the join code shown by Twilio (for example: `join <code>`) to the Twilio sandbox WhatsApp number.
- Wait until Twilio shows your number is joined.

## 4) Configure inbound webhook URL in Twilio Sandbox

- In Sandbox settings, set:
- `When a message comes in` -> `POST` -> `https://<your-ngrok-domain>/api/whatsapp`
- Save configuration.

Important:
- It must be `https`.
- It must point to `/api/whatsapp`.
- Method must be `POST`.

## 5) Send a test WhatsApp message

- Send `hi` to your Twilio sandbox number from the joined WhatsApp account.
- Expected behavior:
- Twilio sends inbound message to your webhook.
- Your webhook forwards text into your existing `/api/chat` bot.
- Bot reply comes back as TwiML and appears in WhatsApp.

## 6) If it does not reply, check these first

- Your Next.js server is running.
- ngrok is still running and URL did not change.
- Twilio webhook URL matches the latest ngrok URL exactly.
- Your WhatsApp number is joined to sandbox.
- Twilio request method is `POST`.
- For ngrok: account is verified and authtoken is installed (`ERR_NGROK_4018` means this is missing).

## 7) Optional but recommended next step

- Add Twilio signature validation for production security using `X-Twilio-Signature`.
- Keep sandbox for current testing; production onboarding can be done later.