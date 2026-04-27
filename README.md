# Accounts Automation MVP (Chat-Only)

This repository contains a chat-only web MVP for accounts automation.

Current scope:

- Natural language chat commands handled by Gemini
- All database write operations require explicit confirmation
- Commands supported through chat:
	- create transaction
	- update pending transaction
	- approve transaction
	- reject transaction
	- summary (today/month)
	- list pending / list recent
- MongoDB for persistent data storage

## Stack

- Next.js (App Router)
- TypeScript
- MongoDB (`mongodb` driver)
- Gemini API (`generateContent`)

## Local Setup

1. **Install dependencies:**

	```bash
	npm install
	```

2. **Set up MongoDB:**

	Option A: Local MongoDB instance (Docker):
	```bash
	docker run --name mongo-accounts -d -p 27017:27017 mongo:7
	```

	Option B: MongoDB Atlas cloud (create account at https://www.mongodb.com/cloud/atlas):
	```
	MONGODB_URL=mongodb+srv://username:password@cluster.mongodb.net/accounts-automation?retryWrites=true&w=majority
	```

3. **Configure environment:**

	- Copy values from `.env.example` to `.env.local`
	- Set `MONGODB_URL` to your MongoDB connection string (default: `mongodb://localhost:27017/accounts-automation`)
	- Add your `GEMINI_API_KEY` and `GEMINI_MODEL`

4. **Start development server:**

	```bash
	npm run dev
	```

5. **Open in browser:**

	http://localhost:3000

## Chat Confirmation Rule

For any write request, the assistant first proposes a DB action and waits.

- Reply `CONFIRM` to execute.
- Reply `CANCEL` to discard.

No write happens before confirmation.

## Database

- SQLite file path is controlled by `DATABASE_PATH`
- Default: `./data/mvp.sqlite`
- Schema is auto-created on first API request

Tables:

- `transactions`
- `vendors`
- `audit_logs`
- `pending_actions`

## API Endpoints

- `POST /api/chat`

Environment placeholders are intentionally minimal now:

- `GEMINI_API_KEY`
- `GEMINI_MODEL`
# wa-automation-project
