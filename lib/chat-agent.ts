import { getDb } from "@/lib/db";
import { ObjectId } from "mongodb";

type TxKind = "purchase" | "payment" | "expense";
type VendorType = "credit" | "bill_to_bill" | "advance";

type AgentIntent =
  | "create_transaction"
  | "approve_transaction"
  | "reject_transaction"
  | "edit_at_approval"
  | "summary"
  | "list_pending"
  | "list_recent"
  | "ledger"
  | "search"
  | "unknown";

type AgentDecision = {
  intent: AgentIntent;
  requiresConfirmation: boolean;
  userMessage: string;
  confirmationMessage?: string;
  actionPayload?: Record<string, unknown>;
};

type GeminiCreateHints = {
  kind?: "purchase" | "payment";
  amount?: number;
  vendorName?: string;
  mode?: string;
  transactionDate?: string;
  product?: string;
  vendorMissing?: boolean;
};

type PendingAction = {
  intent: "create_transaction" | "approve_transaction" | "reject_transaction" | "edit_at_approval";
  actionPayload: Record<string, unknown>;
  requestedBy?: string;
};

type ValidationResult =
  | { valid: true }
  | { valid: false; message: string };

const CONFIRM_RE =
  /^(confirm|yes|yes please|yeah|yep|yup|ok|okay|ok please|sure|proceed|continue|go ahead|do it|approve this|accept it)$/i;
const CANCEL_RE =
  /^(cancel|no|no thanks|no thank you|nope|nah|discard|stop|leave it|drop it|abort)$/i;
const TXN_CODE_RE = /^[a-z0-9]{4}$/i;
const OBJECT_ID_RE = /^[a-f0-9]{24}$/i;
const TXN_CODE_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

// Approval Hierarchy from spec:
// gopi → can be approved by: gopi, father, brother
// father → can be approved by: gopi, brother
// brother → can be approved by: gopi
const APPROVAL_HIERARCHY: Record<string, string[]> = {
  gopi: ["brother"],
  father: ["gopi", "brother"],
  brother: ["gopi"],
};

function normalizeVendorNameForSearch(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]/g, "");
}

function levenshteinDistance(a: string, b: string): number {
  if (a === b) {
    return 0;
  }

  if (!a.length) {
    return b.length;
  }

  if (!b.length) {
    return a.length;
  }

  const previous = Array.from({ length: b.length + 1 }, (_, index) => index);

  for (let i = 1; i <= a.length; i++) {
    let diagonal = previous[0];
    previous[0] = i;

    for (let j = 1; j <= b.length; j++) {
      const temp = previous[j];
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      previous[j] = Math.min(previous[j] + 1, previous[j - 1] + 1, diagonal + cost);
      diagonal = temp;
    }
  }

  return previous[b.length];
}

function scoreVendorMatch(query: string, candidate: string): number {
  const normalizedQuery = normalizeVendorNameForSearch(query);
  const normalizedCandidate = normalizeVendorNameForSearch(candidate);

  if (!normalizedQuery || !normalizedCandidate) {
    return 0;
  }

  if (normalizedQuery === normalizedCandidate) {
    return 1;
  }

  if (
    normalizedCandidate.includes(normalizedQuery) ||
    normalizedQuery.includes(normalizedCandidate)
  ) {
    const lengthGap =
      Math.abs(normalizedCandidate.length - normalizedQuery.length) /
      Math.max(normalizedCandidate.length, normalizedQuery.length);
    return 0.9 - lengthGap * 0.2;
  }

  const distance = levenshteinDistance(normalizedQuery, normalizedCandidate);
  const maxLength = Math.max(normalizedQuery.length, normalizedCandidate.length);
  const similarity = 1 - distance / maxLength;
  const prefixBonus = normalizedQuery[0] === normalizedCandidate[0] ? 0.05 : 0;

  return similarity + prefixBonus;
}

function parseModelJson(text: string): AgentDecision | null {
  const fenced = text.match(/```json\s*([\s\S]*?)```/i);
  const raw = fenced ? fenced[1] : text;

  try {
    const parsed = JSON.parse(raw) as AgentDecision;
    if (!parsed.intent || !parsed.userMessage) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function parseGeminiCreateHints(text: string): GeminiCreateHints | null {
  const fenced = text.match(/```json\s*([\s\S]*?)```/i);
  const raw = fenced ? fenced[1] : text;

  try {
    const parsed = JSON.parse(raw) as GeminiCreateHints;
    if (!parsed || typeof parsed !== "object") {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function escapeRegex(input: string): string {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function getVendorSuggestions(query: string): Promise<string[]> {
  const db = await getDb();
  const cleaned = query.trim();
  if (!cleaned) {
    return [];
  }

  const suggestions = await db
    .collection("vendors")
    .find({
      is_active: { $ne: false },
    })
    .project({ name: 1 })
    .toArray();

  return suggestions
    .map((row) => String(row.name || "").trim())
    .filter(Boolean)
    .map((name) => ({
      name,
      score: scoreVendorMatch(cleaned, name),
    }))
    .filter(({ score, name }) => {
      const normalizedQuery = normalizeVendorNameForSearch(cleaned);
      const normalizedName = normalizeVendorNameForSearch(name);
      return (
        score >= 0.55 ||
        normalizedName.includes(normalizedQuery) ||
        normalizedQuery.includes(normalizedName)
      );
    })
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }

      return left.name.localeCompare(right.name);
    })
    .map(({ name }) => name)
    .filter((name, idx, arr) => arr.findIndex((v) => v.toLowerCase() === name.toLowerCase()) === idx)
    .slice(0, 5);
}

export async function resolveActiveVendor(vendorName: string): Promise<
  | { ok: true; vendorName: string; vendorType: VendorType }
  | { ok: false; message: string; suggestions?: string[] }
> {
  const db = await getDb();
  const cleaned = vendorName.trim();

  if (!cleaned) {
    return {
      ok: false,
      message: "Vendor name is required. Please provide a vendor from Vendor Master.",
    };
  }

  const exactRegex = new RegExp(`^\\s*${escapeRegex(cleaned)}\\s*$`, "i");
  const activeVendor = await db.collection("vendors").findOne({
    name: exactRegex,
    is_active: { $ne: false },
  });

  if (!activeVendor) {
    const suggestions = await getVendorSuggestions(cleaned);
    if (suggestions.length > 0) {
      return {
        ok: false,
        message: `Vendor \"${cleaned}\" is not in active Vendor Master. Did you mean: ${suggestions.join(", ")}?`,
        suggestions,
      };
    }

    return {
      ok: false,
      message: `Vendor \"${cleaned}\" is not in active Vendor Master. Please add/select a valid vendor first.`,
    };
  }

  const typeRaw = String(activeVendor.type || "credit").toLowerCase();
  const vendorType: VendorType = ["credit", "bill_to_bill", "advance"].includes(typeRaw)
    ? (typeRaw as VendorType)
    : "credit";

  return {
    ok: true,
    vendorName: String(activeVendor.name || cleaned),
    vendorType,
  };
}

function randomTxnCode(): string {
  let code = "";
  for (let i = 0; i < 4; i++) {
    const idx = Math.floor(Math.random() * TXN_CODE_ALPHABET.length);
    code += TXN_CODE_ALPHABET[idx];
  }
  return code;
}

async function generateUniqueTxnCode(): Promise<string> {
  const db = await getDb();

  for (let i = 0; i < 30; i++) {
    const code = randomTxnCode();
    const exists = await db.collection("transactions").findOne({ txn_code: code });
    if (!exists) {
      return code;
    }
  }

  throw new Error("Could not generate unique transaction code. Please retry.");
}

async function findTransactionByInputId(idInput: unknown) {
  const db = await getDb();
  const rawId = String(idInput || "").trim();

  if (!rawId) {
    throw new Error("I need a transaction ID.");
  }

  if (TXN_CODE_RE.test(rawId)) {
    const txn = await db.collection("transactions").findOne({ txn_code: rawId.toUpperCase() });
    if (!txn) {
      throw new Error(`Transaction not found for ID: ${rawId.toUpperCase()}`);
    }
    return txn;
  }

  if (OBJECT_ID_RE.test(rawId)) {
    const txn = await db.collection("transactions").findOne({ _id: new ObjectId(rawId) });
    if (!txn) {
      throw new Error("Transaction not found");
    }
    return txn;
  }

  throw new Error("Invalid transaction ID format. Use 4-character ID (e.g. A1B2).");
}

/**
 * Check if actor can approve a transaction created by creator.
 */
function canApprove(createdBy: string, actor: string): boolean {
  const approversForCreator = APPROVAL_HIERARCHY[createdBy.toLowerCase()];
  return approversForCreator ? approversForCreator.includes(actor.toLowerCase()) : false;
}

// Credit-only ledger logic: payment decreases balance, purchase/expense increases balance.
function computeBalanceDelta(kind: TxKind, amount: number): number {
  if (kind === "payment") {
    return -amount;
  }

  return amount;
}

async function applyApprovedLedgerEffect(txnId: ObjectId) {
  const db = await getDb();
  const txn = await db.collection("transactions").findOne({ _id: txnId });

  if (!txn || !txn.vendor_name) {
    return;
  }

  const existingVendor = await db.collection("vendors").findOne({
    name: new RegExp(`^\\s*${escapeRegex(String(txn.vendor_name || ""))}\\s*$`, "i"),
    is_active: { $ne: false },
  });

  if (!existingVendor) {
    throw new Error("Vendor is not active in Vendor Master. Cannot apply ledger effect.");
  }

  const delta = computeBalanceDelta(txn.kind, txn.amount);
  await db.collection("vendors").updateOne(
    { name: txn.vendor_name },
    {
      $inc: { balance: delta },
      $set: { type: "credit" },
    }
  );
}

export function validateWriteAction(
  intent: PendingAction["intent"],
  payload: Record<string, unknown>
): ValidationResult {
  if (intent === "create_transaction") {
    const missing: string[] = [];
    const kind = String(payload.kind || "").toLowerCase();
    const amount = Number(payload.amount);
    const mode = String(payload.mode || "").trim();
    const vendorName = String(payload.vendorName || "").trim();
    const transactionDate = String(payload.transactionDate || "").trim();

    if (!["purchase", "payment"].includes(kind)) {
      missing.push("kind (purchase/payment)");
    }
    if (!Number.isFinite(amount) || amount <= 0) {
      missing.push("amount (> 0)");
    }
    if (!vendorName) {
      missing.push("vendorName (must be from Vendor Master)");
    }
    if (kind === "payment" && !mode) {
      missing.push("mode (upi/cash/bank/etc)");
    }

    if (transactionDate && Number.isNaN(new Date(transactionDate).getTime())) {
      missing.push("transactionDate format (example: today or 2026-03-26)");
    }

    if (missing.length > 0) {
      return {
        valid: false,
        message: `I need these fields before confirmation: ${missing.join(", ")}. Please send them in one message.`,
      };
    }

    return { valid: true };
  }

  if (intent === "edit_at_approval") {
    const id = payload.id;
    const updates = (payload.updates || {}) as Record<string, unknown>;

    if (!id) {
      return {
        valid: false,
        message: "I need a valid transaction ID for edit. Example: edit transaction 12 amount 900.",
      };
    }

    const allowedFields = ["amount", "mode", "vendorName"];
    const invalidFields = Object.keys(updates).filter((key) => !allowedFields.includes(key));

    if (invalidFields.length > 0) {
      return {
        valid: false,
        message: `Cannot edit ${invalidFields.join(", ")}. Editable fields: ${allowedFields.join(", ")}.`,
      };
    }

    if (Object.keys(updates).length === 0) {
      return {
        valid: false,
        message: "I need at least one field to edit (amount, mode, vendorName).",
      };
    }

    return { valid: true };
  }

  if (intent === "approve_transaction" || intent === "reject_transaction") {
    const isBulk = Boolean(payload.bulk);
    if (isBulk) {
      const ids = Array.isArray(payload.ids) ? payload.ids : [];
      if (ids.length === 0) {
        return {
          valid: false,
          message: "I need transaction IDs for bulk approve/reject.",
        };
      }
      return { valid: true };
    }

    const id = payload.id;
    if (!id) {
      return {
        valid: false,
        message: "I need a valid transaction ID to approve/reject. Example: approve transaction A1B2.",
      };
    }

    return { valid: true };
  }

  return { valid: false, message: "Unsupported write intent." };
}

export function isConfirmMessage(message: string) {
  return CONFIRM_RE.test(message.trim());
}

export function isCancelMessage(message: string) {
  return CANCEL_RE.test(message.trim());
}

export async function getPendingAction(sessionId: string): Promise<PendingAction | null> {
  const db = await getDb();
  const row = await db.collection("pending_actions").findOne({ session_id: sessionId });

  if (!row) {
    return null;
  }

  try {
    return JSON.parse(row.action_json) as PendingAction;
  } catch {
    await clearPendingAction(sessionId);
    return null;
  }
}

export async function savePendingAction(sessionId: string, pending: PendingAction) {
  const db = await getDb();
  await db.collection("pending_actions").updateOne(
    { session_id: sessionId },
    {
      $set: {
        action_json: JSON.stringify(pending),
        created_at: new Date().toISOString(),
      },
    },
    { upsert: true }
  );
}

export async function clearPendingAction(sessionId: string) {
  const db = await getDb();
  await db.collection("pending_actions").deleteOne({ session_id: sessionId });
}

export async function askGeminiForDecision(message: string): Promise<AgentDecision> {
  const apiKey = process.env.GEMINI_API_KEY;
  const model = process.env.GEMINI_MODEL || "gemini-2.5-flash";
  const timeoutMs = Number(process.env.GEMINI_TIMEOUT_MS || "8000");

  if (!apiKey) {
    return {
      intent: "unknown",
      requiresConfirmation: false,
      userMessage: "GEMINI_API_KEY is missing. Add it in your env file.",
    };
  }

  const prompt = `You are an accounts automation parser.
Return STRICT JSON only with no markdown. Example: { "intent": "create_transaction", "requiresConfirmation": true, "userMessage": "Creating purchase", "confirmationMessage": "Confirm creation?", "actionPayload": { "kind": "purchase", "amount": 500, "vendorName": "ABC" } }

Intent types:
- create_transaction: new transaction request
- approve_transaction: approval request
- reject_transaction: rejection request
- edit_at_approval: modify pending transaction
- summary: aggregated counts/totals
- list_pending: show all PENDING transactions with full details
- list_recent: show all RECENT transactions with full details  
- ledger: vendor-specific balance info
- search: find specific transactions
- unknown: cannot interpret

DECISION LOGIC (CRITICAL):
- If user asks for "all transactions" OR "show me everything" OR "list all" → use list_recent
- If user asks for "pending" OR "waiting approval" → use list_pending
- If user asks for "summary" OR "totals" OR "counts" → use summary
- If user asks for vendor balance → use ledger
- If user asks to find/search → use search

Rules:
- Write/Delete intents need requiresConfirmation=true
- Read intents need requiresConfirmation=false
- Only parse fields that are provided; omit others from actionPayload
- Transaction IDs are 4-character alphanumeric codes (case-insensitive), e.g. A1B2

User message: ${message}`;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  let response: Response;
  try {
    response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ role: "user", parts: [{ text: prompt }] }],
          generationConfig: { temperature: 0.1 },
        }),
        signal: controller.signal,
      }
    );
  } catch (error) {
    if ((error as Error).name === "AbortError") {
      return {
        intent: "unknown",
        requiresConfirmation: false,
        userMessage: "Decision service timed out. Please retry or provide a more specific command.",
      };
    }

    return {
      intent: "unknown",
      requiresConfirmation: false,
      userMessage: `Decision service failed: ${String((error as Error).message || error).slice(0, 160)}`,
    };
  } finally {
    clearTimeout(timeoutId);
  }

  if (!response.ok) {
    const detail = await response.text();
    return {
      intent: "unknown",
      requiresConfirmation: false,
      userMessage: `Gemini request failed: ${detail.slice(0, 160)}`,
    };
  }

  const data = (await response.json()) as {
    candidates?: Array<{ content?: { parts?: Array<{ text?: string }> } }>;
  };

  const text = data.candidates?.[0]?.content?.parts?.[0]?.text;
  if (!text) {
    return {
      intent: "unknown",
      requiresConfirmation: false,
      userMessage: "Gemini returned an empty response.",
    };
  }

  const parsed = parseModelJson(text);
  if (!parsed) {
    return {
      intent: "unknown",
      requiresConfirmation: false,
      userMessage: "Could not parse Gemini response. Please rephrase your request.",
    };
  }

  return parsed;
}

export async function askGeminiForCreateHints(message: string): Promise<GeminiCreateHints | null> {
  const apiKey = process.env.GEMINI_API_KEY;
  const model = process.env.GEMINI_MODEL || "gemini-2.5-flash";
  const timeoutMs = Number(process.env.GEMINI_TIMEOUT_MS || "8000");

  if (!apiKey) {
    return null;
  }

  const prompt = `Extract transaction-entry hints from the user message.
Return STRICT JSON only. No markdown.

Schema:
{
  "kind": "purchase" | "payment" (optional),
  "amount": number (optional),
  "vendorName": string (optional),
  "mode": string (optional),
  "transactionDate": string ISO date/time (optional),
  "product": string (optional),
  "vendorMissing": boolean (optional)
}

Rules:
- If words like accessories/fruits/groceries appear as category/product and vendor is not explicit, set product and vendorMissing=true.
- If vendor is unclear, do not guess vendorName.
- Include only confident fields.

User message: ${message}`;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ role: "user", parts: [{ text: prompt }] }],
          generationConfig: { temperature: 0.1 },
        }),
        signal: controller.signal,
      }
    );

    if (!response.ok) {
      return null;
    }

    const data = (await response.json()) as {
      candidates?: Array<{ content?: { parts?: Array<{ text?: string }> } }>;
    };
    const text = data.candidates?.[0]?.content?.parts?.[0]?.text;
    if (!text) {
      return null;
    }

    return parseGeminiCreateHints(text);
  } catch {
    return null;
  } finally {
    clearTimeout(timeoutId);
  }
}

export async function executeReadIntent(intent: AgentIntent, payload?: Record<string, unknown>) {
  const db = await getDb();
  const vendorFilterRaw = payload?.vendorFilter ? String(payload.vendorFilter).trim() : "";
  const vendorRegex = vendorFilterRaw ? new RegExp(escapeRegex(vendorFilterRaw), "i") : null;
  const kindFilterRaw = payload?.kindFilter ? String(payload.kindFilter).trim().toLowerCase() : "";
  const kindFilter = ["payment", "purchase", "expense"].includes(kindFilterRaw) ? kindFilterRaw : "";
  const dateFrom = payload?.dateFrom ? String(payload.dateFrom) : "";
  const dateTo = payload?.dateTo ? String(payload.dateTo) : "";

  const dateClause: Record<string, unknown> = {};
  if (dateFrom) {
    dateClause.$gte = dateFrom;
  }
  if (dateTo) {
    dateClause.$lte = dateTo;
  }

  if (intent === "list_pending") {
    const maxRows = 200;
    const pendingFilter: Record<string, unknown> = {
      status: "pending",
      ...(vendorRegex ? { vendor_name: vendorRegex } : {}),
      ...(kindFilter ? { kind: kindFilter } : {}),
      ...(Object.keys(dateClause).length > 0 ? { transaction_time: dateClause } : {}),
    };
    const totalCount = await db.collection("transactions").countDocuments(pendingFilter);
    const rows = await db
      .collection("transactions")
      .find(pendingFilter)
      .sort({ transaction_time: -1 })
      .limit(maxRows)
      .toArray();

    return {
      reply: JSON.stringify(
        {
          pending: rows,
          totalCount,
          totalReturned: rows.length,
          truncated: totalCount > rows.length,
        },
        null,
        2
      ),
    };
  }

  if (intent === "list_recent") {
    const maxRows = 200;
    const recentFilter: Record<string, unknown> = {
      ...(vendorRegex ? { vendor_name: vendorRegex } : {}),
      ...(kindFilter ? { kind: kindFilter } : {}),
      ...(Object.keys(dateClause).length > 0 ? { transaction_time: dateClause } : {}),
    };
    const totalCount = await db.collection("transactions").countDocuments(recentFilter);
    const rows = await db
      .collection("transactions")
      .find(recentFilter)
      .sort({ transaction_time: -1 })
      .limit(maxRows)
      .toArray();

    return {
      reply: JSON.stringify(
        {
          recent: rows,
          totalCount,
          totalReturned: rows.length,
          truncated: totalCount > rows.length,
        },
        null,
        2
      ),
    };
  }

  if (intent === "ledger") {
    const vendorName = payload?.vendorFilter ? String(payload.vendorFilter).trim() : null;

    if (vendorName) {
      // Get ledger for specific vendor
      const vendorRegex = new RegExp(`^\\s*${escapeRegex(vendorName)}\\s*$`, "i");
      const vendor = await db.collection("vendors").findOne({ name: vendorRegex });
      if (!vendor) {
        const allTxnsForVendor = await db
          .collection("transactions")
          .find({ vendor_name: vendorRegex })
          .sort({ approved_at: -1, transaction_time: -1 })
          .toArray();

        const approvedTxns = allTxnsForVendor.filter(
          (row) => String((row as Record<string, unknown>).status || "").toLowerCase() === "approved"
        );

        if (approvedTxns.length > 0) {
          const firstTxn = approvedTxns[0] as Record<string, unknown>;
          const candidateType = String(firstTxn.vendor_type || "credit").toLowerCase();
          const derivedType: VendorType = ["credit", "bill_to_bill", "advance"].includes(candidateType)
            ? (candidateType as VendorType)
            : "credit";

          let derivedBalance = 0;
          for (const txn of approvedTxns) {
            const row = txn as Record<string, unknown>;
            const rowKindRaw = String(row.kind || "").toLowerCase();
            const rowKind: TxKind = ["purchase", "payment", "expense"].includes(rowKindRaw)
              ? (rowKindRaw as TxKind)
              : "purchase";
            const rowAmount = Number(row.amount || 0);
            if (Number.isFinite(rowAmount)) {
              derivedBalance += computeBalanceDelta(rowKind, rowAmount);
            }
          }

          return {
            reply: JSON.stringify(
              {
                vendor: {
                  name: String(firstTxn.vendor_name || vendorName),
                  type: derivedType,
                  balance: derivedBalance,
                },
                transactions: approvedTxns,
              },
              null,
              2
            ),
          };
        }

        if (allTxnsForVendor.length > 0) {
          const firstTxn = allTxnsForVendor[0] as Record<string, unknown>;
          const pendingCount = allTxnsForVendor.filter(
            (row) => String((row as Record<string, unknown>).status || "").toLowerCase() === "pending"
          ).length;

          return {
            reply: JSON.stringify(
              {
                vendor: {
                  name: String(firstTxn.vendor_name || vendorName),
                  type: String(firstTxn.vendor_type || "credit"),
                  balance: 0,
                },
                transactions: [],
                pendingCount,
              },
              null,
              2
            ),
          };
        }

        const partialTxnNameCandidates = await db
          .collection("transactions")
          .distinct("vendor_name", {
            vendor_name: { $regex: escapeRegex(vendorName), $options: "i" },
            status: "approved",
          });

        const normalizedCandidates = partialTxnNameCandidates
          .map((name) => String(name || "").trim())
          .filter((name) => name.length > 0)
          .filter((name, index, arr) => arr.findIndex((item) => item.toLowerCase() === name.toLowerCase()) === index);

        if (normalizedCandidates.length === 1) {
          const resolvedName = normalizedCandidates[0];
          const resolvedRegex = new RegExp(`^\\s*${escapeRegex(resolvedName)}\\s*$`, "i");
          const resolvedApprovedTxns = await db
            .collection("transactions")
            .find({ vendor_name: resolvedRegex, status: "approved" })
            .sort({ approved_at: -1, transaction_time: -1 })
            .toArray();

          if (resolvedApprovedTxns.length > 0) {
            const firstTxn = resolvedApprovedTxns[0] as Record<string, unknown>;
            const candidateType = String(firstTxn.vendor_type || "credit").toLowerCase();
            const derivedType: VendorType = ["credit", "bill_to_bill", "advance"].includes(candidateType)
              ? (candidateType as VendorType)
              : "credit";

            let derivedBalance = 0;
            for (const txn of resolvedApprovedTxns) {
              const row = txn as Record<string, unknown>;
              const rowKindRaw = String(row.kind || "").toLowerCase();
              const rowKind: TxKind = ["purchase", "payment", "expense"].includes(rowKindRaw)
                ? (rowKindRaw as TxKind)
                : "purchase";
              const rowAmount = Number(row.amount || 0);
              if (Number.isFinite(rowAmount)) {
                derivedBalance += computeBalanceDelta(rowKind, rowAmount);
              }
            }

            return {
              reply: JSON.stringify(
                {
                  vendor: {
                    name: String(firstTxn.vendor_name || resolvedName),
                    type: derivedType,
                    balance: derivedBalance,
                  },
                  transactions: resolvedApprovedTxns,
                },
                null,
                2
              ),
            };
          }
        }

        const suggestions = await db
          .collection("vendors")
          .find({ name: { $regex: escapeRegex(vendorName), $options: "i" } })
          .project({ name: 1 })
          .limit(5)
          .toArray();

        const txnNameSuggestionsRaw = await db
          .collection("transactions")
          .distinct("vendor_name", {
            vendor_name: { $regex: escapeRegex(vendorName), $options: "i" },
          });

        const suggestionNames = suggestions
          .map((row) => String(row.name || "").trim())
          .concat(txnNameSuggestionsRaw.map((name) => String(name || "").trim()))
          .filter((name) => name.length > 0)
          .filter((name, index, arr) => arr.findIndex((item) => item.toLowerCase() === name.toLowerCase()) === index)
          .slice(0, 5);

        return {
          reply: JSON.stringify(
            {
              notFoundVendor: vendorName,
              suggestions: suggestionNames,
            },
            null,
            2
          ),
        };
      }
      const txns = await db
        .collection("transactions")
        .find({ vendor_name: vendorRegex, status: "approved" })
        .sort({ approved_at: -1 })
        .toArray();
      return { reply: JSON.stringify({ vendor, transactions: txns }, null, 2) };
    } else {
      // Get all vendor ledgers
      const vendors = await db.collection("vendors").find({ is_active: { $ne: false } }).toArray();
      return { reply: JSON.stringify({ ledger: vendors }, null, 2) };
    }
  }

  if (intent === "search") {
    const vendorFilter = payload?.vendorFilter ? String(payload.vendorFilter).trim() : null;
    const searchQuery = payload?.searchQuery ? String(payload.searchQuery).trim() : null;

    if (!vendorFilter && !searchQuery) {
      return {
        reply: JSON.stringify(
          {
            search_results: [],
            count: 0,
            error: "SEARCH_CRITERIA_REQUIRED",
          },
          null,
          2
        ),
      };
    }

    const filter: Record<string, unknown> = {};
    if (vendorFilter) {
      filter.vendor_name = { $regex: vendorFilter, $options: "i" };
    }
    if (searchQuery) {
      filter.$or = [
        { reference_id: { $regex: searchQuery, $options: "i" } },
        { note: { $regex: searchQuery, $options: "i" } },
        { txn_hash: { $regex: searchQuery, $options: "i" } },
        { txn_code: { $regex: searchQuery, $options: "i" } },
      ];
    }

    const results = await db
      .collection("transactions")
      .find(filter)
      .sort({ transaction_time: -1 })
      .limit(50)
      .toArray();

    return { reply: JSON.stringify({ search_results: results, count: results.length }, null, 2) };
  }

  if (intent === "summary") {
    const range = payload?.range === "today" ? "today" : "month";
    const now = new Date();
    let dateFilter: Record<string, unknown>;

    if (range === "today") {
      const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      const endOfDay = new Date(startOfDay.getTime() + 24 * 60 * 60 * 1000);
      dateFilter = { $gte: startOfDay.toISOString(), $lt: endOfDay.toISOString() };
    } else {
      const startOfMonth = new Date(now.getFullYear(), now.getMonth(), 1);
      const endOfMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1);
      dateFilter = { $gte: startOfMonth.toISOString(), $lt: endOfMonth.toISOString() };
    }

    const rows = await db
      .collection("transactions")
      .find({ transaction_time: dateFilter })
      .toArray();

    let purchaseTotal = 0,
      paymentTotal = 0,
      expenseTotal = 0,
      pendingCount = 0,
      approvedCount = 0;

    for (const row of rows) {
      if (row.kind === "purchase") purchaseTotal += Number(row.amount || 0);
      if (row.kind === "payment") paymentTotal += Number(row.amount || 0);
      if (row.kind === "expense") expenseTotal += Number(row.amount || 0);
      if (row.status === "pending") pendingCount++;
      if (row.status === "approved") approvedCount++;
    }

    return {
      reply: JSON.stringify(
        {
          range,
          summary: {
            purchaseTotal,
            paymentTotal,
            expenseTotal,
            pendingCount,
            approvedCount,
          },
        },
        null,
        2
      ),
    };
  }

  return { reply: "I can help with create/approve/reject/edit, summary, pending/recent lists, ledger, or search." };
}

export async function formatReadResultForUser(rawJsonReply: string, context: string): Promise<string> {
  const apiKey = process.env.GEMINI_API_KEY;
  const model = process.env.GEMINI_MODEL || "gemini-2.5-flash";
  const timeoutMs = Number(process.env.GEMINI_TIMEOUT_MS || "8000");

  if (!apiKey) {
    return rawJsonReply;
  }

  const prompt = `You are a helpful assistant. The user asked about their transactions/accounts data.
Here is the raw data returned:
${rawJsonReply}

Context: "${context}"

IMPORTANT: Include transaction IDs in your response. You can say "ID: <id>" inline or mention them clearly.

Please format this data into a SHORT, NATURAL, USER-FRIENDLY sentence or 2-3 sentences. 
Do NOT return JSON or bullet points. Just a conversational response that includes the transaction IDs.
Example: "You have 2 pending transactions - ID: 60d5ec49c1234d5e8f3a2b1c is a payment of 500 to John via UPI, and ID: 60d5ec49c1234d5e8f3a2b2d is a purchase of 1200 from ABC Store via cash."

Format the response now (MUST include IDs):`;

  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    let response: Response;
    try {
      response = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            contents: [{ role: "user", parts: [{ text: prompt }] }],
            generationConfig: { temperature: 0.3, maxOutputTokens: 500 },
          }),
          signal: controller.signal,
        }
      );
    } finally {
      clearTimeout(timeoutId);
    }

    if (!response.ok) {
      return rawJsonReply;
    }

    const data = (await response.json()) as {
      candidates?: Array<{ content?: { parts?: Array<{ text?: string }> } }>;
    };

    const formattedText = data.candidates?.[0]?.content?.parts?.[0]?.text;
    return formattedText || rawJsonReply;
  } catch {
    return rawJsonReply;
  }
}

export async function executePendingAction(pending: PendingAction, actor: string) {
  const db = await getDb();

  if (pending.intent === "create_transaction") {
    const kind = String(pending.actionPayload.kind || "").toLowerCase() as TxKind;
    const amount = Number(pending.actionPayload.amount);
    const rawMode = String(pending.actionPayload.mode || "").trim();
    const mode = rawMode || (kind === "purchase" ? "credit" : "cash");
    const vendorNameInput = String(pending.actionPayload.vendorName || "").trim();
    const transactionDateRaw = String(pending.actionPayload.transactionDate || "").trim();
    const forceDuplicateConfirm = Boolean(pending.actionPayload.forceDuplicateConfirm);
    const createdBy = String(pending.requestedBy || actor).trim() || actor;
    const referenceId = pending.actionPayload.referenceId ? String(pending.actionPayload.referenceId) : null;
    const noteCandidate =
      pending.actionPayload.note ||
      pending.actionPayload.remarks ||
      pending.actionPayload.remark ||
      pending.actionPayload.product ||
      pending.actionPayload.details;
    const note = noteCandidate ? String(noteCandidate).trim() || null : null;

    if (!vendorNameInput) {
      throw new Error("VENDOR_MASTER: Vendor is required. Please provide a vendor from Vendor Master.");
    }

    if (!["purchase", "payment"].includes(kind) || amount <= 0) {
      throw new Error("Missing or invalid fields for create_transaction");
    }

    if (kind === "payment" && !mode) {
      throw new Error("Missing or invalid fields for create_transaction");
    }

    if (transactionDateRaw && Number.isNaN(new Date(transactionDateRaw).getTime())) {
      throw new Error("Missing or invalid fields for create_transaction");
    }

    const resolvedVendor = await resolveActiveVendor(vendorNameInput);
    if (!resolvedVendor.ok) {
      throw new Error(`VENDOR_MASTER: ${resolvedVendor.message}`);
    }

    const vendorName = resolvedVendor.vendorName;

    const transactionTime = transactionDateRaw
      ? new Date(transactionDateRaw).toISOString()
      : new Date().toISOString();

    const imageUrl = pending.actionPayload.imageUrl ? String(pending.actionPayload.imageUrl) : null;
    const txnHash = pending.actionPayload.txnHash ? String(pending.actionPayload.txnHash) : null;
    const imageHash = pending.actionPayload.imageHash ? String(pending.actionPayload.imageHash) : null;

    // Duplicate detection:
    // 1) exact by image hash
    // 2) exact by UPI reference id
    // 3) possible by vendor+amount+kind+time proximity
    if (imageHash) {
      const imageHashMatch = await db.collection("transactions").findOne({ image_hash: imageHash });
      if (imageHashMatch) {
        throw new Error(
          `DUPLICATE_EXACT: Exact duplicate blocked (same image hash). Existing ID: ${String(imageHashMatch.txn_code || "N/A")}`
        );
      }
    }

    if (referenceId && mode.toLowerCase() === "upi") {
      const refMatch = await db.collection("transactions").findOne({
        reference_id: referenceId,
        mode: { $regex: "^upi$", $options: "i" },
      });
      if (refMatch) {
        throw new Error(
          `DUPLICATE_EXACT: Exact duplicate blocked (same UPI reference). Existing ID: ${String(refMatch.txn_code || "N/A")}`
        );
      }
    }

    const vendorRegex = new RegExp(`^${escapeRegex(vendorName)}$`, "i");
    const txnDate = new Date(transactionTime);
    const windowMs = 2 * 60 * 60 * 1000;
    const start = new Date(txnDate.getTime() - windowMs).toISOString();
    const end = new Date(txnDate.getTime() + windowMs).toISOString();

    const similarDuplicate = await db.collection("transactions").findOne({
      kind,
      amount,
      vendor_name: vendorRegex,
      transaction_time: { $gte: start, $lte: end },
    });

    if (similarDuplicate && !forceDuplicateConfirm) {
      throw new Error(
        `DUPLICATE_POSSIBLE: A similar ${kind} already exists today (ID ${String(similarDuplicate.txn_code || "N/A")}) for ${vendorName}, amount ${amount}.` +
          " Please confirm again if this is a second payment/transaction."
      );
    }

    const txnCode = await generateUniqueTxnCode();

    const newTxn = {
      txn_code: txnCode,
      kind,
      amount,
      mode,
      vendor_name: vendorName,
      vendor_type: "credit",
      status: "pending",
      created_by: createdBy,
      transaction_time: transactionTime,
      created_at: new Date().toISOString(),
      reference_id: referenceId,
      note,
      image_url: imageUrl,
      txn_hash: txnHash,
      image_hash: imageHash,
      approved_by: null,
      approved_at: null,
    };

    const result = await db.collection("transactions").insertOne(newTxn);
    const created = await db.collection("transactions").findOne({ _id: result.insertedId });

    return {
      reply: `Created transaction with ID ${txnCode} in pending state. Awaiting approval from authorized user.`,
      data: created,
    };
  }

  if (pending.intent === "edit_at_approval") {
    const current = await findTransactionByInputId(pending.actionPayload.id);
    const txnId = current._id as ObjectId;

    const updates = (pending.actionPayload.updates || {}) as Record<string, unknown>;
    const editedBy = String(pending.requestedBy || actor).trim() || actor;

    if (typeof updates.vendorName === "string") {
      const resolvedVendor = await resolveActiveVendor(String(updates.vendorName));
      if (!resolvedVendor.ok) {
        throw new Error(`VENDOR_MASTER: ${resolvedVendor.message}`);
      }
      updates.vendorName = resolvedVendor.vendorName;
    }

    if (current.status !== "pending") {
      throw new Error("Only pending transactions can be edited. Cannot edit approved or rejected transactions.");
    }

    const updateMap: Record<string, string> = {
      amount: "amount",
      mode: "mode",
      vendorName: "vendor_name",
    };

    const updateData: Record<string, unknown> = {};

    for (const [inputKey, dbField] of Object.entries(updateMap)) {
      const nextVal = updates[inputKey];
      if (typeof nextVal === "undefined") {
        continue;
      }

      const prevVal = current[dbField];
      if (String(prevVal ?? "") === String(nextVal ?? "")) {
        continue;
      }

      updateData[dbField] = nextVal;

      await db.collection("audit_logs").insertOne({
        txn_id: txnId,
        action: "edit_at_approval",
        field_changed: dbField,
        old_value: String(prevVal ?? ""),
        new_value: String(nextVal ?? ""),
        changed_by: editedBy,
        timestamp: new Date().toISOString(),
      });
    }

    if (Object.keys(updateData).length === 0) {
      throw new Error("No valid changes to apply");
    }

    await db.collection("transactions").updateOne({ _id: txnId }, { $set: updateData });
    const updated = await db.collection("transactions").findOne({ _id: txnId });

    return { reply: `Transaction edited successfully. Ready for approval.`, data: updated };
  }

  if (pending.intent === "approve_transaction") {
    if (pending.actionPayload.bulk) {
      const ids = Array.isArray(pending.actionPayload.ids)
        ? pending.actionPayload.ids.map((id) => String(id))
        : [];

      if (ids.length === 0) {
        throw new Error("I need transaction IDs for bulk approval.");
      }

      let approvedCount = 0;
      let skippedCount = 0;
      const notes: string[] = [];

      for (const id of ids) {
        try {
          const current = await findTransactionByInputId(id);
          const txnId = current._id as ObjectId;

          if (current.status !== "pending") {
            skippedCount++;
            notes.push(`${id.toUpperCase()}: skipped (status is ${String(current.status || "unknown")})`);
            continue;
          }

          const createdBy = String(current.created_by || "").toLowerCase();
          if (!canApprove(createdBy, actor)) {
            skippedCount++;
            notes.push(`${id.toUpperCase()}: skipped (not authorized for creator ${createdBy})`);
            continue;
          }

          await db.collection("transactions").updateOne(
            { _id: txnId },
            {
              $set: {
                status: "approved",
                approved_by: actor,
                approved_at: new Date().toISOString(),
              },
            }
          );

          await applyApprovedLedgerEffect(txnId);

          await db.collection("audit_logs").insertOne({
            txn_id: txnId,
            action: "approve",
            approved_by: actor,
            timestamp: new Date().toISOString(),
          });

          approvedCount++;
        } catch (error) {
          skippedCount++;
          notes.push(`${id.toUpperCase()}: skipped (${(error as Error).message})`);
        }
      }

      const summary = `Bulk approval complete. Approved: ${approvedCount}. Skipped: ${skippedCount}.`;
      const details = notes.length > 0 ? `\nDetails:\n${notes.slice(0, 20).join("\n")}` : "";
      return { reply: `${summary}${details}` };
    }

    const current = await findTransactionByInputId(pending.actionPayload.id);
    const txnId = current._id as ObjectId;

    if (current.status !== "pending") {
      throw new Error("Only pending transactions can be approved");
    }

    const createdBy = String(current.created_by || "").toLowerCase();
    if (!canApprove(createdBy, actor)) {
      throw new Error(
        `You are not authorized to approve this transaction. Created by: ${createdBy}. Only authorized users can approve it.`
      );
    }

    await db.collection("transactions").updateOne(
      { _id: txnId },
      {
        $set: {
          status: "approved",
          approved_by: actor,
          approved_at: new Date().toISOString(),
        },
      }
    );

    await applyApprovedLedgerEffect(txnId);

    await db.collection("audit_logs").insertOne({
      txn_id: txnId,
      action: "approve",
      approved_by: actor,
      timestamp: new Date().toISOString(),
    });

    const updated = await db.collection("transactions").findOne({ _id: txnId });
    return { reply: `Transaction approved successfully. Ledger updated.`, data: updated };
  }

  if (pending.intent === "reject_transaction") {
    if (pending.actionPayload.bulk) {
      const ids = Array.isArray(pending.actionPayload.ids)
        ? pending.actionPayload.ids.map((id) => String(id))
        : [];

      if (ids.length === 0) {
        throw new Error("I need transaction IDs for bulk rejection.");
      }

      let rejectedCount = 0;
      let skippedCount = 0;
      const notes: string[] = [];

      for (const id of ids) {
        try {
          const current = await findTransactionByInputId(id);
          const txnId = current._id as ObjectId;

          if (current.status !== "pending") {
            skippedCount++;
            notes.push(`${id.toUpperCase()}: skipped (status is ${String(current.status || "unknown")})`);
            continue;
          }

          const createdBy = String(current.created_by || "").toLowerCase();
          if (!canApprove(createdBy, actor)) {
            skippedCount++;
            notes.push(`${id.toUpperCase()}: skipped (not authorized for creator ${createdBy})`);
            continue;
          }

          await db.collection("transactions").updateOne(
            { _id: txnId },
            {
              $set: {
                status: "rejected",
                rejected_by: actor,
                rejected_at: new Date().toISOString(),
              },
            }
          );

          await db.collection("audit_logs").insertOne({
            txn_id: txnId,
            action: "reject",
            rejected_by: actor,
            timestamp: new Date().toISOString(),
          });

          rejectedCount++;
        } catch (error) {
          skippedCount++;
          notes.push(`${id.toUpperCase()}: skipped (${(error as Error).message})`);
        }
      }

      const summary = `Bulk rejection complete. Rejected: ${rejectedCount}. Skipped: ${skippedCount}.`;
      const details = notes.length > 0 ? `\nDetails:\n${notes.slice(0, 20).join("\n")}` : "";
      return { reply: `${summary}${details}` };
    }

    const current = await findTransactionByInputId(pending.actionPayload.id);
    const txnId = current._id as ObjectId;

    if (current.status !== "pending") {
      throw new Error("Only pending transactions can be rejected");
    }

    const createdBy = String(current.created_by || "").toLowerCase();
    if (!canApprove(createdBy, actor)) {
      throw new Error(
        `You are not authorized to reject this transaction. Created by: ${createdBy}. Only authorized users can reject it.`
      );
    }

    await db.collection("transactions").updateOne(
      { _id: txnId },
      {
        $set: {
          status: "rejected",
          rejected_by: actor,
          rejected_at: new Date().toISOString(),
        },
      }
    );

    await db.collection("audit_logs").insertOne({
      txn_id: txnId,
      action: "reject",
      rejected_by: actor,
      timestamp: new Date().toISOString(),
    });

    const updated = await db.collection("transactions").findOne({ _id: txnId });
    return { reply: `Transaction rejected.`, data: updated };
  }

  throw new Error("Unsupported pending action");
}
