import { NextRequest, NextResponse } from "next/server";
import { DatabaseConnectionError, getDb } from "@/lib/db";

import {
  askGeminiForCreateHints,
  askGeminiForDecision,
  clearPendingAction,
  executePendingAction,
  executeReadIntent,
  formatReadResultForUser,
  getPendingAction,
  isCancelMessage,
  isConfirmMessage,
  resolveActiveVendor,
  savePendingAction,
  validateWriteAction,
} from "@/lib/chat-agent";

const ALLOWED_USERS = new Set(["gopi", "father", "brother"]);
const VENDOR_MASTER_EDITORS = new Set(["gopi", "brother"]);

const TRANSACTION_RELATED_RE =
  /\b(transaction|transactions|purchase|payment|expense|ledger|summary|approve|approval|reject|pending|vendor|balance|amount|upi|cash|bank|txn|reference|ref)\b/i;

// Vendor categories/types that should be recognized and prompt for actual vendor name
const VENDOR_TYPE_KEYWORDS: Record<string, string> = {
  fruits: "fruits/vegetables",
  vegetables: "fruits/vegetables",
  groceries: "groceries",
  electronics: "electronics",
  hardware: "hardware",
  stationary: "stationary/office supplies",
  stationery: "stationary/office supplies",
  office: "office supplies",
  fuel: "fuel/petrol",
  petrol: "fuel/petrol",
  gas: "fuel/petrol",
  medicine: "medicine/pharmacy",
  dairy: "dairy products",
  milk: "dairy products",
  bread: "bread/bakery",
  bakery: "bakery",
  clothing: "clothing/apparel",
  clothes: "clothing/apparel",
  shoes: "shoes/footwear",
  footwear: "shoes/footwear",
  books: "books",
  travel: "travel/transport",
  uber: "travel/transport",
  taxi: "travel/transport",
  restaurant: "restaurant/food",
  food: "restaurant/food",
  hotel: "hotel/accommodation",
  rent: "rent",
  bills: "bills/utility",
  utility: "bills/utility",
  water: "water",
  electricity: "electricity",
  internet: "internet/telecom",
  phone: "phone/telecom",
  insurance: "insurance",
  gifts: "gifts",
  toys: "toys",
  accessories: "hardware/accessories",
  accesories: "hardware/accessories",
};

const NON_VENDOR_FIRST_TOKENS = new Set([
  "purchase",
  "purchased",
  "buy",
  "bought",
  "payment",
  "paid",
  "pay",
  "paying",
  "expense",
  "expenses",
  "material",
  "materials",
  "goods",
  "bill",
  "entry",
  "order",
  "taken",
  "profile",
  "profiles",
  "vendor",
  "supplier",
  "shop",
  "factory",
  "from",
  "to",
]);

type SessionRow = {
  actor?: string;
  lastTxnCodes?: string[];
  draftWriteAction?: {
    intent: "create_transaction" | "approve_transaction" | "reject_transaction" | "edit_at_approval";
    actionPayload: Record<string, unknown>;
  };
};

type DraftWriteIntent = NonNullable<SessionRow["draftWriteAction"]>["intent"];

function parseLoginCommand(message: string): { username: string; password: string } | null {
  const trimmed = message.trim();
  const patterns = [
    /^login\s+(gopi|father|brother)\s+(\S+)$/i,
    /^(?:login|log\s*in|sign\s*in)\s+(?:as\s+)?(gopi|father|brother)(?:\s+(?:with\s+(?:password|pass)\s+)?(\S+))?$/i,
    /^(?:switch\s+to|use)\s+(gopi|father|brother)(?:\s+(?:with\s+(?:password|pass)\s+)?(\S+))?$/i,
  ];

  for (const pattern of patterns) {
    const match = trimmed.match(pattern);
    if (!match) {
      continue;
    }

    const username = match[1].toLowerCase();
    const password = (match[2] || username).trim();
    return { username, password };
  }

  return null;
}

function isLogoutMessage(message: string): boolean {
  return /^(logout|log\s*out|sign\s*out|log me out|sign me out|switch user)$/i.test(message.trim());
}

function detectVendorTypeFromMessage(message: string): string | null {
  // Check for patterns like "vendor type is X", "for X", "from X category"
  const patterns = [
    /\b(vendor\s+type\s+is\s+|category\s+is\s+|for\s+|from\s+)([a-z]+)\b/i,
  ];
  
  for (const pattern of patterns) {
    const match = message.match(pattern);
    if (match) {
      const keyword = match[2].toLowerCase().trim();
      if (VENDOR_TYPE_KEYWORDS[keyword]) {
        return keyword;
      }
    }
  }

  // Also check if the first word after numbers is a vendor type keyword
  // This handles: "fruits 1500" or "1500 fruits"
  const simpleAmountMatch = message.match(/^\s*([a-z]+)\s+(\d+)(?:\.\d+)?\s*$/i) ||
                            message.match(/^\s*(\d+)(?:\.\d+)?\s+([a-z]+)\s*$/i);
  if (simpleAmountMatch) {
    const maybeCategoryWord = simpleAmountMatch[1];
    if (!isNaN(Number(maybeCategoryWord))) {
      // maybeCategoryWord is the number, other one is the word
      const categoryWord = simpleAmountMatch[2].toLowerCase();
      if (VENDOR_TYPE_KEYWORDS[categoryWord]) {
        return categoryWord;
      }
    } else {
      // maybeCategoryWord is the word
      const categoryWord = maybeCategoryWord.toLowerCase();
      if (VENDOR_TYPE_KEYWORDS[categoryWord]) {
        return categoryWord;
      }
    }
  }

  return null;
}

function isDescribeChangesMessage(message: string): boolean {
  const lower = message.toLowerCase().trim();
  return /\b(change|change to|update|set|modify|make it|instead|too|to|replace)\b/.test(lower) &&
         !/^(change topic|change subject|change mind)\b/.test(lower);
}

function formatFieldValueReadReply(intent: string, rawReply: string, userMessage?: string): string | null {
  try {
    const parsed = JSON.parse(rawReply) as Record<string, unknown>;

    if (intent === "ledger") {
      const ledger = Array.isArray(parsed.ledger) ? (parsed.ledger as Record<string, unknown>[]) : null;
      const vendor = parsed.vendor as Record<string, unknown> | undefined;
      const transactions = Array.isArray(parsed.transactions)
        ? (parsed.transactions as Record<string, unknown>[])
        : [];
      const pendingCount = Number(parsed.pendingCount || 0);
      const notFoundVendor = String(parsed.notFoundVendor || "").trim();
      const suggestions = Array.isArray(parsed.suggestions)
        ? (parsed.suggestions as unknown[]).map((v) => String(v)).filter(Boolean)
        : [];

      if (notFoundVendor) {
        if (suggestions.length > 0) {
          return `I could not find exact ledger for \"${notFoundVendor}\". Did you mean: ${suggestions.join(", ")} ? Reply: ledger for <vendor name>.`;
        }
        return `I could not find ledger for \"${notFoundVendor}\". Please share the exact vendor name. Example: ledger for A1 Traders.`;
      }

      if (vendor) {
        const lines = [
          `Vendor: ${String(vendor.name || "N/A")}`,
          `Type: ${String(vendor.type || "N/A")}`,
          `Balance: ${String(vendor.balance ?? 0)}`,
          `Approved Transactions: ${transactions.length}`,
        ];
        if (pendingCount > 0) {
          lines.push(`Pending Transactions: ${pendingCount}`);
        }
        return lines.join("\n");
      }

      if (ledger && ledger.length > 0) {
        const lines = [`Total Vendors: ${ledger.length}`];
        ledger.forEach((row, index) => {
          lines.push(
            `${index + 1}. Vendor: ${String(row.name || "N/A")} | Type: ${String(row.type || "N/A")} | Balance: ${String(row.balance ?? 0)}`
          );
        });
        return lines.join("\n");
      }
      return "No ledger data found.";
    }

    if (intent === "summary") {
      const summary = (parsed.summary || {}) as Record<string, unknown>;
      return [
        `Range: ${String(parsed.range || "month")}`,
        `Purchase Total: ${String(summary.purchaseTotal ?? 0)}`,
        `Payment Total: ${String(summary.paymentTotal ?? 0)}`,
        `Expense Total: ${String(summary.expenseTotal ?? 0)}`,
        `Pending Count: ${String(summary.pendingCount ?? 0)}`,
        `Approved Count: ${String(summary.approvedCount ?? 0)}`,
      ].join("\n");
    }

    if (intent === "search") {
      if (String(parsed.error || "") === "SEARCH_CRITERIA_REQUIRED") {
        return "What should I search by: transaction ID, vendor, reference, or hash? Follow-up: try 'when was transaction NGK4 created?'";
      }

      const rows = Array.isArray(parsed.search_results)
        ? (parsed.search_results as Record<string, unknown>[])
        : [];
      if (rows.length === 0) {
        return "No matching transactions found.";
      }

      const asksCreatedTime = Boolean(
        userMessage && /\b(when|what time|created|creation)\b/i.test(userMessage) && /\b(transaction|txn)\b/i.test(userMessage)
      );

      if (asksCreatedTime) {
        if (rows.length === 1) {
          const row = rows[0];
          const txnCode = String(row.txn_code || "N/A").toUpperCase();
          const createdBy = String(row.created_by || "N/A");
          const createdAt = row.transaction_time
            ? new Date(String(row.transaction_time)).toLocaleString()
            : "N/A";
          return `Transaction ${txnCode} was created on ${createdAt} by ${createdBy}.`;
        }

        const lines = [
          `I found ${rows.length} matching transactions. Please use a specific ID if you want one exact creation time:`,
        ];
        rows.slice(0, 20).forEach((row, index) => {
          const txnCode = String(row.txn_code || "N/A").toUpperCase();
          const createdAt = row.transaction_time
            ? new Date(String(row.transaction_time)).toLocaleString()
            : "N/A";
          lines.push(`${index + 1}. ID: ${txnCode} | Created: ${createdAt}`);
        });
        return lines.join("\n");
      }

      const lines = [`Matches: ${rows.length}`];
      rows.slice(0, 50).forEach((row, index) => {
        const txnCode = String(row.txn_code || "N/A");
        const vendor = String(row.vendor_name || "N/A");
        const amount = Number(row.amount || 0);
        const status = String(row.status || "N/A");
        const createdAt = row.transaction_time
          ? new Date(String(row.transaction_time)).toLocaleString()
          : "N/A";
        lines.push(
          `${index + 1}. ID: ${txnCode} | Vendor: ${vendor} | Amount: ${amount} | Status: ${status} | Created: ${createdAt}`
        );
      });
      return lines.join("\n");
    }

    return null;
  } catch {
    return null;
  }
}

async function getSessionRow(sessionId: string): Promise<SessionRow | null> {
  const db = await getDb();
  const row = await db.collection("chat_sessions").findOne({ session_id: sessionId });
  if (!row) {
    return null;
  }

  return {
    actor: row.actor ? String(row.actor).toLowerCase() : undefined,
    lastTxnCodes: Array.isArray(row.lastTxnCodes)
      ? row.lastTxnCodes.map((code: unknown) => String(code).toUpperCase())
      : undefined,
    draftWriteAction:
      row.draftWriteAction &&
      typeof row.draftWriteAction === "object" &&
      row.draftWriteAction.intent &&
      row.draftWriteAction.actionPayload
        ? {
            intent: String(row.draftWriteAction.intent) as DraftWriteIntent,
            actionPayload: row.draftWriteAction.actionPayload as Record<string, unknown>,
          }
        : undefined,
  };
}

async function setSessionActor(sessionId: string, actor: string) {
  const db = await getDb();
  await db.collection("chat_sessions").updateOne(
    { session_id: sessionId },
    {
      $set: {
        actor: actor.toLowerCase(),
        updated_at: new Date().toISOString(),
      },
    },
    { upsert: true }
  );
}

async function clearSessionActor(sessionId: string) {
  const db = await getDb();
  await db.collection("chat_sessions").deleteOne({ session_id: sessionId });
}

async function setSessionContext(sessionId: string, context: Partial<SessionRow>) {
  const db = await getDb();
  await db.collection("chat_sessions").updateOne(
    { session_id: sessionId },
    {
      $set: {
        ...context,
        updated_at: new Date().toISOString(),
      },
    },
    { upsert: true }
  );
}

async function clearDraftWriteAction(sessionId: string) {
  const db = await getDb();
  await db.collection("chat_sessions").updateOne(
    { session_id: sessionId },
    {
      $unset: { draftWriteAction: "" },
      $set: { updated_at: new Date().toISOString() },
    },
    { upsert: true }
  );
}

function isTransactionRelatedMessage(message: string): boolean {
  return TRANSACTION_RELATED_RE.test(message);
}

function isGreetingMessage(message: string): boolean {
  const normalized = message.trim().toLowerCase();
  return /^(hi|hello|hey|hola|good\s+morning|good\s+afternoon|good\s+evening)\b/.test(normalized);
}

function getSimpleChatReply(message: string): string {
  const normalized = message.trim().toLowerCase();

  if (/^(hi|hello|hey|hola)\b/.test(normalized)) {
    return "Hi. I can chat normally here. For any transaction request, please login first (example: login gopi gopi).";
  }

  if (/\bhow are you\b/.test(normalized)) {
    return "I am doing well. For transaction actions, login is mandatory (example: login gopi gopi).";
  }

  if (/\bhelp\b/.test(normalized)) {
    return "You can chat normally without login. For transactions, summary, approvals, ledger, and search, login first using: login gopi gopi (or father/father, brother/brother).";
  }

  return "I can chat normally without login. To access transactions, please login first: login gopi gopi.";
}

function isDatabaseConnectivityError(error: unknown): boolean {
  const detail = error instanceof Error ? `${error.name} ${error.message}` : String(error);
  const lower = detail.toLowerCase();

  return (
    lower.includes("mongoserverselectionerror") ||
    lower.includes("mongonetworktimeouterror") ||
    lower.includes("replicasetnoprimary") ||
    lower.includes("secureconnect") ||
    lower.includes("server selection timed out") ||
    lower.includes("database connection failed")
  );
}

function escapeRegex(input: string): string {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

type VendorMasterCommand =
  | { type: "list" }
  | { type: "add"; name: string }
  | { type: "edit"; oldName: string; newName: string }
  | { type: "delete"; name: string };

function parseVendorMasterCommand(message: string): VendorMasterCommand | null {
  const trimmed = message.trim();
  const lower = trimmed.toLowerCase();

  if (
    /^(list|show|get)\s+(all\s+)?vendors?$/i.test(trimmed) ||
    /^(?:list|show|get)\s+(?:the\s+)?vendor\s+list$/i.test(trimmed) ||
    /^vendors?\s+list$/i.test(trimmed) ||
    (/\bvend(?:or|odr)\s+master\b/i.test(trimmed) && /\b(list|show|give|get|all|what\s+are)\b/i.test(trimmed)) ||
    /^what\s+are\s+the\s+vendors?(?:\s+in\s+the\s+vend(?:or|odr)\s+master)?\??$/i.test(trimmed)
  ) {
    return { type: "list" };
  }

  const addMatch = trimmed.match(/^add\s+vendor\s+(.+)$/i);
  if (addMatch) {
    return {
      type: "add",
      name: addMatch[1].trim(),
    };
  }

  const addVendorMasterMatch = trimmed.match(
    /^(?:add|create|insert)\s+(?:this\s+)?vendor(?:\s+in(?:to)?\s+the?\s+vendor\s+master)?\s*[:-]\s*(.+)$/i
  );
  if (addVendorMasterMatch) {
    return {
      type: "add",
      name: addVendorMasterMatch[1].trim(),
    };
  }

  const addToVendorMasterMatch = trimmed.match(
    /^(?:add|create|insert)\s+(.+?)\s+(?:to|in(?:to)?)\s+(?:the\s+)?vendor\s+master$/i
  );
  if (addToVendorMasterMatch) {
    return {
      type: "add",
      name: addToVendorMasterMatch[1].trim(),
    };
  }

  if (/^(?:add|create|insert)\s+(?:this\s+)?vendor\s+(?:in|to|into)\s+(?:the\s+)?vendor\s+master$/i.test(trimmed)) {
    return {
      type: "add",
      name: "",
    };
  }

  if (/^(?:i\s+want\s+to\s+)?(?:add|create|insert|make)\s+(?:a\s+)?new\s+entry\s+(?:in|into|to)\s+(?:the\s+)?vend(?:or|odr)\s+master$/i.test(trimmed)) {
    return {
      type: "add",
      name: "",
    };
  }

  if (/\bvend(?:or|odr)\s+master\b/.test(lower) && /\b(add|create|insert|new\s+entry|make\s+entry)\b/.test(lower)) {
    const nameHint = trimmed.match(/[:-]\s*(.+)$/);
    return {
      type: "add",
      name: nameHint?.[1]?.trim() || "",
    };
  }

  const namedAddMatch = trimmed.match(
    /^(?:please\s+)?(?:add|create|insert|make)\s+(?:a\s+)?vendor(?:\s+named)?\s+(.+)$/i
  );
  if (namedAddMatch) {
    return {
      type: "add",
      name: namedAddMatch[1].trim(),
    };
  }

  const editToMatch = trimmed.match(
    /^edit\s+vendor\s+(.+?)\s+(?:to|as)\s+(.+)$/i
  );
  if (editToMatch) {
    return {
      type: "edit",
      oldName: editToMatch[1].trim(),
      newName: editToMatch[2].trim(),
    };
  }

  const renameMatch = trimmed.match(
    /^(?:rename|change|update)\s+vendor\s+(.+?)\s+(?:to|as)\s+(.+)$/i
  );
  if (renameMatch) {
    return {
      type: "edit",
      oldName: renameMatch[1].trim(),
      newName: renameMatch[2].trim(),
    };
  }

  const deleteMatch = trimmed.match(/^(delete|remove)\s+vendor\s+(.+)$/i);
  if (deleteMatch) {
    return {
      type: "delete",
      name: deleteMatch[2].trim(),
    };
  }

  const deleteVendorMasterMatch = trimmed.match(
    /^(?:delete|remove)\s+(.+?)\s+(?:from\s+)?(?:the\s+)?vendor\s+master$/i
  );
  if (deleteVendorMasterMatch) {
    return {
      type: "delete",
      name: deleteVendorMasterMatch[1].trim(),
    };
  }

  return null;
}

async function getVendorSuggestions(name: string): Promise<string[]> {
  const db = await getDb();
  const suggestions = await db
    .collection("vendors")
    .find({
      name: { $regex: escapeRegex(name), $options: "i" },
      is_active: { $ne: false },
    })
    .project({ name: 1 })
    .limit(5)
    .toArray();

  return suggestions
    .map((row) => String(row.name || "").trim())
    .filter(Boolean)
    .filter((v, idx, arr) => arr.findIndex((x) => x.toLowerCase() === v.toLowerCase()) === idx);
}

async function handleVendorMasterCommand(command: VendorMasterCommand, actor: string): Promise<string> {
  const db = await getDb();

  if (command.type === "list") {
    const rows = await db
      .collection("vendors")
      .find({ is_active: { $ne: false } })
      .project({ name: 1, type: 1, balance: 1 })
      .sort({ name: 1 })
      .toArray();

    if (rows.length === 0) {
      return "Vendor Master is empty. Add vendor using: add vendor <name>.";
    }

    const lines = rows.map(
      (row, idx) => `${idx + 1}. ${String(row.name || "N/A")} | balance: ${Number(row.balance || 0)}`
    );
    return [`Active Vendors (${rows.length}):`, ...lines].join("\n");
  }

  if (!VENDOR_MASTER_EDITORS.has(actor)) {
    return "You are not authorized to modify Vendor Master. Only gopi and brother can add/edit/delete vendors.";
  }

  if (command.type === "add") {
    const name = command.name.trim().replace(/\s{2,}/g, " ");
    if (!name) {
      return "Vendor name is required. Usage: add vendor <name>.";
    }

    const exactRegex = new RegExp(`^\\s*${escapeRegex(name)}\\s*$`, "i");
    const existing = await db.collection("vendors").findOne({ name: exactRegex });

    if (existing && existing.is_active !== false) {
      return `Vendor \"${String(existing.name)}\" already exists in Vendor Master.`;
    }

    if (existing && existing.is_active === false) {
      await db.collection("vendors").updateOne(
        { _id: existing._id },
        {
          $set: {
            is_active: true,
            type: "credit",
            updated_at: new Date().toISOString(),
          },
        }
      );
      return `Vendor \"${String(existing.name)}\" re-activated in Vendor Master.`;
    }

    await db.collection("vendors").insertOne({
      name,
      type: "credit",
      balance: 0,
      is_active: true,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    });
    return `Vendor \"${name}\" added to Vendor Master.`;
  }

  if (command.type === "edit") {
    const oldName = command.oldName.trim();
    const newName = command.newName.trim();
    if (!oldName || !newName) {
      return "Usage: edit vendor <old name> to <new name>.";
    }

    const exactOld = new RegExp(`^\\s*${escapeRegex(oldName)}\\s*$`, "i");
    const current = await db.collection("vendors").findOne({ name: exactOld, is_active: { $ne: false } });
    if (!current) {
      const suggestions = await getVendorSuggestions(oldName);
      if (suggestions.length > 0) {
        return `Vendor \"${oldName}\" not found. Did you mean: ${suggestions.join(", ")}?`;
      }
      return `Vendor \"${oldName}\" not found in active Vendor Master.`;
    }

    const exactNew = new RegExp(`^\\s*${escapeRegex(newName)}\\s*$`, "i");
    const conflicting = await db.collection("vendors").findOne({ name: exactNew, is_active: { $ne: false } });
    if (conflicting && String(conflicting._id) !== String(current._id)) {
      return `Cannot rename to \"${newName}\" because that vendor already exists.`;
    }

    const currentName = String(current.name || oldName);

    await db.collection("vendors").updateOne(
      { _id: current._id },
      {
        $set: {
          name: newName,
          type: "credit",
          updated_at: new Date().toISOString(),
        },
      }
    );

    if (currentName.toLowerCase() !== newName.toLowerCase()) {
      const currentNameRegex = new RegExp(`^\\s*${escapeRegex(currentName)}\\s*$`, "i");
      await db.collection("transactions").updateMany({ vendor_name: currentNameRegex }, { $set: { vendor_name: newName } });
    }

    return `Vendor updated: \"${currentName}\" -> \"${newName}\".`;
  }

  const name = command.name.trim();
  if (!name) {
    return "Usage: delete vendor <name>.";
  }

  const exactRegex = new RegExp(`^\\s*${escapeRegex(name)}\\s*$`, "i");
  const vendor = await db.collection("vendors").findOne({ name: exactRegex, is_active: { $ne: false } });
  if (!vendor) {
    const suggestions = await getVendorSuggestions(name);
    if (suggestions.length > 0) {
      return `Vendor \"${name}\" not found. Did you mean: ${suggestions.join(", ")}?`;
    }
    return `Vendor \"${name}\" not found in active Vendor Master.`;
  }

  const usedCount = await db.collection("transactions").countDocuments({ vendor_name: exactRegex });
  if (usedCount > 0) {
    return `Cannot delete vendor \"${String(vendor.name)}\" because it is already used in ${usedCount} transaction(s).`;
  }

  await db.collection("vendors").updateOne(
    { _id: vendor._id },
    {
      $set: {
        is_active: false,
        updated_at: new Date().toISOString(),
      },
    }
  );

  return `Vendor \"${String(vendor.name)}\" deleted from active Vendor Master.`;
}

function extractVendorFilterFromMessage(message: string): string | null {
  const patterns = [
    /transactions?\s+(?:of|for)\s+([a-z0-9 .&'_-]+)/i,
    /(?:from|for)\s+([a-z0-9 .&'_-]+)\s+transactions?/i,
    /vendor\s+([a-z0-9 .&'_-]+)/i,
  ];

  for (const pattern of patterns) {
    const match = message.match(pattern);
    const candidate = match?.[1]?.trim();
    if (candidate) {
      return candidate;
    }
  }

  return null;
}

function extractLedgerVendorFromMessage(message: string): string | null {
  if (!/\bledger\b/i.test(message)) {
    return null;
  }

  const patterns = [
    /ledger\s+(?:of|for)\s+(.+)$/i,
    /ledger\s+between\s+(.+?)\s+and\s+me\b/i,
    /full\s+ledger\s+between\s+(.+?)\s+and\s+me\b/i,
    /complete\s+ledger\s+of\s+(.+)$/i,
  ];

  for (const pattern of patterns) {
    const match = message.match(pattern);
    const candidate = match?.[1]?.trim();
    if (candidate) {
      return candidate.replace(/[?.!,]+$/, "").trim();
    }
  }

  return null;
}

function extractTxnCodeFromMessage(message: string): string | null {
  const candidates = message.match(/\b[a-z0-9]{4}\b/gi) || [];
  for (const candidate of candidates) {
    const hasDigit = /\d/.test(candidate);
    const hasLetter = /[a-z]/i.test(candidate);
    if (hasDigit && hasLetter) {
      return candidate.toUpperCase();
    }
  }
  return null;
}

function extractApproveRejectIntent(message: string): {
  intent: "approve_transaction" | "reject_transaction";
  id: string;
} | null {
  const normalized = message.trim();

  const approveMatch = normalized.match(
    /\b(?:approve|accept|ok(?:ay)?|clear)(?:\s+transaction|\s+txn|\s+this|\s+that)?\s+([a-z0-9]{4})\b/i
  );
  if (approveMatch) {
    return {
      intent: "approve_transaction",
      id: approveMatch[1].toUpperCase(),
    };
  }

  const rejectMatch = normalized.match(
    /\b(?:reject|decline|deny|cancel)(?:\s+transaction|\s+txn|\s+this|\s+that)?\s+([a-z0-9]{4})\b/i
  );
  if (rejectMatch) {
    return {
      intent: "reject_transaction",
      id: rejectMatch[1].toUpperCase(),
    };
  }

  return null;
}

function extractBulkApproveRejectIntent(message: string):
  | { intent: "approve_transaction" | "reject_transaction" }
  | null {
  const normalized = message.trim().toLowerCase();

  if (/\b(?:approve|accept)\s+all(?:\s+those|\s+the)?\s+(?:pending\s+)?transactions?\b/.test(normalized)) {
    return { intent: "approve_transaction" };
  }

  if (/\b(?:reject|decline)\s+all(?:\s+those|\s+the)?\s+(?:pending\s+)?transactions?\b/.test(normalized)) {
    return { intent: "reject_transaction" };
  }

  return null;
}

function resolveTxnCodeFromContext(message: string, lastTxnCodes: string[] | undefined): string | null {
  if (!lastTxnCodes || lastTxnCodes.length === 0) {
    return null;
  }

  const normalized = message.toLowerCase();
  if (/\b(this|that|it)\s+transaction\b/.test(normalized) && lastTxnCodes.length === 1) {
    return lastTxnCodes[0];
  }

  if (/\b(first|1st)\b/.test(normalized)) {
    return lastTxnCodes[0] || null;
  }

  if (/\b(second|2nd)\b/.test(normalized)) {
    return lastTxnCodes[1] || null;
  }

  if (/\b(third|3rd)\b/.test(normalized)) {
    return lastTxnCodes[2] || null;
  }

  if (/\b(last|latest)\b/.test(normalized)) {
    return lastTxnCodes[lastTxnCodes.length - 1] || null;
  }

  return null;
}

function extractTxnCodesFromResult(intent: string, rawReply: string): string[] {
  try {
    const parsed = JSON.parse(rawReply) as Record<string, unknown>;
    const rows = intent === "list_pending"
      ? (parsed.pending as Record<string, unknown>[] | undefined)
      : intent === "list_recent"
      ? (parsed.recent as Record<string, unknown>[] | undefined)
      : intent === "search"
      ? (parsed.search_results as Record<string, unknown>[] | undefined)
      : undefined;

    if (!Array.isArray(rows)) {
      return [];
    }

    return rows
      .map((row) => String(row.txn_code || "").toUpperCase())
      .filter((code) => /^[A-Z0-9]{4}$/.test(code))
      .slice(0, 20);
  } catch {
    return [];
  }
}

function isCreationTimeQuestion(message: string): boolean {
  return /\b(when|what time|created|creation)\b/i.test(message) && /\b(transaction|txn)\b/i.test(message);
}

function isAllVendorsListQuestion(message: string): boolean {
  return /\b(all\s+vendors|list\s+all\s+vendors|vendor\s+list|list\s+of\s+all\s+vendors)\b/i.test(
    message
  );
}

function extractKindFilterFromMessage(message: string): "payment" | "purchase" | "expense" | null {
  if (/\bpayments?\b/i.test(message)) {
    return "payment";
  }
  if (/\bpurchases?\b/i.test(message)) {
    return "purchase";
  }
  if (/\bexpenses?\b/i.test(message)) {
    return "expense";
  }
  return null;
}

function isKindListReadQuestion(message: string): boolean {
  return /\b(list|show|give|what\s+are|all)\b/i.test(message) && /\b(payments?|purchases?|expenses?)\b/i.test(message);
}

function extractDateRangeFromMessage(message: string): { dateFrom?: string; dateTo?: string } {
  const monthMap: Record<string, number> = {
    january: 0,
    february: 1,
    march: 2,
    april: 3,
    may: 4,
    june: 5,
    july: 6,
    august: 7,
    september: 8,
    october: 9,
    november: 10,
    december: 11,
  };

  const now = new Date();
  const dateRange: { dateFrom?: string; dateTo?: string } = {};
  const fromMatch = message.match(/\bfrom\s+([a-z]+)\s+(\d{1,2})(?:\s*,?\s*(\d{4}))?/i);

  if (fromMatch) {
    const monthName = fromMatch[1].toLowerCase();
    const day = Number(fromMatch[2]);
    const year = fromMatch[3] ? Number(fromMatch[3]) : now.getFullYear();
    const month = monthMap[monthName];

    if (Number.isInteger(month) && Number.isFinite(day) && day >= 1 && day <= 31) {
      const fromDate = new Date(year, month, day, 0, 0, 0, 0);
      dateRange.dateFrom = fromDate.toISOString();
    }
  }

  if (/\b(till\s+today|to\s+till\s+date|till\s+date|to\s+today|until\s+today)\b/i.test(message)) {
    dateRange.dateTo = now.toISOString();
  }

  return dateRange;
}

function extractTransactionDateFromMessage(message: string): string | null {
  const monthMap: Record<string, number> = {
    january: 0,
    february: 1,
    march: 2,
    april: 3,
    may: 4,
    june: 5,
    july: 6,
    august: 7,
    september: 8,
    october: 9,
    november: 10,
    december: 11,
  };

  const now = new Date();

  if (/\btoday\b/i.test(message)) {
    return now.toISOString();
  }

  if (/\byesterday\b/i.test(message)) {
    return new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString();
  }

  const isoMatch = message.match(/\b(\d{4}-\d{2}-\d{2})\b/);
  if (isoMatch) {
    const date = new Date(`${isoMatch[1]}T00:00:00.000Z`);
    if (!Number.isNaN(date.getTime())) {
      return date.toISOString();
    }
  }

  const slashMatch = message.match(/\b(\d{1,2})[\/-](\d{1,2})[\/-](\d{2,4})\b/);
  if (slashMatch) {
    const day = Number(slashMatch[1]);
    const month = Number(slashMatch[2]) - 1;
    const rawYear = Number(slashMatch[3]);
    const year = rawYear < 100 ? 2000 + rawYear : rawYear;
    const date = new Date(year, month, day, 0, 0, 0, 0);
    if (!Number.isNaN(date.getTime())) {
      return date.toISOString();
    }
  }

  const monthDayMatch = message.match(/\b([a-z]+)\s+(\d{1,2})(?:\s*,?\s*(\d{4}))?\b/i);
  if (monthDayMatch) {
    const month = monthMap[monthDayMatch[1].toLowerCase()];
    const day = Number(monthDayMatch[2]);
    const year = monthDayMatch[3] ? Number(monthDayMatch[3]) : now.getFullYear();
    if (Number.isInteger(month) && day >= 1 && day <= 31) {
      const date = new Date(year, month, day, 0, 0, 0, 0);
      if (!Number.isNaN(date.getTime())) {
        return date.toISOString();
      }
    }
  }

  return null;
}

function extractNumberedChoice(message: string): 1 | 2 | null {
  const trimmed = message.trim().toLowerCase();
  if (/^1(?:[.)\s]|$)/.test(trimmed)) {
    return 1;
  }
  if (/^2(?:[.)\s]|$)/.test(trimmed)) {
    return 2;
  }
  return null;
}

function extractVendorNameLoose(message: string): string | null {
  const cleaned = message
    .replace(/\b(today|yesterday|tomorrow|amount|amt|rupees?|rs\.?|cash|upi|bank|card|cheque|neft|rtgs|imps)\b/gi, "")
    .replace(/[^a-z0-9 .&'_-]/gi, " ")
    .replace(/\b\d+(?:\.\d+)?\b/g, " ")
    .replace(/\s{2,}/g, " ")
    .trim();

  if (!cleaned || cleaned.length < 2) {
    return null;
  }
  return cleaned;
}

function isAffirmativeVendorReply(message: string): boolean {
  return /^(yes|yes please|yeah|yep|yup|ok|okay|sure|go ahead|please do|use it|use that|that one|this one|first|first one|1)$/i.test(
    message.trim()
  );
}

function resolveSuggestedVendorReply(message: string, suggestions: string[] | undefined): string | null {
  const cleanedSuggestions = Array.isArray(suggestions)
    ? suggestions.map((value) => String(value || "").trim()).filter(Boolean)
    : [];

  if (cleanedSuggestions.length === 0) {
    return null;
  }

  const trimmed = message.trim();
  if (!trimmed) {
    return null;
  }

  if (isAffirmativeVendorReply(trimmed)) {
    return cleanedSuggestions[0];
  }

  if (/^(second|second one|2)$/i.test(trimmed)) {
    return cleanedSuggestions[1] || null;
  }

  if (/^(third|third one|3)$/i.test(trimmed)) {
    return cleanedSuggestions[2] || null;
  }

  for (const suggestion of cleanedSuggestions) {
    const exact = new RegExp(`^\\s*${escapeRegex(suggestion)}\\s*$`, "i");
    const goWith = new RegExp(`\\b(?:go with|use|select|choose|pick)\\s+${escapeRegex(suggestion)}\\b`, "i");
    if (exact.test(trimmed) || goWith.test(trimmed)) {
      return suggestion;
    }
  }

  return null;
}

function mergePayload(
  base: Record<string, unknown>,
  ...incoming: Array<Record<string, unknown> | undefined>
): Record<string, unknown> {
  const merged: Record<string, unknown> = { ...base };

  for (const source of incoming) {
    if (!source) {
      continue;
    }

    for (const [key, value] of Object.entries(source)) {
      if (typeof value === "undefined" || value === null) {
        continue;
      }
      if (typeof value === "string" && value.trim() === "") {
        continue;
      }
      merged[key] = value;
    }
  }

  return merged;
}

function normalizeSimpleItemEntry(
  message: string,
  payload: Record<string, unknown>
): Record<string, unknown> {
  const nextPayload = { ...payload };
  const trimmed = message.trim();
  const hasExplicitVendorPhrase = /\b(from|to|vendor|supplier|shop)\b/i.test(trimmed);
  const simplePrefixMatch = trimmed.match(/^([a-z][a-z0-9 .&'_-]{1,80})\s+\d+(?:\.\d+)?$/i);
  const simpleSuffixMatch = trimmed.match(/^\d+(?:\.\d+)?\s+([a-z][a-z0-9 .&'_-]{1,80})$/i);
  const itemCandidate = (simplePrefixMatch?.[1] || simpleSuffixMatch?.[1] || "")
    .replace(/[.,;!?]+$/g, "")
    .replace(/\s{2,}/g, " ")
    .trim();

  if (!itemCandidate || hasExplicitVendorPhrase) {
    return nextPayload;
  }

  const existingVendorName = String(nextPayload.vendorName || "").trim();
  const existingNote = String(nextPayload.note || "").trim();
  const normalizedItem = itemCandidate.toLowerCase();

  if (
    existingVendorName &&
    existingVendorName.toLowerCase() === normalizedItem
  ) {
    delete nextPayload.vendorName;
    nextPayload.missingVendor = true;
    if (!existingNote) {
      nextPayload.note = itemCandidate;
    }
  }

  if (!existingVendorName) {
    nextPayload.missingVendor = true;
    if (!existingNote) {
      nextPayload.note = itemCandidate;
    }
  }

  return nextPayload;
}

async function canonicalizeVendorInPayload(payload: Record<string, unknown>): Promise<{
  payload: Record<string, unknown>;
  vendorMissing: boolean;
  followupMessage?: string;
  suggestedVendorNames?: string[];
}> {
  const nextPayload = { ...payload };
  const candidate = String(nextPayload.vendorName || "").trim();
  const candidateLower = candidate.toLowerCase();

  if (!candidate) {
    return {
      payload: nextPayload,
      vendorMissing: true,
      followupMessage:
        "I captured the amount/details. Please share vendor name from Vendor Master.",
    };
  }

  // Product/category words are not vendor names. Keep them as note context.
  if (VENDOR_TYPE_KEYWORDS[candidateLower]) {
    delete nextPayload.vendorName;
    if (!String(nextPayload.note || "").trim()) {
      nextPayload.note = candidate;
    }
    return {
      payload: nextPayload,
      vendorMissing: true,
      followupMessage:
        "I understood this as product/category, not vendor. Please share vendor name from Vendor Master.",
    };
  }

  const resolved = await resolveActiveVendor(candidate);
  if (!resolved.ok) {
    delete nextPayload.vendorName;
    if (!resolved.suggestions?.length && !String(nextPayload.note || "").trim() && candidate) {
      nextPayload.note = candidate;
    }
    return {
      payload: nextPayload,
      vendorMissing: true,
      followupMessage:
        resolved.suggestions && resolved.suggestions.length > 0
          ? resolved.message
          : "I captured the amount/details. Please share vendor name from Vendor Master.",
      suggestedVendorNames: resolved.suggestions,
    };
  }

  nextPayload.vendorName = resolved.vendorName;
  return {
    payload: nextPayload,
    vendorMissing: false,
  };
}

function extractCreateFieldHintsFromMessage(message: string): Record<string, unknown> {
  const hints: Record<string, unknown> = {};
  const lower = message.toLowerCase();

  // Example forms:
  // - "ravi purchase 7800 cash" => vendor=ravi, amount=7800, mode=cash
  // - "profile purchase 18000 credit prasad" => note=profile, amount=18000, mode=credit, vendor=prasad
  const vendorPurchaseAmountModeMatch = message.match(
    /^\s*([a-z][a-z0-9 .&'_-]{1,80}?)\s+(?:purchase|purchased|buy|bought)\s+([0-9]+(?:\.[0-9]+)?)\s+(upi|cash|bank|card|cheque|neft|rtgs|imps|credit)\b(?:\s+([a-z][a-z0-9 .&'_-]{1,80}))?\s*$/i
  );
  if (vendorPurchaseAmountModeMatch) {
    const prefixCandidate = vendorPurchaseAmountModeMatch[1].trim();
    const amountCandidate = Number(vendorPurchaseAmountModeMatch[2]);
    const modeCandidate = vendorPurchaseAmountModeMatch[3].trim().toLowerCase();
    const trailingVendorCandidate = String(vendorPurchaseAmountModeMatch[4] || "").trim();
    const vendorCandidate = trailingVendorCandidate || prefixCandidate;
    const vendorLower = vendorCandidate.toLowerCase();
    const vendorFirstToken = vendorLower.split(/\s+/)[0] || vendorLower;

    if (
      !VENDOR_TYPE_KEYWORDS[vendorLower] &&
      !NON_VENDOR_FIRST_TOKENS.has(vendorFirstToken) &&
      Number.isFinite(amountCandidate) &&
      amountCandidate > 0
    ) {
      hints.vendorName = vendorCandidate;
      hints.amount = amountCandidate;
      hints.mode = modeCandidate === "credit" ? "credit" : modeCandidate;
      if (trailingVendorCandidate && !String(hints.note || "").trim()) {
        hints.note = prefixCandidate;
      }
    }
  }

  const vendorAmountRemarkMatch = message.match(
    /^\s*([a-z][a-z0-9 .&'_-]{1,80}?)\s+([0-9]+(?:\.[0-9]+)?)\s+([a-z][a-z0-9 .&'_-]{1,80})\s*$/i
  );
  if (vendorAmountRemarkMatch && !hints.vendorName) {
    const vendorCandidate = vendorAmountRemarkMatch[1].trim();
    const amountCandidate = Number(vendorAmountRemarkMatch[2]);
    const remarkCandidate = vendorAmountRemarkMatch[3].trim();
    const vendorLower = vendorCandidate.toLowerCase();
    const vendorTokens = vendorLower.split(/\s+/).filter(Boolean);
    const vendorFirstToken = vendorTokens[0] || vendorLower;
    const hasNonVendorToken = vendorTokens.some((token) => NON_VENDOR_FIRST_TOKENS.has(token));

    // Guard against sentences like "Purchased 1999 accessories" where the first word is not a vendor name.
    if (
      NON_VENDOR_FIRST_TOKENS.has(vendorFirstToken) ||
      hasNonVendorToken ||
      VENDOR_TYPE_KEYWORDS[vendorLower]
    ) {
      if (Number.isFinite(amountCandidate) && amountCandidate > 0) {
        hints.amount = amountCandidate;
        hints.missingVendor = true;
        if (!String(hints.note || "").trim()) {
          hints.note = `${vendorCandidate} ${remarkCandidate}`.trim();
        }
      }
    } else if (Number.isFinite(amountCandidate) && amountCandidate > 0) {
      // Example: "prabhakar 1500 plywood" => vendor=prabhakar, note=plywood
      hints.vendorName = vendorCandidate;
      hints.amount = amountCandidate;
      hints.note = remarkCandidate;
    }
  }

  const modeMatch = message.match(/\b(upi|cash|bank|card|cheque|neft|rtgs|imps|credit)\b/i);
  if (modeMatch) {
    hints.mode = modeMatch[1].toLowerCase();
  }

  const amountPatterns = [
    /(?:rs\.?|₹|rupees?)\s*([0-9]+(?:\.[0-9]+)?)/i,
    /([0-9]+(?:\.[0-9]+)?)\s*(?:rs\.?|₹|rupees?)/i,
    /\b(?:purchase|payment|expense)\s+([0-9]+(?:\.[0-9]+)?)/i,
  ];
  for (const pattern of amountPatterns) {
    const amountMatch = message.match(pattern);
    if (amountMatch) {
      hints.amount = Number(amountMatch[1]);
      break;
    }
  }

  if (typeof hints.amount !== "number" || !Number.isFinite(hints.amount)) {
    const genericAmount = message.match(/\b([0-9]{2,}(?:\.[0-9]+)?)\b/);
    if (genericAmount) {
      hints.amount = Number(genericAmount[1]);
    }
  }

  // Check if this message contains a vendor type keyword (like "fruits", "groceries")
  const detectedVendorType = detectVendorTypeFromMessage(message);
  if (detectedVendorType) {
    // Mark that we need to ask for actual vendor name, don't treat the category as vendor name
    hints.awaitingVendorName = true;
    hints.missingVendor = true;
    if (!String(hints.note || "").trim()) {
      hints.note = detectedVendorType;
    }
  }

  const vendorMatch = message.match(/\b(?:from|to)\s+([a-z][a-z0-9 .&'_-]{1,80})/i);
  if (vendorMatch) {
    const extractedMode = typeof hints.mode === "string" ? hints.mode : "";
    let vendorName = vendorMatch[1]
      .replace(/^(?:the\s+)?(?:vendor|supplier|shop|factory)\s+/i, "")
      .replace(/\b(mode\s+is|mode|via|using)\b.*$/i, "")
      .replace(/\b(today|yesterday|tomorrow)\b.*$/i, "")
      .replace(/\b[0-9]+(?:\.[0-9]+)?\b.*$/i, "")
      .replace(/[.,;!?]+$/g, "")
      .replace(/\s{2,}/g, " ")
      .trim();

    // If message ends with a payment mode token (e.g. "a2 traders upi"), drop only that trailing mode word.
    if (extractedMode) {
      const trailingModeRe = new RegExp(`\\s+${extractedMode}\\s*$`, "i");
      vendorName = vendorName.replace(trailingModeRe, "").trim();
    }

    // Don't use vendor name if it's actually a vendor type keyword
    const vendorNameLower = vendorName.toLowerCase();
    if (vendorName && !VENDOR_TYPE_KEYWORDS[vendorNameLower]) {
      hints.vendorName = vendorName;
    }
  }

  if (!hints.vendorName) {
    // Handle patterns like "... credit Prasad" / "... cash Ramesh" at the end.
    const tailModeVendorMatch = message.match(
      /\b(credit|upi|cash|bank|card|cheque|neft|rtgs|imps)\s+([a-z][a-z0-9 .&'_-]{1,80})\s*$/i
    );
    if (tailModeVendorMatch) {
      const extractedMode = tailModeVendorMatch[1].toLowerCase();
      const extractedVendor = tailModeVendorMatch[2].trim();
      const extractedVendorLower = extractedVendor.toLowerCase();
      const extractedVendorFirst = extractedVendorLower.split(/\s+/)[0] || extractedVendorLower;

      if (
        extractedVendor &&
        !VENDOR_TYPE_KEYWORDS[extractedVendorLower] &&
        !NON_VENDOR_FIRST_TOKENS.has(extractedVendorFirst)
      ) {
        hints.mode = hints.mode || extractedMode;
        hints.vendorName = extractedVendor;
      }
    }
  }

  if (!hints.vendorName && typeof hints.amount === "number" && Number.isFinite(hints.amount)) {
    const trailingVendorMatch = message.match(/\b\d+(?:\.\d+)?\b\s+([a-z][a-z0-9 .&'_-]{1,80})$/i);
    if (trailingVendorMatch) {
      const captured = trailingVendorMatch[1]
        .replace(/[.,;!?]+$/g, "")
        .replace(/\s{2,}/g, " ")
        .trim();

      // Handle patterns like "Profile purchase 18000 credit Prasad" (mode + vendor at end).
      const modeVendor = captured.match(/^(credit|upi|cash|bank|card|cheque|neft|rtgs|imps)\s+(.+)$/i);
      if (modeVendor) {
        const extractedMode = modeVendor[1].toLowerCase();
        const extractedVendor = modeVendor[2]
          .replace(/^(?:the\s+)?(?:vendor|supplier|shop|factory)\s+/i, "")
          .trim();
        const extractedVendorLower = extractedVendor.toLowerCase();
        const extractedVendorFirst = extractedVendorLower.split(/\s+/)[0] || extractedVendorLower;
        if (
          extractedVendor &&
          !VENDOR_TYPE_KEYWORDS[extractedVendorLower] &&
          !NON_VENDOR_FIRST_TOKENS.has(extractedVendorFirst)
        ) {
          hints.mode = hints.mode || extractedMode;
          hints.vendorName = extractedVendor;
        }
      } else {
        const hasExplicitVendorKeyword = /\b(from|to)\b/i.test(message);
        if (hasExplicitVendorKeyword) {
          const inferredVendor = captured;
          const inferredVendorLower = inferredVendor.toLowerCase();
          const inferredVendorFirst = inferredVendorLower.split(/\s+/)[0] || inferredVendorLower;
          if (
            inferredVendor &&
            !VENDOR_TYPE_KEYWORDS[inferredVendorLower] &&
            !NON_VENDOR_FIRST_TOKENS.has(inferredVendorFirst)
          ) {
            hints.vendorName = inferredVendor;
          }
        } else {
          hints.missingVendor = true;
          if (!String(hints.note || "").trim()) {
            hints.note = captured;
          }
        }
      }
    }
  }

  // Fallback: preserve free-text remarks around the amount even when misspelled,
  // e.g. "accesroies 1500" or "1500 accesroies".
  if (!String(hints.note || "").trim() && typeof hints.amount === "number" && Number.isFinite(hints.amount)) {
    const prefixAmountMatch = message.match(/^\s*([a-z][a-z0-9 .&'_-]{1,80})\s+\d+(?:\.\d+)?\s*$/i);
    const suffixAmountMatch = message.match(/^\s*\d+(?:\.\d+)?\s+([a-z][a-z0-9 .&'_-]{1,80})\s*$/i);
    const remarkCandidate = (prefixAmountMatch?.[1] || suffixAmountMatch?.[1] || "")
      .replace(/[.,;!?]+$/g, "")
      .replace(/\s{2,}/g, " ")
      .trim();

    if (remarkCandidate) {
      hints.note = remarkCandidate;
    }
  }

  if (Object.keys(hints).length === 0 && /\bmode\b|\bupi\b|\bcash\b|\bbank\b/i.test(lower)) {
    const modeFallback = lower.match(/\b(upi|cash|bank|card|cheque|neft|rtgs|imps)\b/);
    if (modeFallback) {
      hints.mode = modeFallback[1];
    }
  }

  const transactionDate = extractTransactionDateFromMessage(message);
  if (transactionDate) {
    hints.transactionDate = transactionDate;
  }

  return hints;
}

function extractKindChoice(message: string): "payment" | "credit" | null {
  const normalized = message.toLowerCase();

  if (/\b(payment|paid|paying|pay)\b/.test(normalized)) {
    return "payment";
  }

  if (/\b(credit|pending\s+payment|purchase|buy|bought|bill|order)\b/.test(normalized)) {
    return "credit";
  }

  return null;
}

function isPotentialOrderEntryMessage(message: string, hints: Record<string, unknown>): boolean {
  const amount = Number(hints.amount || 0);
  const hasAmount = Number.isFinite(amount) && amount > 0;
  if (!hasAmount) {
    return false;
  }

  if (extractKindChoice(message)) {
    return true;
  }

  // Treat simple "item amount" or "vendor amount remark" messages as entries even without explicit keywords.
  // Examples: "sarees 1500", "1500 plywood", "prabhakar 1500 plywood".
  if (
    /^\s*[a-z][a-z0-9 .&'_-]{1,80}\s+[0-9]+(?:\.[0-9]+)?\s*$/i.test(message) ||
    /^\s*[0-9]+(?:\.[0-9]+)?\s+[a-z][a-z0-9 .&'_-]{1,80}\s*$/i.test(message) ||
    /^\s*[a-z][a-z0-9 .&'_-]{1,80}\s+[0-9]+(?:\.[0-9]+)?\s+[a-z][a-z0-9 .&'_-]{1,120}\s*$/i.test(message)
  ) {
    return true;
  }

  // Check for explicit order keywords OR vendor type keywords
  const hasOrderKeywords = /\b(order|purchase|purchased|bought|material|materials|goods|bill|entry|supplier|vendor|shop|factory|profile|fittings|hardware|accessories|taken|from)\b/i.test(
    message
  );
  
  const hasVendorTypeKeyword = detectVendorTypeFromMessage(message) !== null;

  return hasOrderKeywords || hasVendorTypeKeyword;
}

function isLowSignalMessage(message: string): boolean {
  const trimmed = message.trim();
  if (!trimmed) {
    return true;
  }

  // Ignore inputs that contain no letters or digits, like ".....", "???", or only symbols.
  if (!/[a-z0-9]/i.test(trimmed)) {
    return true;
  }

  // Ignore repeated single-character spam, e.g. "aaaaaa" or "111111".
  const compact = trimmed.replace(/\s+/g, "");
  if (compact.length >= 5 && /^([a-z0-9])\1+$/i.test(compact)) {
    return true;
  }

  return false;
}

function isSupportedCreateCommandMessage(message: string): boolean {
  return /\b(purchase|payment|pay|paid|buy|bought)\b/i.test(message);
}

function isPendingTransactionsQuestion(message: string): boolean {
  const normalized = message.trim().toLowerCase();
  return (
    /\b(pending|pendig|waiting|awaiting)\b/.test(normalized) &&
    /\b(transaction|transactions|approval|approvals|entries)\b/.test(normalized)
  ) ||
  /\bwhat(?:'s| is)?\s+(?:pending|pendig)\b/.test(normalized) ||
  /\bshow\b.*\b(?:pending|pendig)\b/.test(normalized);
}

function isAllTransactionsQuestion(message: string): boolean {
  const normalized = message.trim().toLowerCase();
  return (
    /\b(all|recent|latest)\s+transactions\b/.test(normalized) ||
    /\bshow me everything\b/.test(normalized) ||
    /\blist all\b/.test(normalized) ||
    /\bshow all entries\b/.test(normalized)
  );
}

function isSummaryQuestion(message: string): boolean {
  const normalized = message.trim().toLowerCase();
  return (
    /\bsummary\b/.test(normalized) ||
    /\b(total|totals|count|counts)\b/.test(normalized) ||
    /\bhow much\b.*\b(spent|paid|purchased)\b/.test(normalized) ||
    /\bgive me\b.*\bsummary\b/.test(normalized)
  );
}

function isDraftInterruptionMessage(message: string): boolean {
  return (
    isPendingTransactionsQuestion(message) ||
    isAllTransactionsQuestion(message) ||
    isSummaryQuestion(message) ||
    Boolean(extractLedgerVendorFromMessage(message)) ||
    Boolean(parseVendorMasterCommand(message)) ||
    Boolean(extractApproveRejectIntent(message)) ||
    Boolean(extractBulkApproveRejectIntent(message)) ||
    /\b(search|find|lookup|look up)\b/i.test(message)
  );
}

const REMARK_PREVIEW_CORRECTIONS: Record<string, string> = {
  accesroies: "accessories",
  accesories: "accessories",
  accersories: "accessories",
  acccesories: "accessories",
};

function normalizeRemarkForPreview(note: string): string {
  let normalized = note.trim().replace(/\s{2,}/g, " ");

  for (const [wrong, corrected] of Object.entries(REMARK_PREVIEW_CORRECTIONS)) {
    const pattern = new RegExp(`\\b${wrong}\\b`, "gi");
    normalized = normalized.replace(pattern, corrected);
  }

  return normalized;
}

function formatActionPreview(
  intent: "create_transaction" | "approve_transaction" | "reject_transaction" | "edit_at_approval",
  payload: Record<string, unknown>
): string {
  if (intent === "create_transaction") {
    const kind = String(payload.kind || "N/A").toLowerCase();
    const amount = Number(payload.amount || 0);
    const vendorName = String(payload.vendorName || "unknown vendor");
    const mode = String(payload.mode || "credit");
    const transactionDate = payload.transactionDate ? String(payload.transactionDate) : "now";
    const note = String(payload.note || payload.remarks || payload.product || "").trim();
    const previewNote = note ? normalizeRemarkForPreview(note) : "";

    return [
      "Entry preview:",
      `kind: ${kind}`,
      `amount: ${amount}`,
      `vendor: ${vendorName}`,
      `mode: ${mode}`,
      `transactionDate: ${transactionDate}`,
      ...(previewNote ? [`remarks: ${previewNote}`] : []),
    ].join("\n");
  }

  if (intent === "approve_transaction" || intent === "reject_transaction") {
    if (Array.isArray(payload.ids) && payload.ids.length > 0) {
      return [
        "Action preview:",
        `intent: ${intent}`,
        `bulk: true`,
        `ids: ${payload.ids.map((id) => String(id)).join(", ")}`,
      ].join("\n");
    }

    return [
      "Action preview:",
      `intent: ${intent}`,
      `id: ${String(payload.id || "N/A")}`,
    ].join("\n");
  }

  return [
    "Action preview:",
    `intent: ${intent}`,
    `id: ${String(payload.id || "N/A")}`,
    `updates: ${JSON.stringify(payload.updates || {})}`,
  ].join("\n");
}

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as { message?: string; sessionId?: string };
    const message = (body.message || "").trim();
    const sessionId = (body.sessionId || "default").trim();

    if (!message) {
      return NextResponse.json({ error: "message is required" }, { status: 400 });
    }

    const loginCommand = parseLoginCommand(message);
    if (loginCommand) {
      const { username, password } = loginCommand;

      if (!ALLOWED_USERS.has(username) || password !== username) {
        return NextResponse.json({ reply: "Login failed. Use: login gopi gopi (or father/father, brother/brother)." }, { status: 401 });
      }

      await clearPendingAction(sessionId);
      await clearDraftWriteAction(sessionId);
      await setSessionActor(sessionId, username);
      return NextResponse.json({ reply: `Login successful. You are now logged in as ${username}.` });
    }

    if (isLogoutMessage(message)) {
      await clearSessionActor(sessionId);
      await clearPendingAction(sessionId);
      return NextResponse.json({ reply: "Logged out successfully. Please login again to continue." });
    }

    const sessionRow = await getSessionRow(sessionId);
    const actor = sessionRow?.actor || null;
    if (!actor) {
      if (isTransactionRelatedMessage(message)) {
        return NextResponse.json(
          {
            reply:
              "Login is mandatory for transaction-related requests. Example: login gopi gopi. Available users: gopi, father, brother.",
          },
          { status: 401 }
        );
      }

      return NextResponse.json({ reply: getSimpleChatReply(message) });
    }

    const vendorCommand = parseVendorMasterCommand(message);
    if (vendorCommand) {
      const reply = await handleVendorMasterCommand(vendorCommand, actor);
      return NextResponse.json({ reply });
    }

    const pending = await getPendingAction(sessionId);

    if (pending && isConfirmMessage(message)) {
      try {
        const result = await executePendingAction(pending, actor);
        await clearPendingAction(sessionId);
        await clearDraftWriteAction(sessionId);
        return NextResponse.json({
          reply: `Confirmed and executed. ${result.reply}`,
          executed: true,
          data: result.data,
        });
      } catch (error) {
        const detail = (error as Error).message;

        if (detail.startsWith("DUPLICATE_EXACT:")) {
          await clearPendingAction(sessionId);
          return NextResponse.json(
            {
              reply: detail.replace("DUPLICATE_EXACT:", "").trim(),
            },
            { status: 409 }
          );
        }

        if (detail.startsWith("DUPLICATE_POSSIBLE:")) {
          await savePendingAction(sessionId, {
            ...pending,
            actionPayload: {
              ...pending.actionPayload,
              forceDuplicateConfirm: true,
            },
          });

          return NextResponse.json(
            {
              reply:
                `${detail.replace("DUPLICATE_POSSIBLE:", "").trim()}\nPlease reply CONFIRM again to proceed with second-time entry, or CANCEL to stop.`,
              requiresConfirmation: true,
            },
            { status: 409 }
          );
        }

        if (detail.includes("Missing or invalid fields for create_transaction")) {
          await clearPendingAction(sessionId);
          return NextResponse.json(
            {
              reply:
                "The pending action did not have enough fields, so it was discarded. Please send a complete command with kind, amount, mode, vendorName, and for payments include transaction date.",
            },
            { status: 400 }
          );
        }

        if (detail.startsWith("VENDOR_MASTER:")) {
          return NextResponse.json(
            {
              reply: detail.replace("VENDOR_MASTER:", "").trim(),
            },
            { status: 400 }
          );
        }

        return NextResponse.json(
          { reply: `Confirmation received, but execution failed: ${detail}` },
          { status: 400 }
        );
      }
    }

    if (pending && isCancelMessage(message)) {
      await clearPendingAction(sessionId);
      await clearDraftWriteAction(sessionId);
      return NextResponse.json({ reply: "Pending action canceled. No changes were made." });
    }

    const awaitingSuggestedVendorReply = Boolean(
      sessionRow?.draftWriteAction?.intent === "create_transaction" &&
        sessionRow?.draftWriteAction?.actionPayload?.awaitingVendorName &&
        Array.isArray(sessionRow?.draftWriteAction?.actionPayload?.suggestedVendorNames) &&
        sessionRow?.draftWriteAction?.actionPayload?.suggestedVendorNames.length
    );

    if (!pending && !awaitingSuggestedVendorReply && (isConfirmMessage(message) || isCancelMessage(message))) {
      return NextResponse.json({
        reply:
          "I could not find anything pending to confirm right now. Please send your entry again and I will help you step by step.",
      });
    }

    // Handle "describe changes" - when user wants to update pending action fields
    if (pending && pending.intent === "create_transaction" && isDescribeChangesMessage(message)) {
      const hintedUpdates = extractCreateFieldHintsFromMessage(message);
      
      // Only proceed if user provided actual updates
      if (Object.keys(hintedUpdates).length > 0) {
        const mergedPayload = mergePayload(pending.actionPayload, hintedUpdates);
        const vendorResolution = await canonicalizeVendorInPayload(mergedPayload);
        const updatedPayload = vendorResolution.payload;

        if (vendorResolution.vendorMissing) {
          await savePendingAction(sessionId, {
            intent: "create_transaction",
            actionPayload: {
              ...updatedPayload,
              awaitingVendorName: true,
              suggestedVendorNames: vendorResolution.suggestedVendorNames || [],
            },
            requestedBy: pending.requestedBy,
          });
          return NextResponse.json({
            reply:
              vendorResolution.followupMessage ||
              "Please provide vendor name from Vendor Master.",
            requiresConfirmation: false,
          });
        }
        
        // Validate the updated transaction
        const validation = validateWriteAction("create_transaction", updatedPayload);
        if (!validation.valid) {
          // Store partial updates and return validation message
          await savePendingAction(sessionId, {
            intent: "create_transaction",
            actionPayload: updatedPayload,
            requestedBy: pending.requestedBy,
          });
          return NextResponse.json({
            reply: validation.message,
            requiresConfirmation: true,
          });
        }

        // Validation passed, save the updated action
        await savePendingAction(sessionId, {
          intent: "create_transaction",
          actionPayload: updatedPayload,
          requestedBy: pending.requestedBy,
        });

        const previewText = formatActionPreview("create_transaction", updatedPayload);
        return NextResponse.json({
          reply: `Updated. Here's the new preview:\n\n${previewText}\n\nReply CONFIRM to proceed, CANCEL to discard, or describe more changes.`,
          requiresConfirmation: true,
          proposedAction: {
            intent: "create_transaction",
            payload: updatedPayload,
          },
        });
      }
    }

    if (pending) {
      const pendingPreview = formatActionPreview(pending.intent, pending.actionPayload);
      return NextResponse.json({
        reply:
          `⚠️ You left a transaction unattended. First complete this to start a new entry:\n\n${pendingPreview}\n\nReply CONFIRM to proceed, CANCEL to discard, or provide new details to modify.`,
        pendingAction: pending,
        requiresConfirmation: true,
      });
    }

    if (sessionRow?.draftWriteAction && isCancelMessage(message)) {
      await clearDraftWriteAction(sessionId);
      return NextResponse.json({ reply: "Draft action canceled. Start a new request any time." });
    }

    if (sessionRow?.draftWriteAction && isGreetingMessage(message)) {
      return NextResponse.json({
        reply:
          "Hi. I still have your in-progress transaction draft. Share the missing details, or type CANCEL to discard it.",
      });
    }

    if (sessionRow?.draftWriteAction && isDraftInterruptionMessage(message)) {
      return NextResponse.json({
        reply:
          "You still have an in-progress transaction draft. Please complete it, or type CANCEL to discard it before asking for another action.",
      });
    }

    if (
      sessionRow?.draftWriteAction?.intent === "create_transaction" &&
      Boolean(sessionRow.draftWriteAction.actionPayload.awaitingCashChoice)
    ) {
      const choice = extractNumberedChoice(message);
      if (!choice) {
        return NextResponse.json({
          reply: "Please choose one option: 1 for Cash payment, 2 for Cash purchase.",
        });
      }

      const draftPayload = { ...sessionRow.draftWriteAction.actionPayload };
      delete draftPayload.awaitingCashChoice;

      const classifiedPayload =
        choice === 1
          ? mergePayload(draftPayload, { kind: "payment", mode: "cash" })
          : mergePayload(draftPayload, { kind: "purchase", mode: "cash" });

      await setSessionContext(sessionId, {
        draftWriteAction: {
          intent: "create_transaction",
          actionPayload: {
            ...classifiedPayload,
            awaitingVendorName: !classifiedPayload.vendorName,
            awaitingAmount:
              !(typeof classifiedPayload.amount === "number" && Number.isFinite(Number(classifiedPayload.amount)) && Number(classifiedPayload.amount) > 0),
          },
        },
      });

      if (!classifiedPayload.vendorName) {
        return NextResponse.json({
          reply: "Got it. Please tell me the vendor name.",
        });
      }

      if (!(typeof classifiedPayload.amount === "number" && Number.isFinite(Number(classifiedPayload.amount)) && Number(classifiedPayload.amount) > 0)) {
        return NextResponse.json({
          reply: "Noted. Please tell me the amount.",
        });
      }

      const vendorResolution = await canonicalizeVendorInPayload(classifiedPayload);
      const validatedPayload = vendorResolution.payload;
      if (vendorResolution.vendorMissing) {
        await setSessionContext(sessionId, {
          draftWriteAction: {
            intent: "create_transaction",
            actionPayload: {
              ...validatedPayload,
              awaitingVendorName: true,
              suggestedVendorNames: vendorResolution.suggestedVendorNames || [],
            },
          },
        });
        return NextResponse.json({
          reply:
            vendorResolution.followupMessage ||
            "Please provide vendor name from Vendor Master.",
        });
      }

      const validation = validateWriteAction("create_transaction", validatedPayload);
      if (!validation.valid) {
        await setSessionContext(sessionId, {
          draftWriteAction: {
            intent: "create_transaction",
            actionPayload: validatedPayload,
          },
        });
        return NextResponse.json({ reply: validation.message, requiresConfirmation: false });
      }

      await clearDraftWriteAction(sessionId);
      await savePendingAction(sessionId, {
        intent: "create_transaction",
        actionPayload: validatedPayload,
        requestedBy: actor,
      });

      const previewText = formatActionPreview("create_transaction", validatedPayload);
      return NextResponse.json({
        reply: `I prepared the action. Reply CONFIRM to proceed, CANCEL to discard, or describe changes.\n\n${previewText}`,
        requiresConfirmation: true,
        proposedAction: {
          intent: "create_transaction",
          payload: validatedPayload,
        },
      });
    }

    if (
      sessionRow?.draftWriteAction?.intent === "create_transaction" &&
      Boolean(sessionRow.draftWriteAction.actionPayload.awaitingVendorName)
    ) {
      const draftPayload = { ...sessionRow.draftWriteAction.actionPayload };
      const vendorName =
        resolveSuggestedVendorReply(
          message,
          Array.isArray(draftPayload.suggestedVendorNames)
            ? (draftPayload.suggestedVendorNames as string[])
            : undefined
        ) || extractVendorNameLoose(message);
      if (!vendorName) {
        return NextResponse.json({
          reply: "Please share a valid vendor name from Vendor Master.",
        });
      }

      const vendorResolution = await canonicalizeVendorInPayload(mergePayload(draftPayload, { vendorName }));
      if (vendorResolution.vendorMissing) {
        await setSessionContext(sessionId, {
          draftWriteAction: {
            intent: "create_transaction",
            actionPayload: {
              ...draftPayload,
              awaitingVendorName: true,
              suggestedVendorNames: vendorResolution.suggestedVendorNames || [],
            },
          },
        });
        return NextResponse.json({
          reply:
            vendorResolution.followupMessage ||
            `Vendor "${vendorName}" is not in active Vendor Master. Please select a valid vendor.`,
        });
      }

      delete vendorResolution.payload.awaitingVendorName;
      delete vendorResolution.payload.suggestedVendorNames;
      const nextPayload = vendorResolution.payload;

      await setSessionContext(sessionId, {
        draftWriteAction: {
          intent: "create_transaction",
          actionPayload: {
            ...nextPayload,
            awaitingAmount:
              !(typeof nextPayload.amount === "number" && Number.isFinite(Number(nextPayload.amount)) && Number(nextPayload.amount) > 0),
          },
        },
      });

      if (!(typeof nextPayload.amount === "number" && Number.isFinite(Number(nextPayload.amount)) && Number(nextPayload.amount) > 0)) {
        return NextResponse.json({ reply: "Got it. Please tell me the amount." });
      }

      const validatedPayload = nextPayload;

      const validation = validateWriteAction("create_transaction", validatedPayload);
      if (!validation.valid) {
        await setSessionContext(sessionId, {
          draftWriteAction: {
            intent: "create_transaction",
            actionPayload: validatedPayload,
          },
        });
        return NextResponse.json({ reply: validation.message, requiresConfirmation: false });
      }

      await clearDraftWriteAction(sessionId);
      await savePendingAction(sessionId, {
        intent: "create_transaction",
        actionPayload: validatedPayload,
        requestedBy: actor,
      });

      const previewText = formatActionPreview("create_transaction", validatedPayload);
      return NextResponse.json({
        reply: `I prepared the action. Reply CONFIRM to proceed, CANCEL to discard, or describe changes.\n\n${previewText}`,
        requiresConfirmation: true,
        proposedAction: {
          intent: "create_transaction",
          payload: validatedPayload,
        },
      });
    }

    if (
      sessionRow?.draftWriteAction?.intent === "create_transaction" &&
      Boolean(sessionRow.draftWriteAction.actionPayload.awaitingAmount)
    ) {
      const hinted = extractCreateFieldHintsFromMessage(message);
      const amount = Number(hinted.amount || 0);
      if (!Number.isFinite(amount) || amount <= 0) {
        return NextResponse.json({
          reply: "Please share a valid amount (example: 1200).",
        });
      }

      const draftPayload = { ...sessionRow.draftWriteAction.actionPayload };
      delete draftPayload.awaitingAmount;

      const noteHints: Record<string, unknown> = {};
      for (const key of ["note", "remarks", "remark", "product", "details", "mode", "transactionDate"]) {
        const value = hinted[key];
        if (typeof value !== "undefined") {
          noteHints[key] = value;
        }
      }

      const nextPayload = mergePayload(draftPayload, noteHints, { amount });
      const validation = validateWriteAction("create_transaction", nextPayload);
      if (!validation.valid) {
        await setSessionContext(sessionId, {
          draftWriteAction: {
            intent: "create_transaction",
            actionPayload: nextPayload,
          },
        });
        return NextResponse.json({ reply: validation.message, requiresConfirmation: false });
      }

      await clearDraftWriteAction(sessionId);
      await savePendingAction(sessionId, {
        intent: "create_transaction",
        actionPayload: nextPayload,
        requestedBy: actor,
      });

      const previewText = formatActionPreview("create_transaction", nextPayload);
      return NextResponse.json({
        reply: `I prepared the action. Reply CONFIRM to proceed, CANCEL to discard, or describe changes.\n\n${previewText}`,
        requiresConfirmation: true,
        proposedAction: {
          intent: "create_transaction",
          payload: nextPayload,
        },
      });
    }

    if (
      sessionRow?.draftWriteAction?.intent === "create_transaction" &&
      Boolean(sessionRow.draftWriteAction.actionPayload.awaitingKindChoice)
    ) {
      const choice = extractKindChoice(message);
      if (!choice) {
        return NextResponse.json({
          reply: "Please reply with one word only: payment or credit.",
        });
      }

      const draftPayload = { ...sessionRow.draftWriteAction.actionPayload };
      delete draftPayload.awaitingKindChoice;

      const classifiedPayload = mergePayload(draftPayload, {
        kind: choice === "payment" ? "payment" : "purchase",
        mode:
          choice === "payment"
            ? String(draftPayload.mode || "cash").toLowerCase()
            : String(draftPayload.mode || "credit").toLowerCase(),
      });

      const vendorResolution = await canonicalizeVendorInPayload(classifiedPayload);
      const validatedPayload = vendorResolution.payload;
      if (vendorResolution.vendorMissing) {
        await setSessionContext(sessionId, {
          draftWriteAction: {
            intent: "create_transaction",
            actionPayload: {
              ...validatedPayload,
              awaitingVendorName: true,
              suggestedVendorNames: vendorResolution.suggestedVendorNames || [],
            },
          },
        });
        return NextResponse.json({
          reply:
            vendorResolution.followupMessage ||
            "Please provide vendor name from Vendor Master.",
        });
      }

      const validation = validateWriteAction("create_transaction", validatedPayload);
      if (!validation.valid) {
        await setSessionContext(sessionId, {
          draftWriteAction: {
            intent: "create_transaction",
            actionPayload: { ...validatedPayload, awaitingKindChoice: true },
          },
        });
        return NextResponse.json({ reply: validation.message, requiresConfirmation: false });
      }

      await clearDraftWriteAction(sessionId);
      await savePendingAction(sessionId, {
        intent: "create_transaction",
        actionPayload: validatedPayload,
        requestedBy: actor,
      });

      const previewText = formatActionPreview("create_transaction", validatedPayload);
      return NextResponse.json({
        reply: `I prepared the action. Reply CONFIRM to proceed, CANCEL to discard, or describe changes.\n\n${previewText}`,
        requiresConfirmation: true,
        proposedAction: {
          intent: "create_transaction",
          payload: validatedPayload,
        },
      });
    }

    if (/^\s*cash\s*$/i.test(message)) {
      await setSessionContext(sessionId, {
        draftWriteAction: {
          intent: "create_transaction",
          actionPayload: {
            mode: "cash",
            awaitingCashChoice: true,
          },
        },
      });

      return NextResponse.json({
        reply:
          "What should I record?\n1. Cash payment\n2. Cash purchase\n\nReply with 1 or 2.",
        requiresConfirmation: false,
      });
    }

    if (isLowSignalMessage(message)) {
      return NextResponse.json({
        reply:
          "I could not understand that message. Please send a clear command such as: summary, list pending, ledger for <vendor>, or approve <ID>.",
      });
    }

    const hintedCreateFields = extractCreateFieldHintsFromMessage(message);
    if (isPotentialOrderEntryMessage(message, hintedCreateFields)) {
      const geminiHints = await askGeminiForCreateHints(message);
      const normalizedGeminiHints: Record<string, unknown> = geminiHints
        ? {
            ...(typeof geminiHints.kind === "string" ? { kind: geminiHints.kind } : {}),
            ...(typeof geminiHints.amount === "number" ? { amount: geminiHints.amount } : {}),
            ...(typeof geminiHints.vendorName === "string" ? { vendorName: geminiHints.vendorName } : {}),
            ...(typeof geminiHints.mode === "string" ? { mode: geminiHints.mode.toLowerCase() } : {}),
            ...(typeof geminiHints.transactionDate === "string" ? { transactionDate: geminiHints.transactionDate } : {}),
            ...(typeof geminiHints.product === "string" ? { note: geminiHints.product } : {}),
            ...(geminiHints.vendorMissing ? { missingVendor: true } : {}),
          }
        : {};

      const geminiVendorCandidate = String(normalizedGeminiHints.vendorName || "").trim().toLowerCase();
      if (geminiVendorCandidate && VENDOR_TYPE_KEYWORDS[geminiVendorCandidate]) {
        normalizedGeminiHints.note =
          String(normalizedGeminiHints.note || "").trim() || String(normalizedGeminiHints.vendorName);
        delete normalizedGeminiHints.vendorName;
        normalizedGeminiHints.missingVendor = true;
      }

      const mergedHints = normalizeSimpleItemEntry(
        message,
        mergePayload(normalizedGeminiHints, hintedCreateFields)
      );
      const draftPayload = mergePayload(mergedHints, {
        kind: "purchase",
        mode: String(mergedHints.mode || "credit").toLowerCase(),
        note: String(mergedHints.note || message).trim(),
      });

      const vendorResolution = await canonicalizeVendorInPayload(draftPayload);
      const canonicalPayload = vendorResolution.payload;

      const hasVendor = Boolean(String(canonicalPayload.vendorName || "").trim());
      const hasMissingVendor = Boolean(canonicalPayload.missingVendor) || vendorResolution.vendorMissing;
      const hasAmount = Number.isFinite(Number(canonicalPayload.amount || 0)) && Number(canonicalPayload.amount || 0) > 0;

      if (!hasVendor || !hasAmount || hasMissingVendor) {
        delete canonicalPayload.missingVendor;
        await setSessionContext(sessionId, {
          draftWriteAction: {
            intent: "create_transaction",
            actionPayload: {
              ...canonicalPayload,
              awaitingVendorName: !hasVendor || hasMissingVendor,
              awaitingAmount: !hasAmount,
              suggestedVendorNames: vendorResolution.suggestedVendorNames || [],
            },
          },
        });

        if (!hasVendor || hasMissingVendor) {
          return NextResponse.json({
            reply:
              vendorResolution.followupMessage ||
              "Noted. Which vendor from Vendor Master should I use?",
          });
        }

        return NextResponse.json({ reply: "Noted. Please tell me the amount." });
      }

      const validation = validateWriteAction("create_transaction", canonicalPayload);
      if (!validation.valid) {
        await setSessionContext(sessionId, {
          draftWriteAction: {
            intent: "create_transaction",
            actionPayload: canonicalPayload,
          },
        });
        return NextResponse.json({
          reply: validation.message,
          requiresConfirmation: false,
        });
      }

      await clearDraftWriteAction(sessionId);
      await savePendingAction(sessionId, {
        intent: "create_transaction",
        actionPayload: canonicalPayload,
        requestedBy: actor,
      });

      const previewText = formatActionPreview("create_transaction", canonicalPayload);
      return NextResponse.json({
        reply:
          `Got it. I'll record this as a credit purchase by default. If you meant payment instead, just tell me.\n\n${previewText}\n\nReply CONFIRM to proceed, CANCEL to discard, or describe changes.`,
        requiresConfirmation: true,
        proposedAction: {
          intent: "create_transaction",
          payload: canonicalPayload,
        },
      });
    }
    let decision: Awaited<ReturnType<typeof askGeminiForDecision>>;
    const approveRejectIntent = extractApproveRejectIntent(message);
    const bulkApproveRejectIntent = extractBulkApproveRejectIntent(message);

    if (bulkApproveRejectIntent) {
      const db = await getDb();
      const pendingRows = await db
        .collection("transactions")
        .find({ status: "pending" })
        .project({ txn_code: 1 })
        .sort({ transaction_time: -1 })
        .toArray();

      const ids = pendingRows
        .map((row) => String(row.txn_code || "").toUpperCase())
        .filter((id) => /^[A-Z0-9]{4}$/.test(id));

      if (ids.length === 0) {
        return NextResponse.json({
          reply:
            bulkApproveRejectIntent.intent === "approve_transaction"
              ? "No pending transactions found to approve."
              : "No pending transactions found to reject.",
        });
      }

      const actionWord =
        bulkApproveRejectIntent.intent === "approve_transaction" ? "approve" : "reject";
      decision = {
        intent: bulkApproveRejectIntent.intent,
        requiresConfirmation: true,
        userMessage: `Prepared bulk ${actionWord} for ${ids.length} pending transaction(s).`,
        confirmationMessage: `I found ${ids.length} pending transaction(s). Reply CONFIRM to ${actionWord} all, or CANCEL to discard.`,
        actionPayload: {
          bulk: true,
          ids,
        },
      };
    } else if (approveRejectIntent) {
      decision = {
        intent: approveRejectIntent.intent,
        requiresConfirmation: true,
        userMessage: `Preparing ${approveRejectIntent.intent === "approve_transaction" ? "approval" : "rejection"} for ${approveRejectIntent.id}.`,
        actionPayload: { id: approveRejectIntent.id },
      };
    } else if (
      Object.keys(hintedCreateFields).length > 0 &&
      Number.isFinite(Number(hintedCreateFields.amount || 0)) &&
      Number(hintedCreateFields.amount || 0) > 0 &&
      (typeof hintedCreateFields.vendorName === "string" ||
        isSupportedCreateCommandMessage(message) ||
        /\b(from|to)\b/i.test(message))
    ) {
      decision = {
        intent: "create_transaction",
        requiresConfirmation: true,
        userMessage: "Prepared a transaction from your message.",
        actionPayload: hintedCreateFields,
      };
    } else if (sessionRow?.draftWriteAction?.intent === "create_transaction" && Object.keys(hintedCreateFields).length > 0) {
      decision = {
        intent: "create_transaction",
        requiresConfirmation: true,
        userMessage: "Updated the draft transaction with your latest details.",
        actionPayload: mergePayload(sessionRow.draftWriteAction.actionPayload, hintedCreateFields),
      };
    } else if (isAllVendorsListQuestion(message)) {
      decision = {
        intent: "ledger",
        requiresConfirmation: false,
        userMessage: "Fetching vendor ledger list.",
        actionPayload: {},
      };
    } else {
      decision = await askGeminiForDecision(message);
    }

    const messageLower = message.toLowerCase();
    const explicitVendorFilter = extractVendorFilterFromMessage(message);
    const ledgerVendorCandidate = extractLedgerVendorFromMessage(message);
    const kindFilter = extractKindFilterFromMessage(message);
    const dateRange = extractDateRangeFromMessage(message);
    const explicitTxnCode = extractTxnCodeFromMessage(message);
    const txnCodeFromContext = resolveTxnCodeFromContext(message, sessionRow?.lastTxnCodes);
    const resolvedTxnCode = explicitTxnCode || txnCodeFromContext;
    const asksCreationTime = isCreationTimeQuestion(message);
    const wantsAllTransactions =
      isAllTransactionsQuestion(message) ||
      messageLower.includes("both pending and approved");

    if (isPendingTransactionsQuestion(message) && !bulkApproveRejectIntent) {
      decision.intent = "list_pending";
      decision.requiresConfirmation = false;
      decision.actionPayload = {
        ...(decision.actionPayload || {}),
        ...(explicitVendorFilter ? { vendorFilter: explicitVendorFilter } : {}),
      };
    }

    if (isSummaryQuestion(message) && !bulkApproveRejectIntent) {
      decision.intent = "summary";
      decision.requiresConfirmation = false;
      decision.actionPayload = {
        ...(decision.actionPayload || {}),
        ...dateRange,
      };
    }

    // Deterministic override so explicit requests for all transactions are never downgraded to summary.
    // If a vendor is provided, preserve the vendor filter.
    if (wantsAllTransactions && !bulkApproveRejectIntent) {
      decision.intent = "list_recent";
      decision.requiresConfirmation = false;
      decision.actionPayload = {
        ...(decision.actionPayload || {}),
        ...(explicitVendorFilter ? { vendorFilter: explicitVendorFilter } : {}),
      };
    }

    if (kindFilter && isKindListReadQuestion(message) && !bulkApproveRejectIntent) {
      decision.intent = "list_recent";
      decision.requiresConfirmation = false;
      decision.actionPayload = {
        ...(decision.actionPayload || {}),
        kindFilter,
        ...dateRange,
      };
    }

    if (
      explicitVendorFilter &&
      (decision.intent === "list_recent" || decision.intent === "list_pending" || decision.intent === "search")
    ) {
      decision.actionPayload = {
        ...(decision.actionPayload || {}),
        vendorFilter: explicitVendorFilter,
      };
    }

    if (asksCreationTime) {
      if (resolvedTxnCode) {
        decision.intent = "search";
        decision.requiresConfirmation = false;
        decision.actionPayload = {
          ...(decision.actionPayload || {}),
          searchQuery: resolvedTxnCode,
        };
      } else if (/\bthis\s+transaction\b/i.test(message)) {
        return NextResponse.json({
          reply:
            "Which transaction ID do you mean? Follow-up: say 'when was transaction NGK4 created?'",
        });
      }
    }

    if (ledgerVendorCandidate) {
      const vendorFilter = ledgerVendorCandidate.trim();
      if (vendorFilter) {
        decision.intent = "ledger";
        decision.requiresConfirmation = false;
        decision.actionPayload = {
          ...(decision.actionPayload || {}),
          vendorFilter,
        };
      }
    }

    if (sessionRow?.draftWriteAction && decision.intent === "unknown") {
      const draft = sessionRow.draftWriteAction;
      if (draft.intent === "create_transaction") {
        const hintedCreateFields = extractCreateFieldHintsFromMessage(message);
        if (Object.keys(hintedCreateFields).length > 0) {
          decision.intent = draft.intent;
          decision.requiresConfirmation = true;
          decision.actionPayload = mergePayload(draft.actionPayload, hintedCreateFields);
          decision.userMessage = "Updated the draft transaction with your latest details.";
        }
      }
    }

    if (decision.requiresConfirmation) {
      const intent = decision.intent;
      if (
        intent === "create_transaction" ||
        intent === "approve_transaction" ||
        intent === "reject_transaction" ||
        intent === "edit_at_approval"
      ) {
        const draft = sessionRow?.draftWriteAction;
        const hintedCreateFieldsForValidation = (draft?.intent || intent) === "create_transaction"
          ? extractCreateFieldHintsFromMessage(message)
          : {};
        const actionPayload = draft
          ? mergePayload(
              draft.actionPayload,
              draft.intent === intent ? (decision.actionPayload || {}) : undefined,
              hintedCreateFieldsForValidation
            )
          : mergePayload(decision.actionPayload || {}, hintedCreateFieldsForValidation);

        if (intent === "create_transaction") {
          const kind = String(actionPayload.kind || "").toLowerCase();
          const hasNumericAmount = Number.isFinite(Number(actionPayload.amount || 0)) && Number(actionPayload.amount || 0) > 0;

          if (!draft && !isSupportedCreateCommandMessage(message) && !hasNumericAmount) {
            return NextResponse.json({
              reply:
                "Tell me what to record and I will ask follow-up questions if needed. If you mention payment explicitly, I record payment. Otherwise I record credit purchase by default.",
              requiresConfirmation: false,
            });
          }

          if (!kind) {
            const explicitChoice = extractKindChoice(message);
            if (explicitChoice === "payment") {
              actionPayload.kind = "payment";
            } else if (explicitChoice === "credit") {
              actionPayload.kind = "purchase";
            } else {
              actionPayload.kind = "purchase";
            }
          }

          if (kind === "purchase" && !String(actionPayload.mode || "").trim()) {
            actionPayload.mode = "credit";
          }

          if (String(actionPayload.kind || "").toLowerCase() === "payment" && !String(actionPayload.mode || "").trim()) {
            actionPayload.mode = "cash";
          }

          const vendorResolution = await canonicalizeVendorInPayload(actionPayload);
          Object.assign(actionPayload, vendorResolution.payload);
          if (vendorResolution.vendorMissing) {
            await setSessionContext(sessionId, {
              draftWriteAction: {
                intent: "create_transaction",
                actionPayload: {
                  ...actionPayload,
                  awaitingVendorName: true,
                  suggestedVendorNames: vendorResolution.suggestedVendorNames || [],
                },
              },
            });
            return NextResponse.json({
              reply:
                vendorResolution.followupMessage ||
                "Please provide a vendor name from Vendor Master.",
              requiresConfirmation: false,
            });
          }
        }

        const validation = validateWriteAction(intent, actionPayload);

        if (!validation.valid) {
          await setSessionContext(sessionId, {
            draftWriteAction: {
              intent,
              actionPayload,
            },
          });
          return NextResponse.json({
            reply: validation.message,
            requiresConfirmation: false,
          });
        }

        await clearDraftWriteAction(sessionId);

        await savePendingAction(sessionId, {
          intent,
          actionPayload,
          requestedBy: actor,
        });

        const previewText = formatActionPreview(intent, actionPayload);

        return NextResponse.json({
          reply:
            `${
              decision.confirmationMessage ||
              "I prepared the action. Reply CONFIRM to proceed, CANCEL to discard, or describe changes."
            }\n\n${previewText}`,
          requiresConfirmation: true,
          proposedAction: {
            intent,
            payload: actionPayload,
          },
        });
      }
    }

    if (
      decision.intent === "summary" ||
      decision.intent === "list_pending" ||
      decision.intent === "list_recent" ||
      decision.intent === "ledger" ||
      decision.intent === "search"
    ) {
      const result = await executeReadIntent(decision.intent, decision.actionPayload);
      const txnCodesForContext = extractTxnCodesFromResult(decision.intent, result.reply);
      if (txnCodesForContext.length > 0) {
        await setSessionContext(sessionId, { lastTxnCodes: txnCodesForContext });
      }
      // For list_pending/list_recent, show full details. For others, use formatter.
      if (decision.intent === "list_pending" || decision.intent === "list_recent") {
        try {
          const parsed = JSON.parse(result.reply);
          const txns = parsed.pending || parsed.recent || [];
          const totalCount = Number(parsed.totalCount || txns.length);
          const totalReturned = Number(parsed.totalReturned || txns.length);
          const truncated = Boolean(parsed.truncated);
          const count = txns.length;

          if (count === 0) {
            const msg =
              decision.intent === "list_pending"
                ? "No pending transactions found."
                : "No transactions found.";
            return NextResponse.json({ reply: msg });
          }

          const compactMode = totalCount > 50;
          const header = compactMode
            ? `Found ${totalCount} transaction(s). List is large, so showing compact view (${totalReturned} latest):`
            : `Found ${totalCount} transaction(s):`;

          const txnLines = txns.map((tx: Record<string, unknown>, idx: number) => {
            const txnCode = String(tx.txn_code || "").toUpperCase();
            const idDisplay = txnCode || "N/A";
            const kind = String(tx.kind || "").toUpperCase();
            const vendor = String(tx.vendor_name || tx.vendor || "");
            const amount = Number(tx.amount || 0);
            const status = String(tx.status || "").toUpperCase();
            const createdBy = String(tx.created_by || "");
            const mode = String(tx.mode || "");
            const referenceId = String(tx.reference_id || "");
            const date = tx.transaction_time
              ? new Date(tx.transaction_time as string).toLocaleString()
              : "";

            if (compactMode) {
              return [
                `${idx + 1}. ID: ${idDisplay}`,
                `Vendor: ${vendor || "N/A"}`,
                `Status: ${status || "N/A"}`,
              ].join("\n");
            }

            return [
              `${idx + 1}. ID: ${idDisplay}`,
              `Type: ${kind}`,
              `Amount: Rs.${amount}`,
              `Vendor: ${vendor || "N/A"}`,
              `Status: ${status || "N/A"}`,
              `Created By: ${createdBy || "N/A"}`,
              `Mode: ${mode || "N/A"}`,
              `Reference: ${referenceId || "-"}`,
              `Date: ${date || "N/A"}`,
            ].join("\n");
          });

          const footer = truncated
            ? "Result was truncated due to size. Ask 'search <vendor/ref/hash>' or 'ledger for <vendor>' for focused details."
            : "";

          const fullMsg = [header, ...txnLines, footer].filter(Boolean).join("\n\n");
          const pendingTransactions =
            decision.intent === "list_pending"
              ? txns.map((tx: Record<string, unknown>) => ({
                  txnCode: String(tx.txn_code || "").toUpperCase(),
                  kind: String(tx.kind || "").toUpperCase(),
                  amount: Number(tx.amount || 0),
                  vendorName: String(tx.vendor_name || tx.vendor || ""),
                  createdBy: String(tx.created_by || ""),
                }))
              : undefined;

          return NextResponse.json({
            reply: fullMsg,
            ...(pendingTransactions ? { pendingTransactions } : {}),
          });
        } catch {
          // Fallback to formatter if JSON parsing fails
          const formattedReply = await formatReadResultForUser(result.reply, decision.userMessage);
          return NextResponse.json({ reply: formattedReply });
        }
      }
      
      const deterministicReply = formatFieldValueReadReply(decision.intent, result.reply, message);
      if (deterministicReply) {
        return NextResponse.json({ reply: deterministicReply });
      }

      const formattedReply = await formatReadResultForUser(result.reply, decision.userMessage);
      return NextResponse.json({ reply: formattedReply });
    }

    const safeReply = String(decision.userMessage || "").trim();
    if (
      decision.intent === "unknown" &&
      (safeReply.length === 0 || safeReply.toLowerCase() === message.toLowerCase())
    ) {
      return NextResponse.json({
        reply:
          "I did not fully understand that. Please try again in simple words, and I will guide you.",
      });
    }

    return NextResponse.json({ reply: decision.userMessage });
  } catch (error) {
    if (error instanceof DatabaseConnectionError || isDatabaseConnectivityError(error)) {
      return NextResponse.json(
        {
          reply:
            "Database is temporarily unreachable (MongoDB Atlas network/connectivity issue). Please verify Atlas Network Access allows your current IP and then retry.",
        },
        { status: 503 }
      );
    }

    throw error;
  }
}
