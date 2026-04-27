import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import { Db, ObjectId } from "mongodb";

const APPROVAL_HIERARCHY: Record<string, string[]> = {
  gopi: ["brother"],
  father: ["gopi", "brother"],
  brother: ["gopi"],
};

function canApprove(createdBy: string, actor: string): boolean {
  const approversForCreator = APPROVAL_HIERARCHY[createdBy.toLowerCase()];
  return approversForCreator ? approversForCreator.includes(actor.toLowerCase()) : false;
}

function escapeRegex(input: string): string {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function applyApprovedLedgerEffect(db: Db, txnId: ObjectId) {
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

  const kind = String(txn.kind || "").toLowerCase();
  const amount = Number(txn.amount || 0);
  const delta = kind === "payment" ? -amount : amount;

  await db.collection("vendors").updateOne(
    { name: txn.vendor_name },
    {
      $inc: { balance: delta },
      $set: { type: "credit" },
    }
  );
}

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const body = await request.json();
  const { action, actor } = body as { action: "approve" | "reject"; actor: string };

  try {
    if (!action || !["approve", "reject"].includes(action)) {
      return NextResponse.json(
        { error: "Invalid action. Must be 'approve' or 'reject'." },
        { status: 400 }
      );
    }

    if (!actor) {
      return NextResponse.json(
        { error: "Actor is required for authorization." },
        { status: 401 }
      );
    }

    const db = await getDb();

    let txn;
    if (/^[a-f0-9]{24}$/i.test(id)) {
      txn = await db.collection("transactions").findOne({ _id: new ObjectId(id) });
    } else {
      txn = await db.collection("transactions").findOne({ txn_code: id.toUpperCase() });
    }

    if (!txn) {
      return NextResponse.json(
        { error: "Transaction not found." },
        { status: 404 }
      );
    }

    if (txn.status !== "pending") {
      return NextResponse.json(
        { error: `Transaction is already ${txn.status}.` },
        { status: 400 }
      );
    }

    const createdBy = String(txn.created_by || "").toLowerCase();
    if (!canApprove(createdBy, actor)) {
      return NextResponse.json(
        { error: `You are not authorized to ${action} this transaction. Created by: ${createdBy}.` },
        { status: 403 }
      );
    }

    const txnId = txn._id as ObjectId;
    const now = new Date().toISOString();

    if (action === "approve") {
      await db.collection("transactions").updateOne(
        { _id: txnId },
        {
          $set: {
            status: "approved",
            approved_by: actor,
            approved_at: now,
          },
        }
      );

      await applyApprovedLedgerEffect(db, txnId);

      await db.collection("audit_logs").insertOne({
        txn_id: txnId,
        action: "approve",
        approved_by: actor,
        timestamp: now,
      });
    } else {
      await db.collection("transactions").updateOne(
        { _id: txnId },
        {
          $set: {
            status: "rejected",
            rejected_by: actor,
            rejected_at: now,
          },
        }
      );

      await db.collection("audit_logs").insertOne({
        txn_id: txnId,
        action: "reject",
        rejected_by: actor,
        timestamp: now,
      });
    }

    return NextResponse.json({
      success: true,
      message: `Transaction ${action}d successfully.`,
      transaction: {
        id: String(txnId),
        txnCode: String(txn.txn_code || "").toUpperCase(),
        status: action === "approve" ? "approved" : "rejected",
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    const actionLabel = action === "approve" ? "approve" : "reject";
    return NextResponse.json(
      { error: message || `Failed to ${actionLabel} transaction` },
      { status: 500 }
    );
  }
}
