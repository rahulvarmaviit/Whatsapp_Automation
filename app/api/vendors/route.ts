import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

function escapeRegex(input: string): string {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ name: string }> }
) {
  try {
    const { name } = await params;
    const vendorName = decodeURIComponent(name);
    const db = await getDb();

    const exactRegex = new RegExp(`^\\s*${escapeRegex(vendorName)}\\s*$`, "i");
    const vendor = await db.collection("vendors").findOne({ name: exactRegex });

    if (!vendor) {
      const partialMatch = await db.collection("vendors").findOne({
        name: { $regex: escapeRegex(vendorName), $options: "i" },
      });

      if (!partialMatch) {
        return NextResponse.json(
          { error: `Vendor \"${vendorName}\" not found.` },
          { status: 404 }
        );
      }

      const vendorTxns = await db
        .collection("transactions")
        .find({
          vendor_name:
            exactRegex ||
            new RegExp(`^\\s*${escapeRegex(String(partialMatch.name))}\\s*$`, "i"),
          status: "approved",
        })
        .sort({ approved_at: -1 })
        .toArray();

      const pendingCount = await db.collection("transactions").countDocuments({
        vendor_name: new RegExp(`^\\s*${escapeRegex(String(partialMatch.name))}\\s*$`, "i"),
        status: "pending",
      });

      return NextResponse.json({
        vendor: {
          name: String(partialMatch.name),
          type: String(partialMatch.type || "credit"),
          balance: Number(partialMatch.balance || 0),
        },
        transactions: vendorTxns.map((tx) => ({
          id: String(tx._id),
          txnCode: String(tx.txn_code || "").toUpperCase(),
          kind: String(tx.kind || "").toUpperCase(),
          amount: Number(tx.amount || 0),
          mode: String(tx.mode || "N/A"),
          status: String(tx.status || "N/A"),
          transactionTime: tx.transaction_time ? new Date(tx.transaction_time as string).toISOString() : null,
          approvedAt: tx.approved_at ? new Date(tx.approved_at as string).toISOString() : null,
          createdBy: String(tx.created_by || ""),
          note: String(tx.note || ""),
        })),
        pendingCount,
      });
    }

    const vendorTxns = await db
      .collection("transactions")
      .find({ vendor_name: exactRegex, status: "approved" })
      .sort({ approved_at: -1 })
      .toArray();

    const pendingCount = await db.collection("transactions").countDocuments({
      vendor_name: exactRegex,
      status: "pending",
    });

    return NextResponse.json({
      vendor: {
        name: String(vendor.name),
        type: String(vendor.type || "credit"),
        balance: Number(vendor.balance || 0),
      },
      transactions: vendorTxns.map((tx) => ({
        id: String(tx._id),
        txnCode: String(tx.txn_code || "").toUpperCase(),
        kind: String(tx.kind || "").toUpperCase(),
        amount: Number(tx.amount || 0),
        mode: String(tx.mode || "N/A"),
        status: String(tx.status || "N/A"),
        transactionTime: tx.transaction_time ? new Date(tx.transaction_time as string).toISOString() : null,
        approvedAt: tx.approved_at ? new Date(tx.approved_at as string).toISOString() : null,
        createdBy: String(tx.created_by || ""),
        note: String(tx.note || ""),
      })),
      pendingCount,
    });
  } catch {
    return NextResponse.json(
      { error: "Failed to fetch vendor ledger" },
      { status: 500 }
    );
  }
}
