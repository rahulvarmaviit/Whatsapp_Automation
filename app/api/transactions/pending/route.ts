import { NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export async function GET() {
  try {
    const db = await getDb();
    const pendingRows = await db
      .collection("transactions")
      .find({ status: "pending" })
      .sort({ transaction_time: -1 })
      .toArray();

    const transactions = pendingRows.map((row) => ({
      id: String(row._id),
      txnCode: String(row.txn_code || "").toUpperCase(),
      kind: String(row.kind || "").toUpperCase(),
      amount: Number(row.amount || 0),
      vendorName: String(row.vendor_name || "N/A"),
      createdBy: String(row.created_by || "N/A"),
      mode: String(row.mode || "N/A"),
      referenceId: String(row.reference_id || "-"),
      note: String(row.note || ""),
      transactionTime: row.transaction_time ? new Date(row.transaction_time as string).toISOString() : null,
    }));

    return NextResponse.json({ transactions });
  } catch {
    return NextResponse.json(
      { error: "Failed to fetch pending transactions" },
      { status: 500 }
    );
  }
}
