import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export async function GET(_request: NextRequest) {
  try {
    const db = await getDb();
    const rows = await db
      .collection("vendors")
      .find({ is_active: { $ne: false } })
      .sort({ name: 1 })
      .toArray();

    const vendors = rows.map((row) => ({
      name: String(row.name || "").trim(),
      type: String(row.type || "credit"),
      balance: Number(row.balance || 0),
    }));

    return NextResponse.json({ vendors });
  } catch {
    return NextResponse.json({ error: "Failed to fetch vendors" }, { status: 500 });
  }
}

