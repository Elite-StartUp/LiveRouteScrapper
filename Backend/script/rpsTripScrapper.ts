// backend/script/rpsTripScrapper.ts
// Buffer-only RPS export -> Prisma(Postgres) Trip ingestion
// - Does NOT save Excel to disk
// - Inserts missing Vehicles automatically (upsert)
// - Inserts Trips with skipDuplicates (Unique_Scraper_Trip)
// - Uses your updated Trip model where Route_Reaching_Date_Time is REQUIRED

import { chromium, Download } from "playwright";
import * as dotenv from "dotenv";
import * as XLSX from "xlsx";
import { PrismaClient, VehicleType } from "@prisma/client";

dotenv.config({ path: "./.env" });

const prisma = new PrismaClient();

// ------------------- CONFIG -------------------
const RPS_URL =
  process.env.RPS_URL ??
  "http://smart.dsmsoft.com/FMSSmartApp/Safex_RPS_Reports/RPS_Reports.aspx?usergroup=NRM.101";

// Keep only rows whose Dispatch Date is within last N days
const DISPATCH_CUTOFF_DAYS = Number(process.env.RPS_DISPATCH_CUTOFF_DAYS ?? "12");

// In the datepicker, pick from date = today - N days (your python uses 15)
const PICK_FROM_DAYS = Number(process.env.RPS_PICK_FROM_DAYS ?? "15");

// Insert trips in batches
const INSERT_BATCH_SIZE = Number(process.env.RPS_INSERT_BATCH_SIZE ?? "500");

// ------------------- HELPERS -------------------
function normalizeHeader(h: string | undefined | null): string {
  return (h ?? "").replace(/\s+/g, " ").trim().toLowerCase();
}

function cleanRouteName(route: any): string {
  return String(route ?? "")
    .replace(/\s+/g, "") // remove all spaces
    .trim();
}

/**
 * Convert Excel cell values into JS Date safely.
 * Supports Date objects, Excel serial numbers, and strings.
 */
function toDate(value: any): Date | null {
  if (value == null || value === "") return null;

  if (value instanceof Date && !isNaN(value.getTime())) return value;

  if (typeof value === "number" && isFinite(value)) {
    const parsed = XLSX.SSF.parse_date_code(value);
    if (!parsed) return null;
    const d = new Date(
      parsed.y,
      parsed.m - 1,
      parsed.d,
      parsed.H || 0,
      parsed.M || 0,
      Math.floor(parsed.S || 0)
    );
    return isNaN(d.getTime()) ? null : d;
  }

  if (typeof value === "string") {
    const d = new Date(value);
    return isNaN(d.getTime()) ? null : d;
  }

  return null;
}

async function downloadAsBuffer(download: Download): Promise<Buffer> {
  const stream = await download.createReadStream();
  if (!stream) throw new Error("Unable to create download stream.");

  const chunks: Buffer[] = [];
  await new Promise<void>((resolve, reject) => {
    stream.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    stream.on("end", () => resolve());
    stream.on("error", (err) => reject(err));
  });

  return Buffer.concat(chunks);
}

// ------------------- SCRAPE (BUFFER ONLY) -------------------
async function fetchRpsExcelBuffer(): Promise<Buffer> {
  console.log("🚀 RPS Trip Scraper started (buffer-only, no file saving).");

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ acceptDownloads: true });
  const page = await context.newPage();

  try {
    console.log("🌐 Opening RPS URL:", RPS_URL);
    await page.goto(RPS_URL, { waitUntil: "load" });
    await page.waitForTimeout(4000);

    // Select all vehicles (same XPaths as your Python script)
    console.log("🚛 Selecting all vehicles...");
    await page
      .locator(
        "xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[4]/div[2]"
      )
      .click();
    await page.waitForTimeout(1000);

    await page
      .locator(
        "xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[4]/div[3]/div[2]/ul/li[1]/input"
      )
      .click();
    await page.waitForTimeout(1000);

    // Pick from date (today - PICK_FROM_DAYS) using the same approach as Python
    console.log(`📅 Picking from-date (last ${PICK_FROM_DAYS} days)...`);
    const from = new Date();
    from.setDate(from.getDate() - PICK_FROM_DAYS);
    const fromDay = from.getDate();

    await page
      .locator(
        "xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[1]/div[2]/input"
      )
      .click();
    await page.waitForTimeout(1000);

    // Mimic your python: click previous month once.
    // If the datepicker UI changes, update this logic.
    await page
      .locator(
        '//div[contains(@class,"xdsoft_datepicker")]//button[contains(@class,"xdsoft_prev")]'
      )
      .nth(0)
      .click();
    await page.waitForTimeout(1000);

    const dayXpath = `//td[@data-date="${fromDay}" and contains(@class, "xdsoft_date") and not(contains(@class, "xdsoft_disabled"))]`;
    await page.locator(dayXpath).nth(0).click();
    await page.waitForTimeout(1000);

    // Submit
    console.log("📤 Clicking Submit...");
    await page
      .locator(
        "xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[5]/div/button"
      )
      .click();
    await page.waitForTimeout(5000);

    // Export / download trigger
    console.log("📥 Triggering export download (capturing buffer)...");
    const downloadPromise = page.waitForEvent("download");

    await page
      .locator(
        "xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[4]/div/table/div/div[4]/div/div/div[3]/div[1]/div/div/div"
      )
      .click();

    const download = await downloadPromise;
    console.log("✅ Export started:", download.suggestedFilename());

    const buf = await downloadAsBuffer(download);
    console.log("✅ Excel received in memory. Bytes =", buf.length);

    return buf;
  } finally {
    await browser.close();
  }
}

// ------------------- PARSE EXCEL (BUFFER) -------------------
type ParsedRow = {
  rpsNo: string;
  vehicleNo: string;
  dispatchDate: Date;
  closureDate: Date; // REQUIRED (because Route_Reaching_Date_Time is required now)
  routeName: string;
};

function parseRpsExcel(buf: Buffer): ParsedRow[] {
  const wb = XLSX.read(buf, {
    type: "buffer",
    cellDates: true,
    cellText: false,
    cellNF: false,
  });

  const sheetName = wb.SheetNames[0];
  if (!sheetName) throw new Error("Excel has no sheets.");

  const ws = wb.Sheets[sheetName];

  // Use array-of-arrays to map headers robustly even if columns move
  const aoa = XLSX.utils.sheet_to_json<any[]>(ws, { header: 1, defval: "" });
  if (!aoa.length) return [];

  const headerRow = aoa[0].map((h) => normalizeHeader(String(h)));

  const idxRps = headerRow.findIndex(
    (h) => h === "rps number" || h === "rps no" || h === "rps"
  );
  const idxVeh = headerRow.findIndex(
    (h) => h === "vehicle number" || h === "vehicle no"
  );
  const idxDispatch = headerRow.findIndex((h) => h === "dispatch date");
  const idxClosure = headerRow.findIndex((h) => h === "closure date");
  const idxRoute = headerRow.findIndex((h) => h === "route name" || h === "route");

  const missing: string[] = [];
  if (idxRps === -1) missing.push("RPS Number");
  if (idxVeh === -1) missing.push("Vehicle Number");
  if (idxDispatch === -1) missing.push("Dispatch Date");
  if (idxClosure === -1) missing.push("Closure Date");
  if (idxRoute === -1) missing.push("Route Name");

  if (missing.length) {
    throw new Error(
      `Excel headers missing/changed: ${missing.join(
        ", "
      )}. Found headers: ${aoa[0].join(" | ")}`
    );
  }

  const rows: ParsedRow[] = [];

  for (let i = 1; i < aoa.length; i++) {
    const r = aoa[i];
    if (!r || !r.length) continue;

    const rpsNo = String(r[idxRps] ?? "").trim();
    const vehicleNo = String(r[idxVeh] ?? "").trim();
    const dispatchDate = toDate(r[idxDispatch]);
    const closureDate = toDate(r[idxClosure]); // MUST exist
    const routeName = cleanRouteName(r[idxRoute]);

    // Required validations (based on your updated Trip model)
    if (!rpsNo || !vehicleNo || !routeName) continue;
    if (!dispatchDate) continue;
    if (!closureDate) continue;

    rows.push({
      rpsNo,
      vehicleNo,
      dispatchDate,
      closureDate,
      routeName,
    });
  }

  return rows;
}

// ------------------- PUSH TO DB -------------------
async function ensureVehiclesExist(vehicleNumbers: string[]) {
  // Upsert each vehicle so Trip foreign key always succeeds
  for (const vn of vehicleNumbers) {
    await prisma.vehicle.upsert({
      where: { Vehicle_Number: vn },
      create: {
        Vehicle_Number: vn,
        Vehicle_Type: VehicleType.OTHER,
        Vehicle_Route: null,
        branchId: null,
      },
      update: {}, // do nothing if already exists
    });
  }
}

async function pushToDb(rows: ParsedRow[]) {
  console.log("📊 Total parsed rows:", rows.length);

  // Filter: Dispatch Date within last DISPATCH_CUTOFF_DAYS
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - DISPATCH_CUTOFF_DAYS);

  let filtered = rows.filter((r) => r.dispatchDate >= cutoff);

  if (!filtered.length) {
    console.log(`📭 No rows after ${DISPATCH_CUTOFF_DAYS}-day dispatch cutoff.`);
    return;
  }

  // Sort by closure date (optional, but keeps deterministic inserts)
  filtered.sort((a, b) => a.closureDate.getTime() - b.closureDate.getTime());

  // Ensure vehicles
  const vehicleNumbers = Array.from(new Set(filtered.map((r) => r.vehicleNo)));
  console.log("🚛 Ensuring Vehicle exists:", vehicleNumbers.length);
  await ensureVehiclesExist(vehicleNumbers);

  // Insert Trips with skipDuplicates based on:
  // @@unique([Vehicle_Number, Route_Start_Date_Time, RouteName, RPS_No], name: "Unique_Scraper_Trip")
  let inserted = 0;

  for (let i = 0; i < filtered.length; i += INSERT_BATCH_SIZE) {
    const batch = filtered.slice(i, i + INSERT_BATCH_SIZE);

    const res = await prisma.trip.createMany({
      data: batch.map((b) => ({
        Route_Start_Date_Time: b.dispatchDate,
        Route_Reaching_Date_Time: b.closureDate, // REQUIRED
        Vehicle_Number: b.vehicleNo,
        RPS_No: b.rpsNo,
        RouteName: b.routeName,
        // All other fields omitted => remain NULL (optional ? fields)
      })),
      skipDuplicates: true,
    });

    inserted += res.count;
    console.log(`✅ Batch ${Math.floor(i / INSERT_BATCH_SIZE) + 1}: inserted ${res.count}`);
  }

  console.log("🏁 Done. Total newly inserted Trip rows =", inserted);
}

// ------------------- MAIN -------------------
async function main() {
  if (!process.env.DATABASE_URL) {
    console.error("❌ Missing DATABASE_URL in .env");
    process.exit(1);
  }

  try {
    const buf = await fetchRpsExcelBuffer();
    const parsed = parseRpsExcel(buf);
    await pushToDb(parsed);
  } catch (err: any) {
    console.error("❌ Error:", err?.message ?? err);
    process.exitCode = 1;
  } finally {
    await prisma.$disconnect();
  }
}

main();
