// backend/script/liveRouteScrapper.ts
import { chromium, Page } from 'playwright';
import * as dotenv from 'dotenv';
import * as XLSX from 'xlsx';
import { PrismaClient, TripSide, Prisma } from '@prisma/client';
import { randomUUID } from 'crypto';

dotenv.config({ path: './.env' });

// ------------------- ENV + PRISMA SETUP -------------------

const prisma = new PrismaClient();

const FMS_URL =
  process.env.FMS_URL ?? 'https://fmssmart.dsmsoft.com/FMSSmartApp/#/login';
const USERNAME = process.env.FMS_USERNAME;
const PASSWORD = process.env.FMS_PASSWORD;

if (!USERNAME || !PASSWORD) {
  console.error('Missing FMS_USERNAME or FMS_PASSWORD in .env');
  process.exit(1);
}

const USERNAME_STR: string = USERNAME;
const PASSWORD_STR: string = PASSWORD;

// ------------------- UTILITIES -------------------

function normalizeHeader(h: string | undefined | null): string {
  return (h ?? '').replace(/\s+/g, ' ').trim().toLowerCase();
}

// general location key normalizer (for city/landmark names and codes)
function normalizeLocationKey(s: string): string {
  return String(s || '')
    .toUpperCase()
    .replace(/\s+/g, '')
    .replace(/-/g, '')
    .replace(/\./g, '')
    .replace(/_/g, '');
}

// function buildRouteEndpointKey(name: string): string {
//   let s = String(name || '').toLowerCase();

//   // remove anything in parentheses
//   s = s.replace(/\([^)]*\)/g, ' ');

//   // replace hyphens with spaces (main noisy symbol)
//   s = s.replace(/-/g, ' ');

//   // remove numbers
//   s = s.replace(/[0-9]+/g, ' ');

//   // remove other non-alphanumeric symbols
//   s = s.replace(/[^a-z\s]+/g, ' ');

//   // collapse spaces
//   s = s.replace(/\s+/g, ' ').trim();

//   // remove 'safexpress' prefix if present
//   s = s.replace(/^safexpress\s+/, '');

//   // remove suffixes if present as last word
//   s = s.replace(/\s+(hub|sds|inbound|outbound)\s*$/g, '');

//   // collapse spaces again
//   s = s.replace(/\s+/g, ' ').trim();

//   // finally remove all spaces (build the core key)
//   s = s.replace(/\s+/g, '');

//   return s;
// }


// Extract branch code inside last parentheses – TS version
function normalizeSimple(name: string): string {
  return String(name || '')
    .toLowerCase()
    .replace(/\s+/g, '');     // remove all spaces
}

function extractCodeInParensTS(name: string): string {
  const str = String(name || '');
  const matches = str.match(/\(([^)]+)\)/g);
  if (!matches || !matches.length) return '';
  const last = matches[matches.length - 1];
  return last.replace(/[()]/g, '').trim();
}

// columns we care about (including RPS No)
const TARGET_HEADERS = {
  vehicleNumber: 'Vehicle Number',
  lastLocationDate: 'Last Location Date',
  vehicleGroup: 'Vehicle Group',
  lastLocation: 'Last Location',
  speedKmph: 'Speed(Kmph)',
  consignerName: 'Consigner Name',
  consigneeName: 'Consignee Name',
  dispatchDate: 'Dispatch Date',
  eta: 'ETA',
  plannedDistance: 'Planned Distance(Kms)',
  remainingDistance: 'Remaining Distance(Kms)',
  tripStatus: 'Trip Status',
  delayTime: 'Delay Time(HH:MM:SS)',
  rpsNumber: 'RPS No',
};

type ParsedRow = {
  vehicleNumber: string;
  lastLocationDate: string;
  vehicleGroup: string;
  lastLocation: string;
  speedKmph: string;
  consignerName: string;
  consigneeName: string;
  dispatchDate: string;
  eta: string;
  plannedDistance: string;
  remainingDistance: string;
  tripStatus: string;
  delayTime: string;
  rpsNumber: string;
};

// ---------- MERGE HELPERS (ROUTE + SCRAPED DATA) ----------

// Extract first part of consignerName as "source"
function extractSourceFromConsigner(consignerName: string): string {
  if (!consignerName) return '';
  const parts = consignerName.split(';');
  return (parts[0] ?? '').trim();
}

// Extract last part of consigneeName as "destination"
function extractDestinationFromConsignee(consigneeName: string): string {
  if (!consigneeName) return '';
  const parts = consigneeName.split(';');
  return (parts[parts.length - 1] ?? '').trim();
}

// Extra helpers specifically for city/branch mapping

// Consigner → city-like name (e.g. "LUCKNOW-11" -> "LUCKNOW")
function extractCityFromConsignerName(consignerName: string): string {
  const firstPart = extractSourceFromConsigner(consignerName);
  const main = firstPart.split('-')[0].trim();
  return main;
}

// Consignee → city-like name from last part (e.g. "SAFEXPRESS AMBALA(AML11)" -> "AMBALA")
function extractCityFromConsigneeName(consigneeName: string): string {
  const lastPart = extractDestinationFromConsignee(consigneeName);
  const beforeParen = lastPart.split('(')[0].trim();
  const parts = beforeParen.split(/\s+/);
  return parts[parts.length - 1] || '';
}

// Consignee → branch code in last (...)
function extractBranchCodeFromConsignee(consigneeName: string): string {
  const lastPart = extractDestinationFromConsignee(consigneeName);
  return extractCodeInParensTS(lastPart);
}

// Normalize for matching Route.Source / Route.Destination with consigner/consignee
// IMPORTANT: remove ALL spaces + also remove '-' and '_' for safety.
function makeRouteKey(source: string, destination: string): string {
  const src = normalizeSimple(source);
  const dst = normalizeSimple(destination);
  return `${src}___${dst}`;
}


type RouteIndexItem = {
  id: string;
  RouteName: string;
  Route_Side: 'Up' | 'Down' | null;
  Source: string | null;
  Destination: string | null;
  Middle_Stops: string[] | null;
};

type LiveMergedRow = ParsedRow & {
  routeId?: string;
  routeName?: string;
  routeSource?: string | null;
  routeDestination?: string | null;
  routeSourceLat?: number | null;
  routeSourceLng?: number | null;
  routeDestinationLat?: number | null;
  routeDestinationLng?: number | null;
  routeSide?: 'Up' | 'Down' | null;
  routeMiddleStops?: string[] | null;

  // debug / unmatched tracking
  sourceExtracted: string;
  destinationExtracted: string;
  matchKey: string;
};

// final frontend shape
type LiveRouteVehicleEntry = {
  vehicleNumber: string;
  rpsNumber: string | null;
  dispatchDate: string;
  lastLocationDate: string;
  lastLocation: string;
  lateHours: number;
  middleStops: string[];
  expectedArrival: string;
  lat: number | null;
  lng: number | null;
};

type LiveRouteSideBucket = {
  totalVehicles: number;
  lateVehicles: number;
  vehicles: LiveRouteVehicleEntry[];
};

type LiveRouteRoute = {
  id: string;
  source: string;
  destination: string;
  sourceLat: number | null;
  sourceLng: number | null;
  destLat: number | null;
  destLng: number | null;
  up: LiveRouteSideBucket;
  down: LiveRouteSideBucket;
};

type UnmatchedRoutePair = {
  consignerName: string;
  consigneeName: string;
  sourceExtracted: string;
  destinationExtracted: string;
  matchKey: string;
};

type VehicleCoord = {
  vehicleNumber: string;
  lat: number | null;
  lng: number | null;
};

// For location maps built from Map View dropdowns
type LocationEntry = {
  rawName: string;
  lat: number;
  lng: number;
};

type LocationMaps = {
  cityByNormName: Map<string, LocationEntry>;
  landmarkByNormCode: Map<string, LocationEntry>;
  landmarkByNormName: Map<string, LocationEntry>;
};

// ------------------- ROUTE INDEX (DB) -------------------

async function buildRouteIndex(): Promise<Map<string, RouteIndexItem>> {
  const routes = await prisma.route.findMany({
    select: {
      id: true,
      RouteName: true,
      Route_Side: true,
      Source: true,
      Destination: true,
      Middle_Stops: true,
    },
  });

  const map = new Map<string, RouteIndexItem>();

  for (const r of routes) {
    if (!r.Source || !r.Destination) continue;

    const key = makeRouteKey(r.Source, r.Destination);
    map.set(key, r);
  }

  console.log(
    `Route index built with ${map.size} (Source,Destination) keys.`,
  );
  return map;
}

// Merge parsed Excel rows with Route master data
function mergeWithRoutes(
  rows: ParsedRow[],
  routeIndex: Map<string, RouteIndexItem>,
): LiveMergedRow[] {
  return rows.map((rec) => {
    const rawSource = extractSourceFromConsigner(rec.consignerName);
    const rawDestination = extractDestinationFromConsignee(rec.consigneeName);

    const key = makeRouteKey(rawSource, rawDestination);
    const route = routeIndex.get(key);

    if (!route) {
      console.warn(
        `No route match for consigner="${rawSource}" -> consignee="${rawDestination}" (key=${key})`,
      );
    }

    return {
      ...rec,
      routeId: route?.id,
      routeName: route?.RouteName,
      routeSource: route?.Source ?? null,
      routeDestination: route?.Destination ?? null,
      routeSourceLat: null,
      routeSourceLng: null,
      routeDestinationLat: null,
      routeDestinationLng: null,
      routeSide: route?.Route_Side ?? null,
      routeMiddleStops: route?.Middle_Stops ?? null,

      sourceExtracted: rawSource,
      destinationExtracted: rawDestination,
      matchKey: key,
    };
  });
}

// Collect all unmatched rows for later analysis
function collectUnmatched(merged: LiveMergedRow[]): UnmatchedRoutePair[] {
  const seen = new Set<string>();
  const result: UnmatchedRoutePair[] = [];

  for (const row of merged) {
    if (row.routeId) continue; // matched, skip

    const key =
      row.matchKey +
      '|' +
      row.consignerName +
      '|' +
      row.consigneeName;

    if (seen.has(key)) continue;
    seen.add(key);

    result.push({
      consignerName: row.consignerName,
      consigneeName: row.consigneeName,
      sourceExtracted: row.sourceExtracted,
      destinationExtracted: row.destinationExtracted,
      matchKey: row.matchKey,
    });
  }

  return result;
}

// Build RouteName / Source / Destination for UnmatchedTripSide
// RouteName = Source/MiddleStops/Destination
// If no middle stops: Source/Destination
function buildUnmatchedRouteInfo(
  pair: UnmatchedRoutePair,
): { routeName: string; source: string; destination: string } | null {
  // Prefer extracted source/dest; fall back to parsing; last fallback raw strings
  const sourceRaw =
    pair.sourceExtracted ||
    extractSourceFromConsigner(pair.consignerName) ||
    pair.consignerName;
  const destinationRaw =
    pair.destinationExtracted ||
    extractDestinationFromConsignee(pair.consigneeName) ||
    pair.consigneeName;

  const source = String(sourceRaw || '').trim();
  const destination = String(destinationRaw || '').trim();

  if (!source || !destination) {
    return null; // not enough info to store
  }

  // Middle stops from consigneeName: all parts except the last (split on ';')
  const consigneeParts = String(pair.consigneeName || '')
    .split(';')
    .map((p) => p.trim())
    .filter(Boolean);

  let middleStops: string[] = [];
  if (consigneeParts.length > 1) {
    // everything except last is treated as middle stops
    middleStops = consigneeParts.slice(0, -1);
  }

  let routeName: string;
  if (middleStops.length > 0) {
    routeName = [source, ...middleStops, destination].join('/');
  } else {
    routeName = `${source}/${destination}`;
  }

  return { routeName, source, destination };
}

async function saveUnmatchedToDb(unmatched: UnmatchedRoutePair[]): Promise<void> {
  if (!unmatched.length) {
    console.log('No unmatched route pairs to store.');
    return;
  }

  const data: Prisma.UnmatchedTripSideCreateManyInput[] = [];

  for (const pair of unmatched) {
    const info = buildUnmatchedRouteInfo(pair);
    if (!info) continue;

    const { routeName, source, destination } = info;

    data.push({
      RouteName: routeName,
      Source: source,
      Destination: destination,
      Trip_Side: null, // user will fill Up/Down later
    });
  }

  if (!data.length) {
    console.log('No valid unmatched entries to store in UnmatchedTripSide.');
    return;
  }

  try {
    const result = await prisma.unmatchedTripSide.createMany({
      data,
      skipDuplicates: true, // skip existing RouteName (unique constraint)
    });

    console.log(
      `Inserted ${result.count} new unmatched route patterns into UnmatchedTripSide (duplicates skipped).`,
    );
  } catch (err) {
    console.error(
      'Error inserting into UnmatchedTripSide (some may already exist or schema issue):',
      err,
    );
  }
}


// Parse "HH:MM:SS" delay string into hours (float)
function parseDelayToHours(delay: string): number {
  if (!delay) return 0;
  const parts = delay.split(':').map((p) => parseInt(p, 10));
  if (parts.length < 2 || parts.some((n) => isNaN(n))) return 0;
  const [hh, mm, ss] = [parts[0] || 0, parts[1] || 0, parts[2] || 0];
  return hh + mm / 60 + ss / 3600;
}

// For ETA, if multiple timestamps separated by ';', take the last NON-NA entry
// (final destination ETA). Format is "DD/MM/YYYY HH:MM:SS".
function pickEtaForDestination(eta: string): string {
  if (!eta) return '';
  const parts = eta
    .split(';')
    .map((p) => p.trim())
    .filter((p) => p && p.toUpperCase() !== 'NA');

  if (!parts.length) return '';
  return parts[parts.length - 1];
}

// Aggregate merged rows into per-route structure (with up/down + vehicles array)
function aggregateLiveRoutes(
  merged: LiveMergedRow[],
  coordsByVehicle: Map<string, { lat: number | null; lng: number | null }>,
): LiveRouteRoute[] {
  const routeMap = new Map<string, LiveRouteRoute>();

  for (const row of merged) {
    if (!row.routeId || !row.routeSource || !row.routeDestination) {
      // skip unmatched routes here; we'll handle them via collectUnmatched()
      continue;
    }

    let bucket = routeMap.get(row.routeId);
    if (!bucket) {
      bucket = {
        id: row.routeId,
        source: row.routeSource,
        destination: row.routeDestination,
        sourceLat: row.routeSourceLat ?? null,
        sourceLng: row.routeSourceLng ?? null,
        destLat: row.routeDestinationLat ?? null,
        destLng: row.routeDestinationLng ?? null,
        up: { totalVehicles: 0, lateVehicles: 0, vehicles: [] },
        down: { totalVehicles: 0, lateVehicles: 0, vehicles: [] },
      };
      routeMap.set(row.routeId, bucket);
    }

    const lateHours = parseDelayToHours(row.delayTime);

    // per-vehicle lat/lng if we have it
    const coordKey = row.vehicleNumber.trim();
    const coord = coordsByVehicle.get(coordKey);
    const lat = coord?.lat ?? null;
    const lng = coord?.lng ?? null;

    const vehicleEntry: LiveRouteVehicleEntry = {
      vehicleNumber: row.vehicleNumber,
      rpsNumber: row.rpsNumber || null,
      dispatchDate: row.dispatchDate,
      lastLocationDate: row.lastLocationDate,
      lastLocation: row.lastLocation,
      lateHours,
      middleStops: row.routeMiddleStops ?? [],
      expectedArrival: pickEtaForDestination(row.eta),
      lat,
      lng,
    };

    let sideBucket: LiveRouteSideBucket | null = null;

    if (row.routeSide === 'Up') {
      sideBucket = bucket.up;
    } else if (row.routeSide === 'Down') {
      sideBucket = bucket.down;
    } else {
      console.warn(
        'Row has no valid routeSide (neither Up nor Down), skipping from side buckets:',
        {
          routeId: row.routeId,
          routeName: row.routeName,
          source: row.routeSource,
          destination: row.routeDestination,
          routeSide: row.routeSide,
          vehicleNumber: row.vehicleNumber,
        },
      );
      continue; // do not put in up/down
    }

    sideBucket.vehicles.push(vehicleEntry);
    sideBucket.totalVehicles += 1;
    if (lateHours > 0) {
      sideBucket.lateVehicles += 1;
    }

    // If bucket-level coords not yet set (for this route id), fill from this row
    if (bucket.sourceLat == null && row.routeSourceLat != null) {
      bucket.sourceLat = row.routeSourceLat;
      bucket.sourceLng = row.routeSourceLng ?? null;
    }
    if (bucket.destLat == null && row.routeDestinationLat != null) {
      bucket.destLat = row.routeDestinationLat;
      bucket.destLng = row.routeDestinationLng ?? null;
    }
  }

  return Array.from(routeMap.values());
}

// ------------------- EXCEL PARSING -------------------

function parseRows(rows: any[]): ParsedRow[] {
  if (!rows.length) {
    console.warn('No rows found in Excel');
    return [];
  }

  const headerRow: string[] = rows[0] as string[];

  // build a mapping from normalized header -> column index
  const headerIndexMap: Record<string, number> = {};
  headerRow.forEach((rawHeader, idx) => {
    const norm = normalizeHeader(String(rawHeader));
    if (norm) headerIndexMap[norm] = idx;
  });

  // build mapping from our logical keys -> actual index
  const colIndex = {
    vehicleNumber: headerIndexMap[normalizeHeader(TARGET_HEADERS.vehicleNumber)],
    lastLocationDate:
      headerIndexMap[normalizeHeader(TARGET_HEADERS.lastLocationDate)],
    vehicleGroup: headerIndexMap[normalizeHeader(TARGET_HEADERS.vehicleGroup)],
    lastLocation: headerIndexMap[normalizeHeader(TARGET_HEADERS.lastLocation)],
    speedKmph: headerIndexMap[normalizeHeader(TARGET_HEADERS.speedKmph)],
    consignerName:
      headerIndexMap[normalizeHeader(TARGET_HEADERS.consignerName)],
    consigneeName:
      headerIndexMap[normalizeHeader(TARGET_HEADERS.consigneeName)],
    dispatchDate:
      headerIndexMap[normalizeHeader(TARGET_HEADERS.dispatchDate)],
    eta: headerIndexMap[normalizeHeader(TARGET_HEADERS.eta)],
    plannedDistance:
      headerIndexMap[normalizeHeader(TARGET_HEADERS.plannedDistance)],
    remainingDistance:
      headerIndexMap[normalizeHeader(TARGET_HEADERS.remainingDistance)],
    tripStatus: headerIndexMap[normalizeHeader(TARGET_HEADERS.tripStatus)],
    delayTime: headerIndexMap[normalizeHeader(TARGET_HEADERS.delayTime)],
    rpsNumber: headerIndexMap[normalizeHeader(TARGET_HEADERS.rpsNumber)],
  };

  console.log('Resolved column indices:', colIndex);

  const dataRows = rows.slice(1); // skip header row

  const result: ParsedRow[] = dataRows
    .map((row) => {
      const r = row as (string | number | null | undefined)[];

      const get = (idx: number | undefined) =>
        idx == null ? undefined : r[idx];

      return {
        vehicleNumber: get(colIndex.vehicleNumber)?.toString().trim() ?? '',
        lastLocationDate:
          get(colIndex.lastLocationDate)?.toString().trim() ?? '',
        vehicleGroup: get(colIndex.vehicleGroup)?.toString().trim() ?? '',
        lastLocation: get(colIndex.lastLocation)?.toString().trim() ?? '',
        speedKmph: get(colIndex.speedKmph)?.toString().trim() ?? '',
        consignerName: get(colIndex.consignerName)?.toString().trim() ?? '',
        consigneeName: get(colIndex.consigneeName)?.toString().trim() ?? '',
        dispatchDate: get(colIndex.dispatchDate)?.toString().trim() ?? '',
        eta: get(colIndex.eta)?.toString().trim() ?? '',
        plannedDistance: get(colIndex.plannedDistance)?.toString().trim() ?? '',
        remainingDistance:
          get(colIndex.remainingDistance)?.toString().trim() ?? '',
        tripStatus: get(colIndex.tripStatus)?.toString().trim() ?? '',
        delayTime: get(colIndex.delayTime)?.toString().trim() ?? '',
        rpsNumber: get(colIndex.rpsNumber)?.toString().trim() ?? '',
      };
    })
    // filter out totally empty rows
    .filter((r) =>
      Object.values(r).some((val) => val !== '' && val != null),
    );

  return result;
}

// ------------------- MAP VIEW COORDINATE SCRAPING -------------------

// Open the Angular Map View page *from the On Trip card dropdown*,
// using the same pattern as Grid View (but choosing "Map View" instead).
async function openMapView(page: Page): Promise<void> {
  console.log('Opening Map View from On Trip card dropdown…');

  // 1) Close Grid View modal if it is still open (overlay blocking clicks).
  const modalCloseBtn = page.locator(
    'modal-container button.close, ' +
      'modal-container button[aria-label="Close"], ' +
      'modal-container .modal-header button.close, ' +
      'modal-container button:has-text("Close")',
  );

  try {
    const hasModal = (await modalCloseBtn.count()) > 0;
    if (hasModal && (await modalCloseBtn.first().isVisible())) {
      console.log('Closing Grid View modal…');
      await modalCloseBtn.first().click();
      await page.waitForTimeout(1000);
    }
  } catch (err) {
    console.warn('Could not detect/close modal (may already be closed):', err);
  }

  // 2) Hover on the "On Trip Vehicles" card again
  const onTripCard = page.locator('#onTrip');
  await onTripCard.waitFor({ state: 'visible', timeout: 15000 });
  await onTripCard.hover();
  await page.waitForTimeout(700); // let the hover menu appear

  // 3) Click the dropdown (same one you used for Grid View)
  const dropdownBtn = onTripCard.locator('button#dropdownMenuButton');
  await dropdownBtn.waitFor({ state: 'visible', timeout: 5000 });
  await dropdownBtn.click();
  await page.waitForTimeout(500);

  // 4) Choose the "Map View" option inside that dropdown
  const mapViewOption = page.locator(
    'button:has-text("Map View"), ' +
      'a:has-text("Map View"), ' +
      'li:has-text("Map View")',
  );
  await mapViewOption.first().waitFor({ state: 'visible', timeout: 5000 });
  await mapViewOption.first().click();

  console.log('Map View option clicked. Waiting for map to load…');
  await page.waitForLoadState('networkidle');
  await page.waitForTimeout(3000);
}

// Read live lat/lng for each vehicle and return a Map keyed by vehicleNumber.
async function fetchLiveVehicleCoords(
  page: Page,
): Promise<Map<string, { lat: number | null; lng: number | null }>> {
  // Go to map view (via On Trip card dropdown)
  await openMapView(page);

  // -------- CLICK "All Vehicles" CHECKBOX --------
  try {
    const allVehiclesCheckbox = page.locator(
      'label.checkmark-container:has-text("All Vehicles") input[type="checkbox"][value="all"]',
    );

    await allVehiclesCheckbox.waitFor({ state: 'visible', timeout: 10000 });

    try {
      const allVehiclesLabel = page.locator('label.checkmark-container:has-text("All Vehicles")');
      await allVehiclesLabel.waitFor({ state: 'visible', timeout: 10000 });
    
      await allVehiclesLabel.scrollIntoViewIfNeeded();
      await allVehiclesLabel.click({ force: true });
    
      console.log('Clicked "All Vehicles" label on Map View.');
      await page.waitForLoadState('networkidle').catch(() => {});
      await page.waitForTimeout(2000);
    } catch (err) {
      console.warn(
        'Could not click "All Vehicles" (maybe already checked or selector mismatch):',
        err,
      );
    }

    console.log('Clicked "All Vehicles" checkbox on Map View.');

    await page.waitForLoadState('networkidle').catch(() => {});
    await page.waitForTimeout(2000);
  } catch (err) {
    console.warn(
      'Could not click "All Vehicles" checkbox (maybe already checked or selector mismatch):',
      err,
    );
  }

  // -------- WAIT FOR ANGULAR MAP DATA --------
  await page.waitForFunction(
    () => {
      const w = window as any;
      const c = w.angularComponentRef?.component;
      return (
        c &&
        Array.isArray(c.vehicles) &&
        c.vehicles.length > 0 &&
        Array.isArray(c.markersList) &&
        c.markersList.length > 0
      );
    },
    { timeout: 30000 },
  );

  // -------- EXTRACT COORDINATES + VEHICLE NUMBERS --------
  const coords: VehicleCoord[] = await page.evaluate(() => {
    const w = window as any;
    const c = w.angularComponentRef.component;

    const vehicles: any[] = c.vehicles || [];
    const markersList: any[] = c.markersList || [];

    const metaByVehicleId = new Map<number, any>();
    for (const m of markersList) {
      if (m && m.vehicleId != null) {
        metaByVehicleId.set(m.vehicleId, m);
      }
    }

    const out: VehicleCoord[] = [];

    for (const v of vehicles) {
      const meta = metaByVehicleId.get(v.id);
      if (!meta) continue;

      out.push({
        vehicleNumber: String(meta.vehicleNumber).trim(),
        lat: typeof v.lat === 'number' ? v.lat : null,
        lng: typeof v.lng === 'number' ? v.lng : null,
      });
    }

    return out;
  });

  console.log(
    `Fetched live coordinates for ${coords.length} vehicles from Map View.`,
  );

  return new Map(
    coords.map((v) => [
      v.vehicleNumber,
      { lat: v.lat, lng: v.lng },
    ]),
  );
}

// Build location maps for cities and landmarks from Map View dropdowns
async function fetchLocationMaps(page: Page): Promise<LocationMaps> {
  await page.waitForFunction(
    () => {
      const w = window as any;
      const c = w.angularComponentRef?.component;
      return (
        c &&
        Array.isArray(c.dropdownCityList) &&
        c.dropdownCityList.length > 0 &&
        Array.isArray(c.dropdownLandmarkList) &&
        c.dropdownLandmarkList.length > 0
      );
    },
    { timeout: 30000 },
  );

  type WireEntry = {
    key: string;
    rawName: string;
    lat: number | null;
    lng: number | null;
  };
  type WireMaps = {
    city: WireEntry[];
    landmarkByCode: WireEntry[];
    landmarkByName: WireEntry[];
  };

  const wire: WireMaps = await page.evaluate(() => {
  // IMPORTANT: tsx/esbuild can inject calls like __name(fn,"fn") into this function body.
  // In the browser context, __name does not exist, so we define a no-op version here.
  // This prevents: ReferenceError: __name is not defined
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const __name = (target: any, _value: string) => target;

  const w = window as any;
  const c = w.angularComponentRef.component;

  const norm = (s: string): string => {
    return String(s || '')
      .toUpperCase()
      .replace(/\s+/g, '')
      .replace(/-/g, '')
      .replace(/\./g, '')
      .replace(/_/g, '');
  };

  const extractCodeInParens = (name: string): string => {
    const str = String(name || '');
    const matches = str.match(/\(([^)]+)\)/g);
    if (!matches || !matches.length) return '';
    const last = matches[matches.length - 1];
    return last.replace(/[()]/g, '').trim();
  };

  const cityList: any[] = c.dropdownCityList || [];
  const landmarkList: any[] = c.dropdownLandmarkList || [];

  const city = cityList
    .map((item) => {
      const rawName = String(item.itemName || '');
      const lat = typeof item.latitude === 'number' ? item.latitude : null;
      const lng = typeof item.longitude === 'number' ? item.longitude : null;
      const key = norm(rawName);
      return { key, rawName, lat, lng };
    })
    .filter((x) => x.lat != null && x.lng != null);

  const landmarkByCode: any[] = [];
  const landmarkByName: any[] = [];

  for (const lm of landmarkList) {
    const rawName = String(lm.itemName || '');
    const lat = typeof lm.lat === 'number' ? lm.lat : null;
    const lng = typeof lm.lng === 'number' ? lm.lng : null;
    if (lat == null || lng == null) continue;

    const normName = norm(rawName);
    landmarkByName.push({ key: normName, rawName, lat, lng });

    const code = extractCodeInParens(rawName);
    if (code) {
      const normCode = norm(code);
      landmarkByCode.push({ key: normCode, rawName, lat, lng });
    }
  }

  return { city, landmarkByCode, landmarkByName };
});


  const cityByNormName = new Map<string, LocationEntry>();
  wire.city.forEach((c) => {
    if (c.lat == null || c.lng == null) return;
    if (!cityByNormName.has(c.key)) {
      cityByNormName.set(c.key, {
        rawName: c.rawName,
        lat: c.lat,
        lng: c.lng,
      });
    }
  });

  const landmarkByNormCode = new Map<string, LocationEntry>();
  wire.landmarkByCode.forEach((lm) => {
    if (lm.lat == null || lm.lng == null) return;
    if (!landmarkByNormCode.has(lm.key)) {
      landmarkByNormCode.set(lm.key, {
        rawName: lm.rawName,
        lat: lm.lat,
        lng: lm.lng,
      });
    }
  });

  const landmarkByNormName = new Map<string, LocationEntry>();
  wire.landmarkByName.forEach((lm) => {
    if (lm.lat == null || lm.lng == null) return;
    if (!landmarkByNormName.has(lm.key)) {
      landmarkByNormName.set(lm.key, {
        rawName: lm.rawName,
        lat: lm.lat,
        lng: lm.lng,
      });
    }
  });

  console.log(
    `Location maps built: cities=${cityByNormName.size}, landmarksByCode=${landmarkByNormCode.size}, landmarksByName=${landmarkByNormName.size}`,
  );

  return {
    cityByNormName,
    landmarkByNormCode,
    landmarkByNormName,
  };
}

// Override route-level source/destination lat/lng using FMS location maps,
// but only as:
//   final = FMS value if available, else DB value, else null.
function attachLocationCoordsFromFms(
  merged: LiveMergedRow[],
  loc: LocationMaps,
): void {
  for (const row of merged) {
    const consigner = row.consignerName;
    const consignee = row.consigneeName;

    // ---------- FMS candidate coordinates ----------
    let fmsSrcLat: number | null = null;
    let fmsSrcLng: number | null = null;
    let fmsDestLat: number | null = null;
    let fmsDestLng: number | null = null;

    // Source (consigner) -> city first
    const srcCityRaw = extractCityFromConsignerName(consigner);
    if (srcCityRaw) {
      const key = normalizeLocationKey(srcCityRaw);
      const cityEntry = loc.cityByNormName.get(key);
      if (cityEntry) {
        fmsSrcLat = cityEntry.lat;
        fmsSrcLng = cityEntry.lng;
      } else {
        const lmEntry = loc.landmarkByNormName.get(key);
        if (lmEntry) {
          fmsSrcLat = lmEntry.lat;
          fmsSrcLng = lmEntry.lng;
        }
      }
    }

    // Destination (consignee) -> branch code, then city fallback
    const destBranchCode = extractBranchCodeFromConsignee(consignee);
    if (destBranchCode) {
      const codeKey = normalizeLocationKey(destBranchCode);
      const lmByCode = loc.landmarkByNormCode.get(codeKey);
      if (lmByCode) {
        fmsDestLat = lmByCode.lat;
        fmsDestLng = lmByCode.lng;
      }
    }

    if (fmsDestLat == null || fmsDestLng == null) {
      const destCityRaw = extractCityFromConsigneeName(consignee);
      if (destCityRaw) {
        const cityKey = normalizeLocationKey(destCityRaw);
        const cityEntry = loc.cityByNormName.get(cityKey);
        if (cityEntry) {
          fmsDestLat = cityEntry.lat;
          fmsDestLng = cityEntry.lng;
        } else {
          const lmByName = loc.landmarkByNormName.get(cityKey);
          if (lmByName) {
            fmsDestLat = lmByName.lat;
            fmsDestLng = lmByName.lng;
          }
        }
      }
    }

    // ---------- Final values: FMS first, then DB, else null ----------

    const dbSrcLat =
      typeof row.routeSourceLat === 'number' ? row.routeSourceLat : null;
    const dbSrcLng =
      typeof row.routeSourceLng === 'number' ? row.routeSourceLng : null;
    const dbDestLat =
      typeof row.routeDestinationLat === 'number'
        ? row.routeDestinationLat
        : null;
    const dbDestLng =
      typeof row.routeDestinationLng === 'number'
        ? row.routeDestinationLng
        : null;

    const finalSrcLat =
      (typeof fmsSrcLat === 'number' ? fmsSrcLat : null) ?? dbSrcLat ?? null;
    const finalSrcLng =
      (typeof fmsSrcLng === 'number' ? fmsSrcLng : null) ?? dbSrcLng ?? null;
    const finalDestLat =
      (typeof fmsDestLat === 'number' ? fmsDestLat : null) ?? dbDestLat ?? null;
    const finalDestLng =
      (typeof fmsDestLng === 'number' ? fmsDestLng : null) ?? dbDestLng ?? null;

    row.routeSourceLat = finalSrcLat;
    row.routeSourceLng = finalSrcLng;
    row.routeDestinationLat = finalDestLat;
    row.routeDestinationLng = finalDestLng;
  }
}

// ------------------- NEW: fuzzy location matching helpers (Location table) -------------------

type LocationRow = { name: string; Latitude: number; Longitude: number };

type LocationMatchScore = {
  score: number; // 0..1
  byCode: boolean;
};

// Basic text normalisation for location names
function normalizeBaseName(s: string): string {
  return String(s || '')
    .toLowerCase()
    .replace(/\s*\(/g, ' (') // ensure space before "("
    .replace(/[^a-z0-9()\s]+/g, ' ') // keep only alnum, space, and ()
    .replace(/\s+/g, ' ')
    .trim();
}

// Extract last code in parentheses, e.g. "(AML11)" -> "aml11"
function extractCodeLower(name: string): string {
  const str = normalizeBaseName(name);
  const matches = str.match(/\(([^)]+)\)/g);
  if (!matches || !matches.length) return '';
  const last = matches[matches.length - 1];
  return last.replace(/[()]/g, '').trim().toLowerCase();
}

// Tokenise name (excluding code part)
function tokenizeNameWithoutCode(name: string): string[] {
  let norm = normalizeBaseName(name);

  // remove parentheses content to avoid mixing code into tokens
  norm = norm.replace(/\([^)]*\)/g, ' ');

  const tokens = norm.split(/\s+/).filter(Boolean);

  // optional: remove very generic stopwords
  const stopwords = new Set(['at', 'hub', 'the', 'pvt', 'ltd']);
  return tokens.filter((t) => !stopwords.has(t));
}

// Jaccard similarity between two token sets
function jaccardSimilarity(tokensA: string[], tokensB: string[]): number {
  const setA = new Set(tokensA);
  const setB = new Set(tokensB);

  let common = 0;
  for (const t of setA) {
    if (setB.has(t)) common++;
  }

  const unionSize = setA.size + setB.size - common;
  if (unionSize === 0) return 0;
  return common / unionSize;
}

function computeLocationMatchScore(
  endpointName: string,
  candidateName: string,
): LocationMatchScore {
  const code1 = extractCodeLower(endpointName);
  const code2 = extractCodeLower(candidateName);

  // Hard match on code, if both non-empty and equal
  if (code1 && code2 && code1 === code2) {
    return { score: 1.0, byCode: true };
  }

  const tokens1 = tokenizeNameWithoutCode(endpointName);
  const tokens2 = tokenizeNameWithoutCode(candidateName);

  const score = jaccardSimilarity(tokens1, tokens2);
  return { score, byCode: false };
}

// Given an endpoint name, find best matching Location row using:
// 1) Exact name match (case-insensitive)
// 2) Exact name match with spaces removed
// 3) Code match / token Jaccard (≥ threshold)
// Build a "core" search key from endpoint name (source/destination)
function buildEndpointCoreKey(name: string): string {
  let s = String(name || '').toLowerCase();

  // remove anything in parentheses
  s = s.replace(/\([^)]*\)/g, ' ');

  // replace hyphens with space (main symbol)
  s = s.replace(/-/g, ' ');

  // remove other symbols
  s = s.replace(/[^a-z0-9\s]+/g, ' ');

  // collapse spaces
  s = s.replace(/\s+/g, ' ').trim();

  // remove 'safexpress' prefix
  s = s.replace(/^safexpress\s+/, '');

  // remove suffixes: hub, sds, inbound, outbound
  s = s.replace(/\s+(hub|sds|inbound|outbound)\s*$/g, '');

  // collapse spaces again
  s = s.replace(/\s+/g, ' ').trim();

  // finally remove all spaces → pure core
  s = s.replace(/\s+/g, '');

  return s;
}

// Build a comparable core from Location.name
function buildLocationCoreKey(name: string): string {
  let s = String(name || '').toLowerCase();

  // remove anything in parentheses
  s = s.replace(/\([^)]*\)/g, ' ');

  // replace hyphens with space
  s = s.replace(/-/g, ' ');

  // remove other symbols
  s = s.replace(/[^a-z0-9\s]+/g, ' ');

  // remove all spaces
  s = s.replace(/\s+/g, '');

  return s;
}

function findBestLocationMatch(
  endpointName: string,
  locs: LocationRow[],
  threshold: number = 0.9,
): LocationRow | null {
  if (!endpointName.trim() || !locs.length) return null;

  const endpointTrim = endpointName.trim();

  // 1) Exact match (case-insensitive, normal spaces)
  const endpointNorm = endpointTrim.toLowerCase();
  const exact = locs.find(
    (l) => l.name.trim().toLowerCase() === endpointNorm,
  );
  if (exact) return exact;

  // 2) Exact match AFTER removing all spaces (case-insensitive)
  const endpointNoSpace = endpointNorm.replace(/\s+/g, '');
  const exactNoSpace = locs.find((l) => {
    const nameNoSpace = l.name.trim().toLowerCase().replace(/\s+/g, '');
    return nameNoSpace === endpointNoSpace;
  });
  if (exactNoSpace) return exactNoSpace;

  // 3) SEARCH-STYLE MATCH:
  //    string1 = endpointCore (source/dest cleaned)
  //    string2 = locCore (Location.name cleaned)
  //    condition: string1 is inside string2
  const endpointCore = buildEndpointCoreKey(endpointName);
  if (endpointCore) {
    const searchMatch = locs.find((l) => {
      // Remove spaces from Location.name FIRST (your requirement)
      const locNoSpace = l.name.toLowerCase().replace(/\s+/g, '');

      // Also create the full cleaned core version of Location.name
      const locCore = buildLocationCoreKey(l.name);

      // Check both:
      // 1) raw Location.name without spaces includes endpointCore
      // 2) fully cleaned location core includes endpointCore
      return locNoSpace.includes(endpointCore) || locCore.includes(endpointCore);
    });

    if (searchMatch) return searchMatch;
  }


  // 4) If still nothing: fall back to code + Jaccard logic
  let best: LocationRow | null = null;
  let bestScore = 0;
  let bestByCode = false;

  for (const loc of locs) {
    const { score, byCode } = computeLocationMatchScore(
      endpointName,
      loc.name,
    );

    const isBetter =
      score > bestScore ||
      (byCode && !bestByCode && score === bestScore);

    if (isBetter) {
      best = loc;
      bestScore = score;
      bestByCode = byCode;
    }
  }

  if (!best) return null;
  if (bestScore < threshold) return null;

  return best;
}


// ------------------- NEW: fallback from Location table -------------------

// When route coords are still null after FMS and Route DB coords,
// fill them using Location table with fuzzy matching strategy.
async function enrichLiveRoutesWithLocation(
  liveRoutes: LiveRouteRoute[],
): Promise<void> {
  if (!liveRoutes.length) {
    console.log('No live routes to enrich from Location table.');
    return;
  }

  // Fetch all locations (we need whole list for fuzzy matching)
  const locs = await prisma.location.findMany({
    select: { name: true, Latitude: true, Longitude: true },
  });

  if (!locs.length) {
    console.log('No Location rows found in DB for enrichment.');
    return;
  }

  for (const r of liveRoutes) {
    if (r.source && (r.sourceLat == null || r.sourceLng == null)) {
      const bestSrc = findBestLocationMatch(r.source, locs, 0.9);
      if (bestSrc) {
        r.sourceLat = bestSrc.Latitude;
        r.sourceLng = bestSrc.Longitude;
      }
    }

    if (r.destination && (r.destLat == null || r.destLng == null)) {
      const bestDest = findBestLocationMatch(r.destination, locs, 0.9);
      if (bestDest) {
        r.destLat = bestDest.Latitude;
        r.destLng = bestDest.Longitude;
      }
    }
  }

  console.log(
    'Location enrichment with fuzzy matching completed for',
    liveRoutes.length,
    'routes.',
  );
}

async function syncLiveRoutesToDb(liveRoutes: LiveRouteRoute[]) {
  const now = new Date();

  // Before saving, enrich missing coords from Location table (fuzzy)
  await enrichLiveRoutesWithLocation(liveRoutes);

  // ----- 1) Prepare LiveRoute rows in memory -----
  const routesData: Prisma.LiveRouteCreateManyInput[] =
    liveRoutes.map((r) => ({
      id: r.id, // route.id from your aggregation
      source: r.source,
      destination: r.destination,
      sourceLat: r.sourceLat ?? null,
      sourceLng: r.sourceLng ?? null,
      destLat: r.destLat ?? null,
      destLng: r.destLng ?? null,
      updatedAt: now,
    }));

  // ----- 2) Prepare LiveVehicle rows in memory -----
  const vehiclesData: Prisma.LiveVehicleCreateManyInput[] = [];

  for (const route of liveRoutes) {
    const pushSide = (side: 'Up' | 'Down', bucket: LiveRouteSideBucket) => {
      for (const v of bucket.vehicles) {
        vehiclesData.push({
          id: randomUUID(), // LiveVehicle.id has no default in schema
          vehicleNumber: v.vehicleNumber ?? null,
          rpsNumber: v.rpsNumber ?? null,
          dispatchDate: safeDate(v.dispatchDate),
          lastLocationDate: safeDate(v.lastLocationDate),
          lastLocation: v.lastLocation ?? null,
          lat: v.lat ?? null,
          lng: v.lng ?? null,
          expectedArrival: safeDate(v.expectedArrival),
          lateHours: v.lateHours ?? null,
          middleStops: v.middleStops ?? [],
          direction: side === 'Up' ? TripSide.Up : TripSide.Down,
          routeId: route.id,
        });
      }
    };

    // Trip side Up/Down kept exactly as before
    pushSide('Up', route.up);
    pushSide('Down', route.down);
  }

  console.log(
    `Prepared ${routesData.length} LiveRoute rows and ${vehiclesData.length} LiveVehicle rows.`,
  );

  // ----- 3) Atomic snapshot write (transaction) -----
  await prisma.$transaction(async (tx) => {
    // delete children first because of FK routeId → LiveRoute.id
    await tx.liveVehicle.deleteMany({});
    await tx.liveRoute.deleteMany({});

    if (routesData.length > 0) {
      await tx.liveRoute.createMany({ data: routesData });
    }

    if (vehiclesData.length > 0) {
      await tx.liveVehicle.createMany({ data: vehiclesData });
    }
  });

  console.log('Live route snapshot saved to database (transaction committed).');
}

// ------------------- DATE PARSING HELPERS -------------------

// Parse known date-time formats into JS Date:
// 1) "YYYY-MM-DD HH:MM:SS"   (Last Location Date)
// 2) "DD/MM/YYYY HH:MM:SS"   (Dispatch Date, ETA)
function parseCustomDateTimeString(raw: string): Date | null {
  const value = raw.trim();
  if (!value) return null;

  // 1) YYYY-MM-DD HH:MM:SS
  const isoLike =
    /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})$/;
  const isoMatch = value.match(isoLike);
  if (isoMatch) {
    const [, y, m, d, hh, mm, ss] = isoMatch;
    const year = parseInt(y, 10);
    const month = parseInt(m, 10) - 1;
    const day = parseInt(d, 10);
    const hour = parseInt(hh, 10);
    const minute = parseInt(mm, 10);
    const second = parseInt(ss, 10);
    const date = new Date(year, month, day, hour, minute, second);
    if (!isNaN(date.getTime())) return date;
  }

  // 2) DD/MM/YYYY HH:MM:SS
  const dmyLike =
    /^(\d{2})\/(\d{2})\/(\d{4})[ T](\d{2}):(\d{2}):(\d{2})$/;
  const dmyMatch = value.match(dmyLike);
  if (dmyMatch) {
    const [, dd, mm, yyyy, hh, mi, ss] = dmyMatch;
    const day = parseInt(dd, 10);
    const month = parseInt(mm, 10) - 1;
    const year = parseInt(yyyy, 10);
    const hour = parseInt(hh, 10);
    const minute = parseInt(mi, 10);
    const second = parseInt(ss, 10);
    const date = new Date(year, month, day, hour, minute, second);
    if (!isNaN(date.getTime())) return date;
  }

  // Fallback: let JS try
  const fallback = new Date(value);
  if (!isNaN(fallback.getTime())) return fallback;

  console.warn('Could not parse date-time string:', raw);
  return null;
}

function safeDate(value: string | Date | null | undefined): Date | null {
  if (!value) return null;

  if (value instanceof Date) {
    return isNaN(value.getTime()) ? null : value;
  }

  if (typeof value === 'string') {
    const parsed = parseCustomDateTimeString(value);
    if (parsed) return parsed;
    console.warn('Invalid date string encountered:', value, '→ storing null');
    return null;
  }

  // Any other type is unexpected; try generic parsing then.
  const d = new Date(value as any);
  if (isNaN(d.getTime())) {
    console.warn('Invalid date value encountered:', value, '→ storing null');
    return null;
  }

  return d;
}

// ------------------- MAIN SCRAPER -------------------

async function main() {
  const headless = (process.env.HEADLESS ?? 'true').toLowerCase() === 'true';
  const browser = await chromium.launch({ headless });
  const context = await browser.newContext({ acceptDownloads: true });
  
  // FIX: tsx/esbuild injects __name(...) into evaluated functions.
  // Define __name globally inside the page context so page.evaluate never crashes.
  await context.addInitScript({
    content: 'var __name = (target, name) => target;'
  });
  
  const page = await context.newPage();


  console.log('Opening login page:', FMS_URL);
  await page.goto(FMS_URL, { waitUntil: 'networkidle' });

  // ---- LOGIN ----
  console.log('Filling login form…');

  // Username
  try {
    await page.getByPlaceholder(/user/i).fill(USERNAME_STR);
  } catch {
    await page.fill('input[type="text"]', USERNAME_STR);
  }

  // Password
  try {
    await page.getByPlaceholder(/password/i).fill(PASSWORD_STR);
  } catch {
    await page.fill('input[type="password"]', PASSWORD_STR);
  }

  // Submit
  try {
    await page.click('#mysubmit');
  } catch {
    try {
      await page.getByRole('button', { name: /sign in/i }).click();
    } catch {
      await page.click('button.signinbutton');
    }
  }

  await page.waitForNavigation({ waitUntil: 'networkidle' });
  console.log('Logged in, waiting for dashboard…');
  await page.waitForTimeout(3000);

  // ---- GRID VIEW (hover card then click) ----
  console.log('Hovering on the "On Trip Vehicles" card and clicking Grid View…');

  const onTripCard = page.locator('#onTrip');

  await onTripCard.hover();
  await page.waitForTimeout(700); // let the hover effect show the button

  const gridViewButton = onTripCard.locator('button#dropdownMenuButton');
  await gridViewButton.waitFor({ state: 'visible', timeout: 5000 });
  await gridViewButton.click();

  console.log('Grid View clicked, waiting for grid to load…');
  await page.waitForTimeout(2000);

  // ---- DOWNLOAD EXCEL (in memory) ----
  console.log('Clicking "Download Excel" button…');

  const [download] = await Promise.all([
    page.waitForEvent('download'),
    page.locator('div.tool-bar-btn[title="Download Excel"]').click(),
  ]);

  const stream = await download.createReadStream();
  if (!stream) {
    console.error('Failed to get download stream');
    await browser.close();
    return;
  }

  const chunks: Uint8Array[] = [];
  for await (const chunk of stream as any) {
    chunks.push(chunk);
  }
  const excelBuffer = Buffer.concat(chunks);

  const workbook = XLSX.read(excelBuffer, { type: 'buffer' });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];

  const rows: any[] = XLSX.utils.sheet_to_json(sheet, { header: 1 });

  // ---- PARSE EXCEL ----
  const records = parseRows(rows);

  console.log(`Extracted ${records.length} rows from Excel.`);

  // ---- MERGE WITH ROUTE MASTER (DB) ----
  const routeIndex = await buildRouteIndex();
  const merged = mergeWithRoutes(records, routeIndex);

  // ---- FETCH LIVE VEHICLE COORDINATES FROM MAP VIEW ----
  const coordsByVehicle = await fetchLiveVehicleCoords(page);

  // ---- BUILD LOCATION MAPS (cities + landmarks) FROM MAP VIEW ----
  const locationMaps = await fetchLocationMaps(page);

  // ---- APPLY FMS COORDS (FMS first, then DB, else null) ----
  attachLocationCoordsFromFms(merged, locationMaps);

  // ---- AGGREGATE INTO FINAL PER-ROUTE STRUCTURE (ONLY MATCHED) ----
  const liveRoutes: LiveRouteRoute[] = aggregateLiveRoutes(
    merged,
    coordsByVehicle,
  );

  console.log('All live routes (matched):');
  console.dir(liveRoutes, { depth: null, maxArrayLength: null });

  // ---- COLLECT UNMATCHED ROUTE PAIRS FOR LATER FIXING ----
  const unmatchedRoutes = collectUnmatched(merged);
  console.log(
    `\nUnmatched route pairs (${unmatchedRoutes.length} unique):`,
  );
  console.dir(unmatchedRoutes, { depth: null, maxArrayLength: null });

  // ---- STORE UNMATCHED ROUTES INTO UnmatchedTripSide (unique RouteName) ----
  await saveUnmatchedToDb(unmatchedRoutes);

  await syncLiveRoutesToDb(liveRoutes);

  await browser.close();

}

// ------------------- RUN -------------------

main()
  .catch((err) => {
    console.error('Scraper error:', err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
