// tasks/generateVisionJson.js
// Build ./processed/{productId}.vision.json per unique Insertion Order.
// Uses:
//   - rawData/device.csv
//   - intermediate/gender.deaggregated.jsonl
//   - rawData/unique.csv
//   - rawData/categories.csv
//   - intermediate/categoryscored.jsonl
//
// Behavior highlights:
// - DV footer: stop parsing CSVs at lines starting with "Report Time" (case-insensitive).
// - productId = first segment of "Insertion Order" split by '_'.
// - Devices: Tablet + Smart Phone => Mobile; compute % of impressions (not by date).
//   If any device % < config minimum, roll those % into the highest device bucket.
// - Demo: from gender.deaggregated.jsonl, compute percentages (0–100) with 4 decimals,
//   across the whole insertion order (not by date). Only output requested fields.
// - Per-day & Totals: from unique.csv, compute daily metrics and grand totals.
// - Key properties: from categories.csv, per App/URL sums.
// - Content taxonomy: from intermediate/categoryscored.jsonl,
//   * campaign_delivery: per date per iabId -> {id,date,name,value (rounded int), percent (4d)}
//   * audience_distribution: totals across dates per iabId similarly.
//
// Output:
//   { data: { products: { [productId]: { byDevices, totals, entities: [], keyProperties: [], demo: {...},
//       contentTaxonomy: { audience_distribution: [], campaign_delivery: [], campaign_interactions: [] },
//       perDay: [] }}}}
//
// CLI:
//   --provider [dv|ttd|zed]  (default 'dv')  // for footer rule consistency

import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import config from '../config.js';
import { ensureDirSync, fileExistsSync } from '../utils/fs.js';
import { applyCommonArgs } from '../utils/argumentos.js';

const argv = applyCommonArgs(
  yargs(hideBin(process.argv))
).strict().argv;

// --- Paths & config
const RAW_DIR = config.paths?.raw ?? './rawData';
const INTERMEDIATE_DIR = config.paths?.intermediate ?? './intermediate';
const PROCESSED_DIR = config.paths?.processed ?? './processed';
const ENCODING = config.csv?.encoding ?? 'utf8';
const DELIM = config.csv?.delimiter ?? ',';
const DEVICE_MIN_PCT = (config.devices?.minPct ?? config.deviceMinPct ?? 1.0); // percentage points

const DEVICE_CSV = path.join(RAW_DIR, 'device.csv');
const UNIQUE_CSV = path.join(RAW_DIR, 'unique.csv');
const CATEGORIES_CSV = path.join(RAW_DIR, 'categories.csv');
const DEMO_JSONL = path.join(INTERMEDIATE_DIR, 'gender.deaggregated.jsonl');
const IAB_SCORED_JSONL = path.join(INTERMEDIATE_DIR, 'categoryscored.jsonl');

// --- Small helpers
function splitCSV(line, delimiter) {
  if (!line.includes('"')) return line.split(delimiter);
  const out = []; let cur = ''; let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') { cur += '"'; i++; } else inQuotes = !inQuotes;
      continue;
    }
    if (ch === delimiter && !inQuotes) { out.push(cur); cur = ''; continue; }
    cur += ch;
  }
  out.push(cur);
  return out;
}
function parseNumber(x) {
  if (x == null) return 0;
  const n = Number(String(x).replace(/,/g, '').trim());
  return Number.isFinite(n) ? n : 0;
}
function isDVFooter(line) {
  if (argv.provider !== 'dv') return false;
  return /^\s*report time\b/i.test(line);
}
function normDate(s) {
  const str = String(s || '').trim();
  let m = str.match(/^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$/);
  if (m) {
    const [_, y, mo, d] = m;
    return `${y}-${String(mo).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
    }
  m = str.match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$/); // mm/dd/yyyy
  if (m) {
    const [_, mo, d, y] = m;
    return `${y}-${String(mo).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
  }
  return str; // fallback
}
function round4(x) {
  return Number.isFinite(x) ? Number(x.toFixed(4)) : 0;
}
function pct4(num, den) {
  return den > 0 ? round4((num / den) * 100) : 0;
}
function productIdFromInsertionOrder(io) {
  const s = String(io || '');
  const idx = s.indexOf('_');
  return idx >= 0 ? s.slice(0, idx) : s;
}
function normalizeDeviceType(dtRaw) {
  const s = String(dtRaw || '').toLowerCase().trim();
  if (!s) return 'Other';
  // Common aliases
  if (s.includes('ctv') || (s.includes('connected') && s.includes('tv'))) return 'CTV';
  if (s.includes('tablet')) return 'Tablet';
  if (s.replace(/\s+/g, '').includes('smartphone') || (s.includes('smart') && s.includes('phone'))) return 'Smart Phone';
  if (s === 'mobile') return 'Mobile';
  if (s.includes('desktop')) return 'Desktop';
  if (s.includes('phone')) return 'Smart Phone';
  return dtRaw; // keep as-is (original casing)
}

// --- Data containers
// products[productId] = { byDevices, totals, entities, keyProperties, demo, contentTaxonomy, perDay }
const products = new Map();

// Ensure structure
function ensureProduct(pid) {
  if (!products.has(pid)) {
    products.set(pid, {
      byDevices: {},
      totals: {},
      entities: [],
      keyProperties: [],
      demo: {},
      contentTaxonomy: {
        audience_distribution: [],
        campaign_delivery: [],
        campaign_interactions: []
      },
      perDay: []
    });
  }
  return products.get(pid);
}

// --- DEVICE.CSV
async function ingestDevices() {
  if (!fileExistsSync(DEVICE_CSV)) return;
  const stream = fs.createReadStream(DEVICE_CSV, ENCODING);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let lineNo = 0;
  let idx = { insertionOrder: 0, date: 1, deviceType: 2, impressions: 3 };
  const perProductImps = new Map(); // pid -> total imps
  const perProductDeviceImps = new Map(); // pid -> Map<Device, imps>

  for await (const rawLine of rl) {
    const line = rawLine ?? '';
    if (isDVFooter(line)) break;
    if (!line.trim()) continue;

    lineNo += 1;
    const cols = splitCSV(line, DELIM);

    if (lineNo === 1) {
      const headers = splitCSV(line, DELIM).map(h => String(h).trim().toLowerCase());
      const map = {
        'insertion order': 'insertionOrder',
        'date': 'date',
        'device type': 'deviceType',
        'impressions': 'impressions'
      };
      let found = false;
      for (let i = 0; i < headers.length; i++) {
        const k = map[headers[i]];
        if (k) { idx[k] = i; found = true; }
      }
      if (found && headers.includes('device type')) continue; // header row
    }

    const IO = cols[idx.insertionOrder];
    const pid = productIdFromInsertionOrder(IO);
    const deviceRaw = cols[idx.deviceType];
    const imps = parseNumber(cols[idx.impressions]);
    if (!pid || !imps) continue;

    let bucket = normalizeDeviceType(deviceRaw);
    // Aggregate Tablet & Smart Phone into Mobile
    if (bucket === 'Tablet' || bucket === 'Smart Phone') bucket = 'Mobile';

    const total = (perProductImps.get(pid) ?? 0) + imps;
    perProductImps.set(pid, total);

    const devMap = perProductDeviceImps.get(pid) ?? new Map();
    devMap.set(bucket, (devMap.get(bucket) ?? 0) + imps);
    perProductDeviceImps.set(pid, devMap);
  }

  // Compute percentages & threshold roll-up
  for (const [pid, devMap] of perProductDeviceImps.entries()) {
    const total = perProductImps.get(pid) || 0;
    if (total <= 0) { ensureProduct(pid).byDevices = {}; continue; }

    // raw percentages
    const raw = [];
    for (const [dev, imp] of devMap.entries()) {
      raw.push({ dev, pct: (imp / total) * 100 });
    }
    if (raw.length === 0) { ensureProduct(pid).byDevices = {}; continue; }

    // find max device
    let maxIdx = 0;
    for (let i = 1; i < raw.length; i++) if (raw[i].pct > raw[maxIdx].pct) maxIdx = i;

    // collect smalls
    let smallSum = 0;
    const keep = [];
    for (let i = 0; i < raw.length; i++) {
      const r = raw[i];
      if (r.pct < DEVICE_MIN_PCT && i !== maxIdx) smallSum += r.pct;
      else keep.push(r);
    }
    // add smallSum to max
    if (smallSum > 0) {
      keep[maxIdx >= keep.length ? keep.length - 1 : keep.findIndex(k => k.dev === raw[maxIdx].dev)].pct += smallSum;
    }

    const outObj = {};
    for (const k of keep) outObj[k.dev] = round4(k.pct);
    ensureProduct(pid).byDevices = outObj;
  }
}

// --- DEMO (gender.deaggregated.jsonl)
async function ingestDemo() {
  if (!fileExistsSync(DEMO_JSONL)) return;
  const rl = readline.createInterface({ input: fs.createReadStream(DEMO_JSONL, ENCODING), crlfDelay: Infinity });

  // per PID sums
  const male = new Map(), female = new Map();
  const ages = new Map(); // pid -> { '18-24':imps, '25-34':..., '35-44', '45-54', '55-64', '+65':imps }

  for await (const line of rl) {
    if (!line.trim()) continue;
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }
    
    const io = obj.insertionOrder ?? obj.insertion_order ?? '';
    const pid = productIdFromInsertionOrder(io);
    if (!pid) continue;

    const imps = parseNumber(obj.impressions);
    if (!imps) continue;

    // gender
    const g = String(obj.gender || '').toLowerCase();    
    if (g === 'male') {
      male.set(pid, (male.get(pid) ?? 0) + imps);
    } else if (g==='female') {
          female.set(pid, (female.get(pid) ?? 0) + imps);
    }

    // age bins we track
    const a = String(obj.age || '').trim();
    const aMap = ages.get(pid) ?? { '18-24': 0, '25-34': 0, '35-44': 0, '45-54': 0, '55-64': 0, '+65': 0 };
    if (a === '18-24') aMap['18-24'] += imps;
    else if (a === '25-34') aMap['25-34'] += imps;
    else if (a === '35-44') aMap['35-44'] += imps;
    else if (a === '45-54') aMap['45-54'] += imps;
    else if (a === '55-64') aMap['55-64'] += imps;
    else if (a === '+65' || a === '65+') aMap['+65'] += imps;
    ages.set(pid, aMap);
  }

  // compute percentages with corrected denominators
  for (const pid of new Set([...male.keys(), ...female.keys(), ...ages.keys()])) {
    const m = male.get(pid) ?? 0;
    const f = female.get(pid) ?? 0;
    
    const genderDen = m + f;

    const aMap = ages.get(pid) ?? { '18-24': 0, '25-34': 0, '35-44': 0, '45-54': 0, '55-64': 0, '+65': 0 };
    const ageDen = (aMap['18-24'] + aMap['25-34'] + aMap['35-44'] + aMap['45-54'] + aMap['55-64'] + aMap['+65']);

    const d = {
      gender_male: genderDen > 0 ? pct4(m, genderDen) : 0,
      gender_female: genderDen > 0 ? pct4(f, genderDen) : 0,
      age_18_24: ageDen > 0 ? pct4(aMap['18-24'], ageDen) : 0,
      age_25_34: ageDen > 0 ? pct4(aMap['25-34'], ageDen) : 0,
      age_35_44: ageDen > 0 ? pct4(aMap['35-44'], ageDen) : 0,
      age_45_54: ageDen > 0 ? pct4(aMap['45-54'], ageDen) : 0,
      age_55_64: ageDen > 0 ? pct4(aMap['55-64'], ageDen) : 0,
      age_65:     ageDen > 0 ? pct4(aMap['+65'],    ageDen) : 0
    };

    ensureProduct(pid).demo = d;
  }
}

// --- UNIQUE.CSV (perDay + totals)
async function ingestUnique() {
  if (!fileExistsSync(UNIQUE_CSV)) return;
  const rl = readline.createInterface({ input: fs.createReadStream(UNIQUE_CSV, ENCODING), crlfDelay: Infinity });

  let lineNo = 0;
  let idx = {
    insertionOrder: 0, date: 1,
    impressions: 2, clicks: 3, viewableImps: 4,
    starts: 6, v25: 7, v50: 8, v75: 9, v100: 10
  };

  // per PID totals and per-day arrays
  const perDay = new Map(); // pid -> array of day objects
  const totals = new Map(); // pid -> totals object

  for await (const rawLine of rl) {
    const line = rawLine ?? '';
    if (isDVFooter(line)) break;
    if (!line.trim()) continue;
    lineNo += 1;
    const cols = splitCSV(line, DELIM);

    if (lineNo === 1) {
      const headers = splitCSV(line, DELIM).map(h => String(h).trim().toLowerCase());
      const map = {
        'insertion order': 'insertionOrder',
        'date': 'date',
        'impressions': 'impressions',
        'clicks': 'clicks',
        'viewable impressions': 'viewableImps',
        'unique impression': 'unique', // not used
        'video_starts': 'starts',
        'video_views25': 'v25',
        'video_views50': 'v50',
        'video_views75': 'v75',
        'video_views100': 'v100'
      };
      let found = false;
      for (let i = 0; i < headers.length; i++) {
        const k = map[headers[i]];
        if (k && k in idx) { idx[k] = i; found = true; }
      }
      if (found && headers.includes('date')) continue; // header row
    }

    const IO = cols[idx.insertionOrder];
    const pid = productIdFromInsertionOrder(IO);
    if (!pid) continue;

    const date = normDate(cols[idx.date]);
    const impressions = parseNumber(cols[idx.impressions]);
    const clicks = parseNumber(cols[idx.clicks]);
    const viewable = parseNumber(cols[idx.viewableImps]);
    const starts = parseNumber(cols[idx.starts]);
    const v25 = parseNumber(cols[idx.v25]);
    const v50 = parseNumber(cols[idx.v50]);
    const v75 = parseNumber(cols[idx.v75]);
    const v100 = parseNumber(cols[idx.v100]);

    const dayObj = {
      analytic_engagements: 0,
      analytic_engagementsPercent: 0,
      analytic_date: date,
      analytic_viewability: pct4(viewable, impressions),
      analytic_uniqueUsers: 0,
      analytic_views: v100,
      analytic_views25: v25,
      analytic_views50: v50,
      analytic_views75: v75,
      analytic_vtr: pct4(v100, starts),
      analytic_ctr: pct4(clicks, impressions),
      analytic_impressions: impressions,
      analytic_clicks: clicks
    };
    const arr = perDay.get(pid) ?? [];
    arr.push(dayObj);
    perDay.set(pid, arr);

    const tot = totals.get(pid) ?? {
      impressions: 0, clicks: 0, viewable: 0, starts: 0, v25: 0, v50: 0, v75: 0, v100: 0
    };
    tot.impressions += impressions;
    tot.clicks += clicks;
    tot.viewable += viewable;
    tot.starts += starts;
    tot.v25 += v25;
    tot.v50 += v50;
    tot.v75 += v75;
    tot.v100 += v100;
    totals.set(pid, tot);
  }

  // Assign into products
  for (const [pid, arr] of perDay.entries()) {
    // sort by date asc for stability
    arr.sort((a, b) => String(a.analytic_date).localeCompare(String(b.analytic_date)));
    ensureProduct(pid).perDay = arr;
  }
  for (const [pid, t] of totals.entries()) {
    ensureProduct(pid).totals = {
      analytic_engagements: 0,
      analytic_engagementsPercent: 0,
      analytic_viewability: pct4(t.viewable, t.impressions),
      analytic_uniqueUsers:0,
      analytic_views: t.v100,
      analytic_views25: t.v25,
      analytic_views50: t.v50,
      analytic_views75: t.v75,
      analytic_vtr: pct4(t.v100, t.starts),
      analytic_ctr: pct4(t.clicks, t.impressions),
      analytic_impressions: t.impressions,
      analytic_clicks: t.clicks
    };
  }
}

// --- CATEGORIES.CSV (keyProperties)
async function ingestKeyProps() {
  if (!fileExistsSync(CATEGORIES_CSV)) return;
  const rl = readline.createInterface({ input: fs.createReadStream(CATEGORIES_CSV, ENCODING), crlfDelay: Infinity });

  let lineNo = 0;
  let idx = { insertionOrder: 0, date: 1, category: 2, appUrl: 3, impressions: 4, clicks: 5, viewableImps: 6 };
  // pid -> Map(appUrl -> {imp,clk,view})
  const agg = new Map();

  for await (const rawLine of rl) {
    const line = rawLine ?? '';
    if (isDVFooter(line)) break;
    if (!line.trim()) continue;
    lineNo += 1;
    const cols = splitCSV(line, DELIM);

    if (lineNo === 1) {
      const headers = splitCSV(line, DELIM).map(h => String(h).trim().toLowerCase());
      const map = {
        'insertion order': 'insertionOrder',
        'date': 'date',
        'category': 'category',
        'app/url': 'appUrl',
        'impressions': 'impressions',
        'clicks': 'clicks',
        'viewable impressions': 'viewableImps'
      };
      let found = false;
      for (let i = 0; i < headers.length; i++) {
        const k = map[headers[i]];
        if (k) { idx[k] = i; found = true; }
      }
      if (found && headers.includes('category')) continue; // header row
    }

    const IO = cols[idx.insertionOrder];
    const pid = productIdFromInsertionOrder(IO);
    if (!pid) continue;

    const app = String(cols[idx.appUrl] ?? '').trim();
    if (!app) continue;

    const imps = parseNumber(cols[idx.impressions]);
    const clk = parseNumber(cols[idx.clicks]);
    const view = parseNumber(cols[idx.viewableImps]);

    const mapPid = agg.get(pid) ?? new Map();
    const current = mapPid.get(app) ?? { impressions: 0, clicks: 0, viewability: 0 };
    current.impressions += imps;
    current.clicks += clk;
    current.viewability += view; // sum of viewable impressions (as requested)
    mapPid.set(app, current);
    agg.set(pid, mapPid);
  }

  for (const [pid, mapPid] of agg.entries()) {
    const list = [];
    for (const [placement_domain, vals] of mapPid.entries()) {
      list.push({ placement_domain, impressions: vals.impressions, clicks: vals.clicks, viewability: vals.viewability });
    }
    ensureProduct(pid).keyProperties = list;
  }
}

// --- IAB SCORED (campaign_delivery + audience_distribution)
async function ingestIabScored() {
  if (!fileExistsSync(IAB_SCORED_JSONL)) return;
  const rl = readline.createInterface({ input: fs.createReadStream(IAB_SCORED_JSONL, ENCODING), crlfDelay: Infinity });

  // Structures:
  // per pid:
  //   daily: Map(date => Map(iabId => {name, val}))
  //   totals: Map(iabId => {name, val})
  const dailyByPid = new Map();
  const totalsByPid = new Map();

  for await (const line of rl) {
    if (!line.trim()) continue;
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }
    const io = obj.insertionOrder ?? obj.insertion_order ?? '';
    const pid = productIdFromInsertionOrder(io);
    if (!pid) continue;

    const date = normDate(obj.date);
    const iabId = String(obj.iabId ?? obj.iab_id ?? '');
    const name = String(obj.iabcategoryName ?? obj.name ?? '');
    const val = parseNumber(obj.iabscore);
    if (!date || !iabId || !val) continue;

    // daily
    const byDate = dailyByPid.get(pid) ?? new Map();
    const catMap = byDate.get(date) ?? new Map();
    const cur = catMap.get(iabId) ?? { name, val: 0 };
    cur.val += val;
    // prefer latest name if varies
    cur.name = name || cur.name;
    catMap.set(iabId, cur);
    byDate.set(date, catMap);
    dailyByPid.set(pid, byDate);

    // totals
    const totMap = totalsByPid.get(pid) ?? new Map();
    const curTot = totMap.get(iabId) ?? { name, val: 0 };
    curTot.val += val;
    curTot.name = name || curTot.name;
    totMap.set(iabId, curTot);
    totalsByPid.set(pid, totMap);
  }

  // Emit into product objects
  for (const [pid, byDate] of dailyByPid.entries()) {
    const product = ensureProduct(pid);
    const deliveries = [];
    for (const [date, catMap] of byDate.entries()) {
      let dayTotal = 0;
      for (const v of catMap.values()) dayTotal += v.val;
      if (dayTotal <= 0) continue;
      for (const [id, v] of catMap.entries()) {
        const valueRounded = Math.round(v.val);
        const percent = pct4(v.val, dayTotal);
        deliveries.push({ id, date, name: v.name, value: valueRounded, percent });
      }
    }
    // Sort by date asc for stability
    deliveries.sort((a, b) => String(a.date).localeCompare(String(b.date)) || String(a.id).localeCompare(String(b.id)));
    product.contentTaxonomy.campaign_delivery = deliveries;
  }

  for (const [pid, totMap] of totalsByPid.entries()) {
    const product = ensureProduct(pid);
    let grand = 0; for (const v of totMap.values()) grand += v.val;
    const audience = [];
    if (grand > 0) {
      for (const [id, v] of totMap.entries()) {
        audience.push({ id, name: v.name, value: Math.round(v.val), percent: pct4(v.val, grand) });
      }
    }
    // sort by value desc then name
    audience.sort((a, b) => (b.value - a.value) || String(a.name).localeCompare(String(b.name)));
    product.contentTaxonomy.audience_distribution = audience;
  }
}

// --- WRITE FILES
function writeOutputs() {
  ensureDirSync(PROCESSED_DIR);
  for (const [pid, node] of products.entries()) {
    const out = { data: { products: { [pid]: node } } };
    const file = path.join(PROCESSED_DIR, `${pid}.vision.json`);
    fs.writeFileSync(file, JSON.stringify(out, null, 2), 'utf8'); // pretty with 2 spaces
    console.log(`Wrote ${file}`);
  }
}

// --- MAIN
async function main() {
  await ingestDevices();
  await ingestDemo();
  await ingestUnique();
  await ingestKeyProps();
  await ingestIabScored();
  writeOutputs();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
