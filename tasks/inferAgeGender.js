// tasks/inferAgeGender.js
import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import config from '../config.js';
import { fileExistsSync, ensureDirSync, safeWriteLinesAtomicSync } from '../utils/fs.js';
import { applyCommonArgs } from '../utils/argumentos.js';

const argv = applyCommonArgs(
  yargs(hideBin(process.argv))
).strict().argv;

const RAW_DIR = config.paths?.raw ?? './rawData';
const INTERMEDIATE_DIR = config.paths?.intermediate ?? './intermediate';
const ENCODING = config.csv?.encoding ?? 'utf8';
const DELIM = config.csv?.delimiter ?? ',';

const INPUT_A = path.join(RAW_DIR, 'genders.csv');
const INPUT_B = path.join(RAW_DIR, 'gender.csv');
const INPUT = fileExistsSync(INPUT_A) ? INPUT_A : INPUT_B;

if (!INPUT || !fileExistsSync(INPUT)) {
  console.error(`Missing input file: ${INPUT_A} (or ${INPUT_B})`);
  process.exit(1);
}

const OUT = path.join(INTERMEDIATE_DIR, 'gender.deaggregated.jsonl');

// --- CSV splitter (simple, quote-aware) ---
function splitCSV(line, delimiter) {
  if (!line.includes('"')) return line.split(delimiter);
  const out = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') { cur += '"'; i++; }
      else inQuotes = !inQuotes;
      continue;
    }
    if (ch === delimiter && !inQuotes) { out.push(cur); cur = ''; continue; }
    cur += ch;
  }
  out.push(cur);
  return out;
}

function resolveIndexes(headerLine) {
  let idx = { insertionOrder: 0, date: 1, gender: 2, age: 3, impressions: 4 };
  if (!headerLine) return idx;
  const headers = splitCSV(headerLine, DELIM).map(h => String(h).trim().toLowerCase());
  const map = {
    'insertion order': 'insertionOrder',
    'date': 'date',
    'gender': 'gender',
    'age': 'age',
    'impressions': 'impressions'
  };
  for (let i = 0; i < headers.length; i++) {
    const key = map[headers[i]];
    if (key) idx[key] = i;
  }
  return idx;
}

function isFooterLine(provider, line) {
  if (provider !== 'dv') return false;
  return /^\s*report time\b/i.test(line);
}

// ---- Age parsing & expansion ----
function parseAgeToken(ageStr) {
  const s = String(ageStr).trim();
  if (!s) return { type: 'unknown' };

  if (/^-21$/i.test(s) || /^<=?\s*21$/i.test(s)) return { type: 'under21' };
  if (/^65\+$/i.test(s) || /^>=?\s*65$/i.test(s)) return { type: 'over65' };
  if (/^21\+$/i.test(s)) return { type: '21plus' };

  const m = s.match(/^(\d{1,2})\s*-\s*(\d{1,2})$/);
  if (m) {
    const a = parseInt(m[1], 10);
    const b = parseInt(m[2], 10);
    if (!Number.isNaN(a) && !Number.isNaN(b) && a <= b) {
      return { type: 'range', start: a, end: b };
    }
  }
  return { type: 'unknown', raw: s };
}

// Expand to per-year (or +65 bucket). Returns iterable of {year?: number, plus65?: boolean, impressions}
function* expandAge(ageToken, impressions) {
  switch (ageToken.type) {
    case 'under21': {
      // Expand uniformly to 0..21 (22 "years")
      const start = 0, end = 21;
      const count = end - start + 1;
      const per = impressions / count;
      for (let y = start; y <= end; y++) yield { year: y, impressions: per };
      return;
    }
    case 'over65': {
      yield { plus65: true, impressions };
      return;
    }
    case '21plus': {
      // 21..64 + a single 65+ bucket as one "year" in the split
      const years = [];
      for (let y = 21; y <= 64; y++) years.push(y);
      const denom = years.length + 1; // + one for 65+
      const per = impressions / denom;
      for (const y of years) yield { year: y, impressions: per };
      yield { plus65: true, impressions: per };
      return;
    }
    case 'range': {
      const count = (ageToken.end - ageToken.start + 1);
      const per = impressions / count;
      for (let y = ageToken.start; y <= ageToken.end; y++) {
        yield { year: y, impressions: per };
      }
      return;
    }
    default: {
      // Unknown: pass-through as-is (no split), we cannot sensibly bin -> drop or keep?
      // We'll drop unknowns quietly.
      return;
    }
  }
}

// ---- Binning ----
// Returns one of the labels: '-18','18-24','25-34','35-44','45-54','55-64','+65'
function yearToBin(y) {
  if (y < 18) return '-18';
  if (y <= 24) return '18-24';
  if (y <= 34) return '25-34';
  if (y <= 44) return '35-44';
  if (y <= 54) return '45-54';
  if (y <= 64) return '55-64';
  return '+65';
}

async function main() {
  ensureDirSync(INTERMEDIATE_DIR);

  const stream = fs.createReadStream(INPUT, ENCODING);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let lineNo = 0;
  let indexes = null;

  // Aggregate by insertionOrder|date|gender|ageRange
  const agg = new Map();

  const add = (insertionOrder, date, gender, ageRange, impressions) => {
    const key = [insertionOrder, date, gender, ageRange].join('\u0001');
    const prev = agg.get(key) || 0;
    agg.set(key, prev + impressions);
  };

  for await (const rawLine of rl) {
    const line = rawLine ?? '';

    if (isFooterLine(argv.provider, line)) {
      console.log('Detected DV summary footer; stopping further processing.');
      break;
    }
    if (!line.trim()) continue;

    lineNo += 1;
    const cols = splitCSV(line, DELIM);

    if (lineNo === 1) {
      indexes = resolveIndexes(line);
      const maybeAge = splitCSV(line, DELIM)[indexes.age];
      if (String(maybeAge).trim().toLowerCase() === 'age') continue; // skip header
    }

    const get = (i) => cols[i];

    const insertionOrder = get(indexes.insertionOrder);
    const date = get(indexes.date);
    const gender = get(indexes.gender);
    const ageStr = get(indexes.age);
    const impressionsRaw = get(indexes.impressions);

    if (ageStr == null || impressionsRaw == null) continue;

    const impressionsNum = Number(String(impressionsRaw).replace(/,/g, '').trim());
    if (!Number.isFinite(impressionsNum)) continue;

    const token = parseAgeToken(ageStr);

    for (const part of expandAge(token, impressionsNum)) {
      if (part.plus65) {
        add(insertionOrder, date, gender, '+65', part.impressions);
      } else if (typeof part.year === 'number') {
        const bin = yearToBin(part.year);
        add(insertionOrder, date, gender, bin, part.impressions);
      }
      // unknowns are ignored
    }
  }

  // Emit JSONL
  const outLines = [];
  for (const [key, impressions] of agg.entries()) {
    const [insertionOrder, date, gender, ageRange] = key.split('\u0001');
    outLines.push(JSON.stringify({ insertionOrder, date, gender, age: ageRange, impressions: Math.round(impressions) }));
  }

  safeWriteLinesAtomicSync(OUT, outLines);
  console.log(`Wrote ${outLines.length} records to ${OUT}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
