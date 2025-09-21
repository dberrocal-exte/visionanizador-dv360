// tasks/inferIabScoring.js
// Build ./intermediate/categoryscored.jsonl using ./dictionary/tier1_iab_mapping_top10_unique.jsonl
// Params:
//   --provider [dv|ttd|zed] (default 'dv')   // from utils:dataProvider via applyCommonArgs
//   --minscore <float 0..1> (default 0.4)
//   --splitval <char> (default '/')
//
// Output JSONL records (aggregated by IO, Date, iabId, iabName):
//   { insertionOrder, date, iabId, iabcategoryName, iabscore }

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
    .option('minscore', {
      type: 'number',
      default: 0.4,
      describe: 'Minimum dictionary score to include (0..1)'
    })
    .option('splitval', {
      type: 'string',
      default: '/',
      describe: 'Single character used as category separator'
    })
)
  .check((args) => {
    if (!(args.minscore >= 0 && args.minscore <= 1)) {
      throw new Error('minscore must be between 0 and 1');
    }
    if (!args.splitval || args.splitval.length !== 1) {
      throw new Error('splitval must be a single character');
    }
    return true;
  })
  .strict()
  .argv;

const RAW_DIR = config.paths?.raw ?? './rawData';
const DICT_DIR = config.paths?.dictionary ?? './dictionary';
const INTERMEDIATE_DIR = config.paths?.intermediate ?? './intermediate';
const ENCODING = config.csv?.encoding ?? 'utf8';
const DELIM = config.csv?.delimiter ?? ',';

const INPUT = path.join(RAW_DIR, 'categories.csv');
const DICT_FILE = path.join(DICT_DIR, 'tier1_iab_mapping_top10_unique.jsonl');
const OUT = path.join(INTERMEDIATE_DIR, 'categoryscored.jsonl');

// --- helpers ---
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

function isFooterLine(provider, line) {
  if (provider !== 'dv') return false;
  return /^\s*report time\b/i.test(line);
}

function normalizePart(s) {
  return String(s ?? '').trim();
}

function preprocessForProvider(raw, provider, splitChar) {
  if (provider === 'dv') {
    const idx = raw.indexOf(splitChar);
    if (idx >= 0) {
      return raw.slice(0, idx) + raw.slice(idx + 1);
    }
  }
  return raw;
}

function resolveIndexes(headerLine) {
  // Defaults per categories.csv schema
  let idx = {
    insertionOrder: 0, // "Insertion Order"
    date: 1,           // "Date"
    category: 2,       // "Category"
    impressions: 4     // "Impressions"
  };

  if (!headerLine) return idx;
  const headers = splitCSV(headerLine, DELIM).map(h => String(h).trim().toLowerCase());

  const map = {
    'insertion order': 'insertionOrder',
    'date': 'date',
    'category': 'category',
    'impressions': 'impressions'
  };

  for (let i = 0; i < headers.length; i++) {
    const key = map[headers[i]];
    if (key) idx[key] = i;
  }
  return idx;
}

// Load dictionary: { lower(tier1) -> [{id, name, score}, ...] }
function loadDictionary(dictPath) {
  const dict = new Map();
  const content = fs.readFileSync(dictPath, ENCODING);
  for (const line of content.split(/\r?\n/)) {
    if (!line.trim()) continue;
    try {
      const obj = JSON.parse(line);
      const key = String(obj.tier1 || '').trim().toLowerCase();
      const arr = Array.isArray(obj.iab) ? obj.iab : [];
      if (key) dict.set(key, arr);
    } catch {
      // ignore malformed lines
    }
  }
  return dict;
}

// --- main ---
async function main() {
  if (!fileExistsSync(INPUT)) {
    console.error(`Missing input file: ${INPUT}`);
    process.exit(1);
  }
  if (!fileExistsSync(DICT_FILE)) {
    console.error(`Missing dictionary file: ${DICT_FILE}`);
    process.exit(1);
  }

  ensureDirSync(INTERMEDIATE_DIR);

  const dict = loadDictionary(DICT_FILE);

  const stream = fs.createReadStream(INPUT, ENCODING);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let lineNo = 0;
  let indexes = null;

  // Aggregate category score by IO, Date, iabId, iabName
  const agg = new Map(); // key = IO\u0001Date\u0001iabId\u0001iabName -> sum score

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
      const maybeCategory = splitCSV(line, DELIM)[indexes.category];
      if (String(maybeCategory).trim().toLowerCase() === 'category') continue; // skip header
    }

    const get = (i) => cols[i];

    const insertionOrder = get(indexes.insertionOrder);
    const date = get(indexes.date);
    const catRaw = get(indexes.category);
    const impressionsRaw = get(indexes.impressions);

    if (catRaw == null || impressionsRaw == null) continue;

    const impressions = Number(String(impressionsRaw).replace(/,/g, '').trim());
    if (!Number.isFinite(impressions)) continue;

    // Get Tier 1 token
    const pre = preprocessForProvider(String(catRaw), argv.provider, argv.splitval);
    const parts = pre.split(argv.splitval).map(normalizePart).filter(Boolean);
    if (parts.length === 0) continue;

    const tier1 = parts[0];
    const candidates = dict.get(tier1.toLowerCase());
    if (!candidates || candidates.length === 0) continue;

    for (const c of candidates) {
      const score = Number(c.score);
      if (!(score >= argv.minscore)) continue;

      const iabId = c.id;
      const iabName = c.name;
      const categoryScore = impressions * score;

      const key = [insertionOrder, date, iabId, iabName].join('\u0001');
      agg.set(key, (agg.get(key) ?? 0) + categoryScore);
    }
  }

  // Write aggregated JSONL
  const out = fs.createWriteStream(OUT, { encoding: 'utf8' });
  for (const [key, totalScore] of agg.entries()) {
    const [insertionOrder, date, iabId, iabName] = key.split('\u0001');
    const rec = {
      insertionOrder,
      date,
      iabId,
      iabcategoryName: iabName,
      iabscore: totalScore
    };
    out.write(JSON.stringify(rec) + '\n');
  }
  out.end();

  console.log(`Wrote ${OUT} (${agg.size} aggregated records)`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
