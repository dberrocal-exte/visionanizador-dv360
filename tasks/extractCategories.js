// tasks/extractCategories.js
// Reads rawData/categories.csv, extracts unique category tiers -> intermediate/categories.tierN.jsonl
// Params (via yargs):
//   --tiers [1|2|3|4] (default 1)
//   --provider [dv|ttd|zed] (default 'dv')
//   --splitval (default '/'; any single char)
// Notes:
//   - If provider === 'dv', remove the FIRST occurrence of splitval before splitting.
//   - Deduplicate unique tier combinations.
//   - Validate file and directories exist/are created.

import fs from 'node:fs';
import readline from 'node:readline';
import path from 'node:path';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import config from '../config.js';
import { ensureDirSync, fileExistsSync, safeWriteLinesAtomicSync } from '../utils/fs.js';
import { applyCommonArgs } from '../utils/argumentos.js';

// ...

const argv = applyCommonArgs(
  yargs(hideBin(process.argv))
    .option('splitval', {
      type: 'string',
      default: '/',
      describe: 'Single character used as category separator'
    })
)
  .check((args) => {
    if (!args.splitval || args.splitval.length !== 1) {
      throw new Error('splitval must be a single character');
    }
    return true;
  })
  .strict()
  .argv;
const RAW_DIR = config.paths?.raw ?? './rawData';
const INTERMEDIATE_DIR = config.paths?.intermediate ?? './intermediate';
const INPUT = path.join(RAW_DIR, 'categories.csv');
const OUT = path.join(INTERMEDIATE_DIR, `categories.tier${argv.tiers}.jsonl`);
const DELIM = config.csv?.delimiter ?? ',';
const ENCODING = config.csv?.encoding ?? 'utf8';

// Resolve category column index
function resolveCategoryIndex(headerLine) {
  // 1) Prefer explicit mapping from config
  const mapped = config.mapping?.categories?.category;
  if (Number.isInteger(mapped)) return mapped;

  // 2) Try auto-detect by header tokens (for when headers exist)
  if (headerLine) {
    const headers = splitCSV(headerLine, DELIM);
    const idx = headers.findIndex(h =>
      String(h).toLowerCase().trim() === 'category'
    );
    if (idx >= 0) return idx;
  }

  // 3) Fallback to 2 (third column), matching your intended schema
  return 2;
}

// Very minimal CSV splitter (native-first). Handles simple CSV; if you later hit
// quoting/escaping edge-cases, we can swap this to `csv-parse`.
function splitCSV(line, delimiter) {
  // Fast path: no quotes
  if (!line.includes('"')) return line.split(delimiter);

  // Simple quote-aware parser (still not exhaustive for all CSV edge-cases)
  const out = [];
  let cur = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') { // escaped quote
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === delimiter && !inQuotes) {
      out.push(cur);
      cur = '';
      continue;
    }

    cur += ch;
  }
  out.push(cur);
  return out;
}

function normalizePart(s) {
  return String(s ?? '').trim();
}

function preprocessForProvider(raw, provider, splitChar) {
  if (provider === 'dv') {
    // remove the FIRST occurrence of the split char before splitting
    const idx = raw.indexOf(splitChar);
    if (idx >= 0) {
      return raw.slice(0, idx) + raw.slice(idx + 1);
    }
  }
  return raw;
}

async function main() {
  if (!fileExistsSync(INPUT)) {
    console.error(`Missing input file: ${INPUT}`);
    process.exit(1);
  }

  ensureDirSync(INTERMEDIATE_DIR);

  const stream = fs.createReadStream(INPUT, ENCODING);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let lineNo = 0;
  let categoryIdx = null;

  const uniq = new Set();

  for await (const rawLine of rl) {
    const line = rawLine ?? '';
  // NEW: stop on DV footer
    if (argv.provider === 'dv') {
        const startsFooter = /^\s*Report Time\b/i.test(line);
        if (startsFooter) {
        console.log('Detected DV summary footer; stopping further processing.');
        break;
        }
    }
    lineNo += 1;
    if (!line) continue;

    const cols = splitCSV(line, DELIM);

    // Detect header on first line if category index unresolved
    if (lineNo === 1 && categoryIdx === null) {
      categoryIdx = resolveCategoryIndex(line);
      // If first line looks like header (i.e., the value at categoryIdx equals 'Category'),
      // skip it for data rows.
      const maybeHeader = splitCSV(line, DELIM)[categoryIdx];
      if (String(maybeHeader).toLowerCase().trim() === 'category') {
        // move to next line; categoryIdx is set
        continue;
      }
    }

    if (categoryIdx === null) {
      // If we still couldn't resolve, assume default column 2
      categoryIdx = 2;
    }

    const rawCategory = cols[categoryIdx];
    if (rawCategory == null) continue;

    const preprocessed = preprocessForProvider(String(rawCategory), argv.provider, argv.splitval);
    const parts = preprocessed
      .split(argv.splitval)
      .map(normalizePart)
      .filter(Boolean);
    if (parts.length < argv.tiers) {
    continue;
    }
    const tiers = parts.slice(0, argv.tiers);
    if (tiers.length === 0) continue;

    // Build key for dedupe
    const key = tiers.join('\u0001');
    if (uniq.has(key)) continue;
    uniq.add(key);
  }

  // Write JSONL
  const lines = [];
  for (const key of uniq) {
    const parts = key.split('\u0001');
    const obj = {};
    parts.forEach((p, i) => {
      obj[`tier${i + 1}`] = p;
    });
    lines.push(JSON.stringify(obj));
  }

  safeWriteLinesAtomicSync(OUT, lines);
  console.log(`Wrote ${lines.length} unique categories to ${OUT}`);
}

// Run
main().catch((err) => {
  console.error(err);
  process.exit(1);
});