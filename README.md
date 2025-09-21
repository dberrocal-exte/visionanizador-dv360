# Vision ETL (Node.js + pnpm, JavaScript-only)

A small, native-first ETL toolchain to process raw campaign CSVs into a **Vision** JSON for each product (Insertion Order).  
Runtime is **Node 22** (ESM) with **pnpm**; external dependency kept to **yargs** for CLI options.

---

## Requirements

- **Node.js 22** (ESM)
- **pnpm** (package manager)
- OS: macOS, Linux, or Windows

> This project is **JavaScript only** (no TypeScript). Prefer native Node APIs over extra libraries.

---

## Project Structure

```
./rawData/              # source CSVs (input)
./intermediate/         # ETL intermediate artifacts (JSONL/CSV)
./processed/            # final outputs
./dictionary/           # mapping/dictionary files (IAB Tier-1 matching)
./config.js             # user-configurable settings (paths, CSV delimiter, device threshold, etc.)
./utils/                # shared utilities (fs helpers, shared yargs options)
./tasks/                # runnable scripts
```

---

## Input Files (expected columns; **order may vary**)

### `rawData/categories.csv`
- **Insertion Order**, **Date**, **Category**, **App/URL**, **Impressions**, **Clicks**, **Viewable Impressions**

### `rawData/genders.csv` (or `rawData/gender.csv`)
- **Insertion Order**, **Date**, **Gender**, **Age**, **Impressions**, **Clicks**

### `rawData/device.csv`
- **Insertion Order**, **Date**, **Device Type**, **Impressions**, **Clicks**, **Viewable Impressions**

### `rawData/unique.csv`
- **Insertion Order**, **Date**, **Impressions**, **Clicks**, **Viewable Impressions**, **Unique Impression**,
  **video_starts**, **video_views25**, **video_views50**, **video_views75**, **video_views100**

> The scripts detect headers by **name** and fall back to default positions. If your vendor exports differ,
> adjust indexes in `config.js` later if needed.

---

## Global Rules & Conventions

- **Provider footer (DV)**: When `--provider dv`, all CSV readers **stop** when a line starts with **`Report Time`** (case-insensitive).
- **Category path fix (DV only)**: Remove the **first** occurrence of the split char (default `/`) before splitting a category path.
- **Reusable CLI options** (in `utils/argumentos.js`):
  - `--tiers` (`1|2|3|4`, default **1**) → how many tiers to extract from category paths.
  - `--provider` (`dv|ttd|zed`, default **dv**) → provider-specific behavior (DV footer & category fix).
- **Percent formatting**: 4 decimals (0–100 scale) when specified.
- **Rounding**: when asked, use integer rounding via `Math.round`.

---

## Install

```bash
pnpm install
```

> Project assumes ESM (`"type": "module"` in `package.json`).

---

## Commands (pnpm scripts)

### 1) `extract:categories`
Extract unique category tiers from `rawData/categories.csv`.

**Usage**
```bash
pnpm run extract:categories -- [--tiers <1|2|3|4>] [--provider <dv|ttd|zed>] [--splitval <char>]
```

**Options**
- `--tiers` (default **1**): number of tiers to extract. **Skips** rows that do **not** have enough depth (e.g., `--tiers 3` requires ≥3 tiers).
- `--provider` (default **dv**): enables DV rules (footer stop + first split removal).
- `--splitval` (default **/**): single character used to split the `Category` field.

**Output**
- `./intermediate/categories.tier{N}.jsonl`  
  e.g., `{"tier1":"Arts & Entertainment","tier2":"Music & Audio"}`

---

### 2) `infer:ageGender`
Normalize age & gender from `genders.csv` / `gender.csv` and bin into fixed ranges.

**Usage**
```bash
pnpm run infer:ageGender -- [--provider <dv|ttd|zed>]
```

**Behavior**
- Expands age ranges to per-year, then aggregates into bins:
  `-18`, `18-24`, `25-34`, `35-44`, `45-54`, `55-64`, `+65`.
- DV footer respected.
- **Impressions are rounded** (integers).
- **Gender normalization** uses strict equality on normalized strings:
  - `'male'` → male
  - `'female'` → female

**Output**
- `./intermediate/gender.deaggregated.jsonl`  
  Fields: `insertionOrder, date, gender, age, impressions`

---

### 3) Dictionary: IAB Tier-1 Matching
Build a lightweight mapping between your Tier-1 categories and IAB Tier-1 (v3.0) using **token Jaccard** similarity.

**Primary artifact**
- `./dictionary/tier1_iab_mapping_top10_unique.jsonl`  
  Each line:
  ```json
  {
    "tier1": "<your tier1>",
    "iab": [
      {"id":"<Unique ID>","name":"<IAB Tier 1>","score": 0..1},
      ...
    ]
  }
  ```

> You can regenerate/update this file separately if needed; the scorer (below) consumes it.

---

### 4) `infer:iabScoring`
Score categories in `categories.csv` against IAB Tier-1 using the dictionary above. Emits **JSONL**.

**Usage**
```bash
pnpm run infer:iabScoring -- [--provider <dv|ttd|zed>] [--minscore <0..1>] [--splitval <char>]
```

**Options**
- `--provider` (default **dv**): DV footer + category path first-split removal.
- `--minscore` (default **0.4**): only dictionary matches with `score >= minscore` are kept.
- `--splitval` (default **/**): split char for category paths.

**Logic**
- Derive Tier-1 from `Category`, look up dictionary matches (`tier1_iab_mapping_top10_unique.jsonl`),
- For each match with `score >= minscore`, compute
  `iabscore = Impressions × score` and aggregate by
  `(Insertion Order, Date, iabId, iabcategoryName)`.

**Output**
- `./intermediate/categoryscored.jsonl`  
  Each line:
  ```json
  {"insertionOrder":"...", "date":"YYYY-MM-DD", "iabId":"...", "iabcategoryName":"...", "iabscore": <number>}
  ```

---

### 5) `generate:vision`
Combine all intermediates into one **Vision JSON** per product (productId).

**Usage**
```bash
pnpm run generate:vision -- [--provider <dv|ttd|zed>]
```

**Inputs**
- `rawData/device.csv` → `byDevices`
  - Device buckets: `Desktop`, `Mobile` (**Tablet + Smart Phone**), `CTV`, others kept as-is.
  - Compute % of impressions **across all dates** (per product).
  - **Threshold roll-up:** devices below the minimum percentage (see config) are summed and moved into the **largest** bucket.
  - 4-decimal percentages.
- `intermediate/gender.deaggregated.jsonl` → `demo`
  - **Genders:** % over *(male + female)* only.
  - **Ages:** % over sum of bins `18-24, 25-34, 35-44, 45-54, 55-64, +65`.
  - 4-decimal percentages.
- `rawData/unique.csv` → `perDay` & `totals`
  - Per-day metrics: `analytic_viewability`, `analytic_vtr`, `analytic_ctr` (4 decimals, safe 0 when denominator is 0), plus raw counts.
  - Totals computed from sums (viewability/vtr/ctr recomputed from totals).
- `rawData/categories.csv` → `keyProperties`
  - Per `App/URL`: `{placement_domain, impressions: Σ, clicks: Σ, viewability: Σ viewable impressions}`
- `intermediate/categoryscored.jsonl` → `contentTaxonomy`
  - **campaign_delivery** (per date): `{ id, date, name, value: Math.round(dayScore), percent: share of that date }`
  - **audience_distribution** (totals): `{ id, name, value: Math.round(totalScore), percent: share of grand total }`

**Output**
- `./processed/{productId}.vision.json` (pretty-printed with **2 spaces**)

**productId**
- Defined as: `Insertion Order.split('_')[0]`.

---

## Wildcard runners

- `pnpm run extract*` → runs all `extract:*` scripts.
- `pnpm run infer*` → runs all `infer:*` scripts in order (see `tasks/runInferAll.js`).

> These helpers iterate the `scripts` in `package.json` and execute matching ones in lexical order.

---

## Configuration (`config.js`)

Example minimal shape (adjust paths/options as needed):
```js
export default {
  paths: {
    raw: './rawData',
    intermediate: './intermediate',
    processed: './processed',
    dictionary: './dictionary'
  },
  csv: {
    encoding: 'utf8',
    delimiter: ','
  },
  devices: {
    // Minimum percentage points to display a device bucket.
    // Buckets below this are rolled into the largest bucket.
    minPct: 1.0
  }
};
```

> Do **not** change `config.js` unless you know what you’re doing — scripts already do header autodetection.

---

## Outputs Overview

- `./intermediate/categories.tier{N}.jsonl`
- `./intermediate/gender.deaggregated.jsonl`
- `./dictionary/tier1_iab_mapping_top10_unique.jsonl`
- `./intermediate/categoryscored.jsonl`
- `./processed/{productId}.vision.json`

---

## Troubleshooting

- **DV datasets stop early?** Check for a trailing line starting with `Report Time` — that footer is intentionally treated as the end of data.
- **Header order changes per vendor?** Scripts autodetect by name; if something is off, confirm the header text matches the expected labels or extend `config.js` indexes.
- **Weird gender splits?** Only exact normalized `'male'` / `'female'` are counted; other values are ignored by design.
- **Missing dictionary file?** Ensure `./dictionary/tier1_iab_mapping_top10_unique.jsonl` exists before running `infer:iabScoring` and `generate:vision`.

---

## License

Internal use. Adapt as needed.
