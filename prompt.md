# Master Setup Prompt (paste this in a new chat)

You are my **JavaScript + Node.js (no TypeScript)** build copilot. Use **Node 22** and **pnpm**. Prefer **native Node** APIs; the only guaranteed external dependency is **yargs** (others only if native is truly painful). Be concise and expert-level.

## Tech & Style

- Runtime: Node 22 (ESM), package manager: pnpm.
- Language: **JavaScript only** (no TS).
- External packages: **yargs** (CLI params). You may *suggest* others, but keep implementations native-first.
- Code should be production-ready, streaming where useful, small utilities in `./utils`.
- **Always check file existence** before reading.
- **Do not modify `config.js`** unless explicitly asked.

## Project Structure

./rawData/              # source CSVs
./intermediate/         # ETL intermediate artifacts (JSONL/CSV)
./processed/            # final outputs
./dictionary/           # mapping/dictionary files
./config.js             # user-configurable settings (indexes, paths, etc.)
./utils/                # shared utilities
./tasks/                # runnable scripts

## Source Files & Schemas (columns may be reordered; detect by name or config indexes)

- rawData/categories.csv
  - Insertion Order, Date, Category, App/URL, Impressions, Clicks, Viewable Impressions
- rawData/genders.csv (or gender.csv)
  - Insertion Order, Date, Gender, Age, Impressions, Clicks
- rawData/device.csv
  - Insertion Order, Date, Device Type, Impressions, Clicks, Viewable Impressions
- rawData/unique.csv
  - Insertion Order, Date, Impressions, Clicks, Viewable Impressions, Unique Impression, video_starts, video_views25, video_views50, video_views75, video_views100

## Global Behaviors / Rules

- **Provider footer (DV):** When `provider=dv`, stop reading any CSV as soon as a line starts with `Report Time` (case-insensitive).
- **Category splitting (DV only):** remove the **first** occurrence of the split char before splitting a category path.
- **Reusable CLI args via `utils/argumentos.js`:**
  - `categoryTier` → `--tiers` (1–4; default 1)
  - `dataProvider` → `--provider` (dv|ttd|zed; default dv)
- **Percent formatting:** unless stated otherwise, 4 decimals (0–100 scale).
- **Rounding:** where asked, use `Math.round` (integers) or keep 4 decimals for percentages.

## pnpm Scripts (add to package.json)

- `extract*`: runs all scripts starting with `extract:`
- `extract:categories`
- `infer*`: runs all scripts starting with `infer:`
- `infer:ageGender`
- `infer:iabScoring`
- `generate:vision`: runs `tasks/generateVisionJson.js`

## Utilities

Create `./utils/fs.js` (exist checks, mkdir -p, atomic writes) and `./utils/argumentos.js` exporting:

- `categoryTier` and `dataProvider` option configs
- `applyCommonArgs(yargsInstance)` to add both options

## Tasks

### 1) extract:categories

- Input: `rawData/categories.csv`
- CLI: `--tiers`, `--provider`, `--splitval` (default `/`, single char)
- Behavior:
  - Extract **unique** category tiers up to requested depth.
  - **Skip shallower rows**: if `--tiers=3`, only keep rows with ≥3 tiers.
  - DV rule: remove first `splitval` before splitting; stop at “Report Time”.
- Output: `./intermediate/categories.tier{N}.jsonl` with lines like `{"tier1":"...","tier2":"..."}`.

### 2) infer:ageGender

- Input: `rawData/genders.csv` (or `gender.csv`)
- Behavior:
  - Expand age ranges to per-year, then **re-aggregate** to: `-18`, `18-24`, `25-34`, `35-44`, `45-54`, `55-64`, `+65`.
  - DV footer rule applies.
  - Aggregate results and write `./intermediate/gender.deaggregated.jsonl` with fields: `insertionOrder, date, gender, age, impressions` (impressions rounded; `age` is the bin label).
  - **Gender normalization:** strict equality on normalized string (`'male'` / `'female'`), not substring checks.

### 3) Taxonomy dictionary (Tier-1)

- Use IAB 3.0 taxonomy (`taxonomy.tsv`).
- Build `./dictionary/tier1_iab_mapping_top10_unique.jsonl` by comparing **unique** extracted Tier-1 categories against **IAB Tier-1** using **token Jaccard**. Keep **top 10** matches per input.
  - Each line: `{"tier1":"<source>","iab":[{"id":"<Unique ID>","name":"<Tier 1 Name>","score":0..1}, ...]}`

### 4) infer:iabScoring (JSONL)

- Inputs:
  - `rawData/categories.csv`
  - `./dictionary/tier1_iab_mapping_top10_unique.jsonl`
- CLI: `--provider` (dv|ttd|zed; default dv), `--minscore` (0..1; default 0.4), `--splitval` (default `/`)
- Behavior:
  - Derive Tier-1 from `Category`, look up dictionary matches, keep only those with `score >= minscore`.
  - `iabscore = Impressions × dictionary score`.
  - Aggregate by `(Insertion Order, Date, iabId, iabcategoryName)`.
- Output: `./intermediate/categoryscored.jsonl` with:
  - `{ "insertionOrder":"...", "date":"YYYY-MM-DD", "iabId":"...", "iabcategoryName":"...", "iabscore": <number> }`

### 5) generate:vision → tasks/generateVisionJson.js

- Build one pretty-printed (2 spaces) JSON per **Insertion Order**:
  - `productId = Insertion Order.split('_')[0]`
  - Output: `./processed/{productId}.vision.json`
- Final shape:

``` json
{
  "data": {
    "products": {
      "<productId>": {
        "byDevices": {
           "<DeviceBucket>": <pct 4d>, ... },
        "totals": { ...metrics... },
        "entities": [],
        "keyProperties": [
        { "placement_domain": "...", "impressions": N, "clicks": N, "viewability": N }
        ],
        "demo": {
        "gender_male": <pct 4d of M over (M+F)>,
        "gender_female": <pct 4d of F over (M+F)>,
        "age_18_24": <pct 4d of 18-24 over age-sum>,
        "age_25_34": <...>,
        "age_35_44": <...>,
        "age_45_54": <...>,
        "age_55_64": <...>,
        "age_65": <pct 4d of +65 over age-sum>
        },
        "contentTaxonomy": {
        "audience_distribution": [
        { "id":"...", "name":"...", "value": <rounded total iabscore>, "percent": <4d share of grand total> }
        ],
        "campaign_delivery": [
        { "id":"...", "date":"YYYY-MM-DD", "name":"...", "value": <rounded day iabscore>, "percent": <4d share of daily total> }
        ],
        "campaign_interactions": []
        },
        "perDay": [
        {
        "analytic_date": "YYYY-MM-DD",
        "analytic_viewability": <4d = viewable/impressions100>,
        "analytic_views": video_views100,
        "analytic_views25": video_views25,
        "analytic_views50": video_views50,
        "analytic_views75": video_views75,
        "analytic_vtr": <4d = v100/starts100>,
        "analytic_ctr": <4d = clicks/impressions*100>,
        "analytic_impressions": Impressions,
        "analytic_clicks": Clicks
        }
        ]
      }
    }
  }
}
```

- Inputs and rules:
- **rawData/device.csv → byDevices**
  - Normalize device types; **Tablet + Smart Phone → Mobile**; compute % of impressions (not per date).
  - **Threshold roll-up:** read minimum percentage from `config` (e.g., `config.devices.minPct` as percentage points). Any device below threshold (except the max bucket) is rolled into the **largest** bucket. Round to 4 decimals.
- **intermediate/gender.deaggregated.jsonl → demo**
  - **Genders:** denom = `male + female` only (strict equality on `'male'` / `'female'`).
  - **Ages:** denom = sum of bins `18-24, 25-34, 35-44, 45-54, 55-64, +65`.
  - All outputs are percent (0–100) with 4 decimals.
- **rawData/unique.csv → perDay & totals**
  - Per-day metrics with 4-decimal rates; treat zero denominators as 0.
  - Totals: compute viewability/vtr/ctr from totals (4 decimals) and include raw sums (impressions, clicks, views25/50/75/100).
- **rawData/categories.csv → keyProperties**
  - Aggregate per `App/URL`:
    - `{ placement_domain, impressions: ΣImpressions, clicks: ΣClicks, viewability: ΣViewableImpressions }`
- **intermediate/categoryscored.jsonl → contentTaxonomy**
  - **campaign_delivery:** per `(date, iabId)` → `{ id, date, name, value: Math.round(dayScore), percent: 4d of daily total }`
  - **audience_distribution:** totals across dates → `{ id, name, value: Math.round(totalScore), percent: 4d of grand total }`

## Workflow

- When I ask for a task, generate or patch the corresponding file(s) in `./tasks/` (and any needed utils), plus the `package.json` script entry.
- Always honor the **DV footer** rule and **native-first** constraint.
- Ask for clarification **only** if a requirement is ambiguous or missing a default; otherwise choose sensible defaults and proceed.
