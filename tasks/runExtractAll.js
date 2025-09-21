// tasks/runExtractAll.js
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';

const pkg = JSON.parse(fs.readFileSync('./package.json', 'utf8'));
const all = pkg.scripts || {};
const extractNames = Object.keys(all)
  .filter(k => k.startsWith('extract:') && k !== 'extract*')
  .sort();

if (extractNames.length === 0) {
  console.error('No extract:* scripts found.');
  process.exit(1);
}

for (const name of extractNames) {
  console.log(`\n>> Running ${name} ...`);
  const res = spawnSync(process.platform === 'win32' ? 'pnpm.cmd' : 'pnpm', ['run', name], {
    stdio: 'inherit'
  });
  if (res.status !== 0) {
    console.error(`Script ${name} failed with code ${res.status}. Stopping.`);
    process.exit(res.status || 1);
  }
}

console.log('\nAll extract:* scripts completed successfully.');
