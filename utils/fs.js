// utils/fs.js
import fs from 'node:fs';
import path from 'node:path';

export function ensureDirSync(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

export function fileExistsSync(p) {
  try {
    fs.accessSync(p, fs.constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

export function safeWriteLinesAtomicSync(filePath, lines) {
  const dir = path.dirname(filePath);
  ensureDirSync(dir);
  const tmp = `${filePath}.tmp-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  fs.writeFileSync(tmp, lines.join('\n'), 'utf8');
  fs.renameSync(tmp, filePath);
}
