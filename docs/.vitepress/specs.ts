import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { parseSpec } from 'vitepress-openapi';

const streamstack = resolve(fileURLToPath(new URL('../..', import.meta.url)));

function loadSpec(rel: string) {
  return parseSpec(readFileSync(resolve(streamstack, rel), 'utf8'));
}

export const dsSpec = loadSpec('frontend/ds/openapi.yml');
export const s2Spec = loadSpec('frontend/s2/openapi.yml');
export const adminSpec = loadSpec('server/openapi.yml');
