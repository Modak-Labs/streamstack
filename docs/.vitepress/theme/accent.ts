const KEY = 'pico-accent-hex-v1';
export const DEFAULT_ACCENT = '#1f2a37';

export const SWATCHES = [
  '#1f2a37',
  '#0f1620',
  '#5e6878',
  '#8096ff',
  '#cc8b00',
  '#ff6b6b',
  '#a78bfa',
  '#22d3ee',
] as const;

function safeGet(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function isHex(value: string): boolean {
  return /^#[0-9a-fA-F]{6}$/.test(value);
}

function parseRgb(hex: string): [number, number, number] {
  const n = parseInt(hex.slice(1), 16);

  return [n >> 16, (n >> 8) & 255, n & 255];
}

function toHex(r: number, g: number, b: number): string {
  return `#${[r, g, b]
    .map((c) => Math.round(c).toString(16).padStart(2, '0'))
    .join('')}`;
}

function mixWhite(hex: string, amount: number): string {
  const [r, g, b] = parseRgb(hex);

  return toHex(r + (255 - r) * amount, g + (255 - g) * amount, b + (255 - b) * amount);
}

export function readAccentHex(): string {
  const stored = safeGet(KEY);

  return stored && isHex(stored) ? stored : DEFAULT_ACCENT;
}

export function applyAccentHex(hex: string) {
  const value = isHex(hex) ? hex : DEFAULT_ACCENT;
  const [r, g, b] = parseRgb(value);
  const root = document.documentElement.style;

  root.setProperty('--vp-c-brand-1', value);
  root.setProperty('--vp-c-brand-2', mixWhite(value, 0.18));
  root.setProperty('--vp-c-brand-3', mixWhite(value, 0.36));
  root.setProperty('--vp-c-brand-soft', `rgb(${r} ${g} ${b} / 0.14)`);

  try {
    localStorage.setItem(KEY, value);
  } catch {
    // ignore
  }

  window.dispatchEvent(new CustomEvent('pico-accent', { detail: value }));
}
