<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue';
import { DEFAULT_ACCENT, SWATCHES, applyAccentHex, readAccentHex } from './accent';

const open = ref(false);
const current = ref(DEFAULT_ACCENT);
const root = ref<HTMLElement | null>(null);

onMounted(() => {
  current.value = readAccentHex();
  applyAccentHex(current.value);
  window.addEventListener('pointerdown', onPointerDown);
  window.addEventListener('keydown', onKeyDown);
});

onBeforeUnmount(() => {
  window.removeEventListener('pointerdown', onPointerDown);
  window.removeEventListener('keydown', onKeyDown);
});

function onPointerDown(event: PointerEvent) {
  if (!open.value || !root.value) {
    return;
  }

  if (!root.value.contains(event.target as Node)) {
    open.value = false;
  }
}

function onKeyDown(event: KeyboardEvent) {
  if (event.key === 'Escape') {
    open.value = false;
  }
}

function pick(hex: string) {
  current.value = hex;
  applyAccentHex(hex);
}

function onCustom(event: Event) {
  const value = (event.target as HTMLInputElement).value;

  pick(value);
}
</script>

<template>
  <div ref="root" class="ss-picker">
    <button
      class="ss-picker__toggle"
      type="button"
      aria-label="Accent color"
      :aria-expanded="open"
      aria-haspopup="true"
      @click="open = !open"
    >
      <svg viewBox="0 0 24 24" width="20" height="20" aria-hidden="true">
        <path fill="currentColor" d="M12 12L12 0A12 12 0 0 1 22.39 6Z" />
        <path fill="currentColor" opacity="0.78" d="M12 12L22.39 6A12 12 0 0 1 22.39 18Z" />
        <path fill="currentColor" opacity="0.56" d="M12 12L22.39 18A12 12 0 0 1 12 24Z" />
        <path fill="currentColor" opacity="0.38" d="M12 12L12 24A12 12 0 0 1 1.61 18Z" />
        <path fill="currentColor" opacity="0.22" d="M12 12L1.61 18A12 12 0 0 1 1.61 6Z" />
        <path fill="currentColor" opacity="0.1" d="M12 12L1.61 6A12 12 0 0 1 12 0Z" />
      </svg>
    </button>

    <div v-if="open" class="ss-picker__panel" role="dialog" aria-label="Accent color">
      <div class="ss-picker__swatches">
        <button
          v-for="hex in SWATCHES"
          :key="hex"
          class="ss-picker__swatch"
          type="button"
          :aria-label="hex"
          :aria-pressed="current.toLowerCase() === hex"
          :style="{ background: hex }"
          @click="pick(hex)"
        />
      </div>
      <label class="ss-picker__custom">
        Custom
        <span class="ss-picker__swatch" :style="{ background: current }">
          <input type="color" :value="current" @input="onCustom" />
        </span>
      </label>
    </div>
  </div>
</template>

<style scoped>
.ss-picker {
  position: relative;
  display: flex;
  align-items: center;
  height: var(--vp-nav-height);
  margin-left: 0.35rem;
}

.ss-picker__toggle {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  padding: 0;
  border: none;
  border-radius: 2px;
  outline: none;
  background: transparent;
  color: var(--vp-c-text-2);
  cursor: pointer;
}

.ss-picker__toggle svg {
  display: block;
  width: 20px;
  height: 20px;
}

.ss-picker__toggle:hover {
  color: var(--vp-c-text-1);
}

.ss-picker__panel {
  position: absolute;
  top: calc(100% - 10px);
  right: 0;
  z-index: 40;
  width: 148px;
  padding: 0.65rem;
  border: 1px solid var(--vp-c-divider);
  border-radius: 2px;
  background: var(--vp-c-bg-elv);
  box-shadow: 0 12px 32px rgb(0 0 0 / 0.35);
}

.ss-picker__swatches {
  display: flex;
  flex-wrap: wrap;
  gap: 0.45rem;
}

.ss-picker__swatch {
  position: relative;
  width: 18px;
  height: 18px;
  padding: 0;
  overflow: hidden;
  border: none;
  border-radius: 50%;
  outline: none;
  cursor: pointer;
}

.ss-picker__swatch input {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
  padding: 0;
  border: none;
  opacity: 0;
  cursor: pointer;
}

.ss-picker__custom {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.5rem;
  margin-top: 0.6rem;
  padding-top: 0.6rem;
  border-top: 1px solid var(--vp-c-divider);
  font-size: 0.72rem;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--vp-c-text-2);
}
</style>
