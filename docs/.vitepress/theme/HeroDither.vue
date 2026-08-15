<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue';
import {
  DitheringShapes,
  DitheringTypes,
  ShaderFitOptions,
  ShaderMount,
  ditheringFragmentShader,
  getShaderColorFromString,
} from '@paper-design/shaders';

import { readAccentHex } from './accent';

const shaderEl = ref<HTMLElement | null>(null);
let shader: ShaderMount | undefined;

function frontColor(hex?: string) {
  return getShaderColorFromString(`${hex ?? readAccentHex()}66`);
}

function syncFront(event?: Event) {
  const hex =
    event instanceof CustomEvent ? (event.detail as string | undefined) : undefined;

  shader?.setUniforms({ u_colorFront: frontColor(hex) });
}

onMounted(() => {
  if (!shaderEl.value) {
    return;
  }

  const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  shader = new ShaderMount(
    shaderEl.value,
    ditheringFragmentShader,
    {
      u_colorBack: getShaderColorFromString('#00000000'),
      u_colorFront: frontColor(),
      u_shape: DitheringShapes.warp,
      u_type: DitheringTypes['4x4'],
      u_pxSize: 2,
      u_fit: ShaderFitOptions.none,
      u_scale: 0.6,
      u_rotation: 0,
      u_offsetX: 0,
      u_offsetY: 0,
      u_originX: 0.5,
      u_originY: 0.5,
      u_worldWidth: 0,
      u_worldHeight: 0,
    },
    undefined,
    reducedMotion ? 0 : 0.4,
  );

  window.addEventListener('ss-accent', syncFront);
});

onBeforeUnmount(() => {
  window.removeEventListener('ss-accent', syncFront);
  shader?.dispose();
  shader = undefined;
});
</script>

<template>
  <div class="hero-dither" aria-hidden="true">
    <div ref="shaderEl" class="hero-dither__shader" />
  </div>
</template>

<style scoped>
.hero-dither {
  --ss-dither-mask:
    linear-gradient(
      to bottom,
      #fff 0%,
      #fff 18%,
      rgb(255 255 255 / 0.45) 42%,
      rgb(255 255 255 / 0.18) 72%,
      rgb(255 255 255 / 0.08) 100%
    ),
    linear-gradient(to right, transparent 0%, #fff 18%, #fff 82%, transparent 100%);

  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: 0;
  pointer-events: none;
  opacity: 0.42;
  -webkit-mask-image: var(--ss-dither-mask);
  -webkit-mask-composite: source-in;
  mask-image: var(--ss-dither-mask);
  mask-composite: intersect;
}

.hero-dither__shader {
  width: 100%;
  height: 100%;
}
</style>
