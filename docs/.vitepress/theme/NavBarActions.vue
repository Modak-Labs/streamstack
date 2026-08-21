<script setup lang="ts">
import { useData } from 'vitepress';
import { VPSocialLinks } from 'vitepress/theme-without-fonts';
import ColorSwitch from './ColorSwitch.vue';

const { theme } = useData();
</script>

<template>
  <div class="pico-nav-actions">
    <VPSocialLinks
      v-if="theme.socialLinks"
      class="pico-nav-actions__social"
      :links="theme.socialLinks"
    />
    <ColorSwitch />
  </div>
</template>

<style scoped>
.pico-nav-actions {
  display: flex;
  align-items: center;
  height: var(--vp-nav-height);
}

.pico-nav-actions__social {
  display: none;
}

@media (min-width: 1280px) {
  .pico-nav-actions__social {
    display: flex;
    align-items: center;
  }
}
</style>
