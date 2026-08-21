import DefaultTheme from 'vitepress/theme';
import { h } from 'vue';
import type { Theme } from 'vitepress';
import NavBarActions from './NavBarActions.vue';
import HeroHeadline from './HeroHeadline.vue';
import HomeEngine from './HomeEngine.vue';
import SiteLogo from './SiteLogo.vue';
import './custom.css';

export default {
  extends: DefaultTheme,
  Layout: () => {
    return h(DefaultTheme.Layout, null, {
      'nav-bar-title-before': () => h(SiteLogo),
      'nav-bar-content-after': () => h(NavBarActions),
      'home-hero-info': () => h(HeroHeadline),
      'home-hero-after': () => h(HomeEngine),
      'doc-after': () =>
        h('p', { class: 'pico-doc-copyright' }, '© 2026 PicoMQ. Apache 2.0.'),
    });
  },
} satisfies Theme;
