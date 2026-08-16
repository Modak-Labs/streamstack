import DefaultTheme from 'vitepress/theme';
import { h } from 'vue';
import type { Theme } from 'vitepress';
import { theme as openapiTheme } from 'vitepress-openapi/client';
import ColorSwitch from './ColorSwitch.vue';
import HeroDither from './HeroDither.vue';
import HomeChips from './HomeChips.vue';
import HomeBento from './HomeBento.vue';
import SiteLogo from './SiteLogo.vue';
import { setupProtocolTabs } from './protocolTabs';
import 'vitepress-openapi/dist/style.css';
import './custom.css';

export default {
  extends: DefaultTheme,
  Layout: () => {
    return h(DefaultTheme.Layout, null, {
      'nav-bar-title-before': () => h(SiteLogo),
      'nav-bar-content-after': () => h(ColorSwitch),
      'home-hero-before': () => h(HeroDither),
      'home-hero-after': () => h(HomeChips),
      'home-features-after': () => h(HomeBento),
      'doc-after': () =>
        h('p', { class: 'ss-doc-copyright' }, '© 2026 Modak Labs. Apache 2.0.'),
    });
  },
  enhanceApp({ app, router }) {
    openapiTheme.enhanceApp({ app });

    if (typeof window !== 'undefined') {
      setupProtocolTabs((hook) => {
        const previous = router.onAfterRouteChange;

        router.onAfterRouteChange = async (to) => {
          await previous?.(to);
          hook();
        };
      });
    }
  },
} satisfies Theme;
