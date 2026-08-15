import { defineConfig } from 'vitepress';
import { useSidebar } from 'vitepress-openapi';
import { adminSpec, dsSpec, s2Spec } from './specs';

function sideItem(label: string, link: string, path: string) {
  return {
    text: `<span class="ss-side-ico" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none">${path}</svg></span>${label}`,
    link,
  };
}

function operationItems(spec: object, prefix: string) {
  return useSidebar({ spec, linkPrefix: prefix })
    .generateSidebarGroups({ linkPrefix: prefix })
    .flatMap((group) => group.items ?? []);
}

const docsSidebar = [
  sideItem(
    'Introduction',
    '/introduction',
    '<path d="M6 4.5h9.5A2.5 2.5 0 0 1 18 7v13H8.5A2.5 2.5 0 0 0 6 17.5V4.5Z" stroke="currentColor" stroke-width="1.7"/><path d="M6 4.5A2.5 2.5 0 0 0 3.5 7v10.5A2.5 2.5 0 0 1 6 17.5" stroke="currentColor" stroke-width="1.7"/>',
  ),
  sideItem(
    'Quickstart',
    '/quickstart',
    '<path d="M8 6.5v11l10-5.5L8 6.5Z" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round"/>',
  ),
  sideItem(
    'Benchmark',
    '/benchmark',
    '<path d="M3 16l5.2-5.2 3.6 3.6L21 5" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/>',
  ),
  {
    ...sideItem(
      'Protocol',
      '/protocol',
      '<path d="M5 8h14M5 12h14M5 16h9" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/><path d="M4 6.5h16v11H4z" stroke="currentColor" stroke-width="1.7"/>',
    ),
    collapsed: false,
    items: [
      {
        ...sideItem(
          'Durable Streams',
          '/protocol/durable-streams',
          '<path d="M4 12c2-4 4-4 6 0s4 4 6 0 4-4 4 0" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/>',
        ),
        collapsed: true,
        items: operationItems(dsSpec, '/protocol/durable-streams/'),
      },
      {
        ...sideItem(
          'S2',
          '/protocol/s2',
          '<path d="M7 8h7.5a3.5 3.5 0 1 1 0 7H9.5a3.5 3.5 0 1 0 0 7H17" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/>',
        ),
        collapsed: true,
        items: operationItems(s2Spec, '/protocol/s2/'),
      },
    ],
  },
  {
    ...sideItem(
      'Deployment',
      '/deployment',
      '<path d="M12 3v10M8 7l4-4 4 4" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/><path d="M5 14.5v3A2.5 2.5 0 0 0 7.5 20h9a2.5 2.5 0 0 0 2.5-2.5v-3" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/>',
    ),
    collapsed: false,
    items: [
      sideItem(
        'AWS',
        '/deployment/aws',
        '<path d="M12 3.5 20 8v8l-8 4.5L4 16V8l8-4.5Z" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round"/><path d="M12 12v8.5M12 12 4 8M12 12l8-4" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/>',
      ),
      sideItem(
        'Fly.io',
        '/deployment/fly',
        '<path d="M5 19L20 12L5 5l4.2 7L5 19Z" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round"/><path d="M9.2 12H20" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/>',
      ),
    ],
  },
  {
    ...sideItem(
      'Operations',
      '/operations',
      '<path d="M4 8h16M4 16h16M9 5v6M15 13v6" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/>',
    ),
    collapsed: false,
    items: [
      sideItem(
        'Durability',
        '/operations/durability',
        '<path d="M12 3.5 19 7v5.5c0 4.2-2.8 7.4-7 8.5-4.2-1.1-7-4.3-7-8.5V7l7-3.5Z" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round"/>',
      ),
      {
        ...sideItem(
          'Admin',
          '/operations/api',
          '<path d="M8 7h8M8 12h8M8 17h5" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/><path d="M5 5h14v14H5z" stroke="currentColor" stroke-width="1.7"/>',
        ),
        collapsed: true,
        items: operationItems(adminSpec, '/operations/api/'),
      },
    ],
  },
];

export default defineConfig({
  title: 'Stream Stack',
  description: 'A durable stream engine on object storage.',
  base: process.env.BASE_PATH ?? '/',
  cleanUrls: true,
  appearance: 'force-dark',
  srcDir: 'pages',
  vite: {
    publicDir: 'assets',
    server: {
      fs: {
        allow: ['..'],
      },
    },
  },
  markdown: {
    theme: 'github-dark',
  },
  head: [
    ['link', { rel: 'icon', href: '/images/favicon.ico', sizes: 'any' }],
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/images/logo.svg' }],
    [
      'script',
      {},
      `(function(){function m(h,a){var n=parseInt(h.slice(1),16),r=n>>16,g=n>>8&255,b=n&255;return'#'+[r,g,b].map(function(c){c=Math.round(c+(255-c)*a);return c.toString(16).padStart(2,'0')}).join('')}try{var h=localStorage.getItem('ss-accent-hex-v2');if(h&&/^#[0-9a-fA-F]{6}$/.test(h)){var d=document.documentElement,s=d.style;var n=parseInt(h.slice(1),16);d.dataset.ssAccent=h.toLowerCase()==='#ffffff'?'white':'color';s.setProperty('--vp-c-brand-1',h);s.setProperty('--vp-c-brand-2',m(h,.18));s.setProperty('--vp-c-brand-3',m(h,.36));s.setProperty('--vp-c-brand-soft','rgb('+(n>>16)+' '+(n>>8&255)+' '+(n&255)+' / 0.14)')}}catch(e){}})();`,
    ],
  ],
  themeConfig: {
    siteTitle: false,
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Docs', link: '/introduction' },
    ],
    sidebar: docsSidebar,
    search: {
      provider: 'local',
    },
    socialLinks: [
      { icon: 'github', link: 'https://github.com/Modak-Labs/streamstack' },
    ],
    outline: {
      level: [1, 3],
      label: 'On this page',
    },
    footer: {
      copyright: '© 2026 Modak Labs. Apache 2.0.',
    },
  },
  transformPageData(pageData) {
    if (pageData.params?.pageTitle) {
      pageData.title = pageData.params.pageTitle;
    }
  },
});
