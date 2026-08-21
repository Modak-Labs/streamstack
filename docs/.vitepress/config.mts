import { defineConfig } from 'vitepress';

function sideItem(label: string, link: string, path: string) {
  return {
    text: `<span class="pico-side-ico" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none">${path}</svg></span>${label}`,
    link,
  };
}

const docsSidebar = [
  sideItem(
    'Docs',
    '/docs',
    '<path d="M6 4.5h9.5A2.5 2.5 0 0 1 18 7v13H8.5A2.5 2.5 0 0 0 6 17.5V4.5Z" stroke="currentColor" stroke-width="1.7"/><path d="M6 4.5A2.5 2.5 0 0 0 3.5 7v10.5A2.5 2.5 0 0 1 6 17.5" stroke="currentColor" stroke-width="1.7"/>',
  ),
];

export default defineConfig({
  title: 'PicoMQ',
  description:
    'PicoMQ is durable, real-time streams over HTTP, built on S3-compatible object storage.',
  base: process.env.BASE_PATH ?? '/',
  cleanUrls: true,
  appearance: 'force-light',
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
    theme: 'github-light',
  },
  head: [
    ['link', { rel: 'icon', href: '/images/favicon.ico', sizes: 'any' }],
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/images/logo.svg' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.googleapis.com' }],
    [
      'link',
      { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' },
    ],
    [
      'link',
      {
        rel: 'stylesheet',
        href: 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&family=Marcellus&family=Playfair+Display:wght@400;500;600&display=swap',
      },
    ],
    [
      'script',
      {},
      `(function(){function m(h,a){var n=parseInt(h.slice(1),16),r=n>>16,g=n>>8&255,b=n&255;return'#'+[r,g,b].map(function(c){c=Math.round(c+(255-c)*a);return c.toString(16).padStart(2,'0')}).join('')}try{var h=localStorage.getItem('pico-accent-hex-v1');if(h&&/^#[0-9a-fA-F]{6}$/.test(h)){var s=document.documentElement.style;var n=parseInt(h.slice(1),16);s.setProperty('--vp-c-brand-1',h);s.setProperty('--vp-c-brand-2',m(h,.18));s.setProperty('--vp-c-brand-3',m(h,.36));s.setProperty('--vp-c-brand-soft','rgb('+(n>>16)+' '+(n>>8&255)+' '+(n&255)+' / 0.14)')}}catch(e){}})();`,
    ],
  ],
  themeConfig: {
    siteTitle: false,
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Docs', link: '/docs' },
    ],
    sidebar: docsSidebar,
    search: {
      provider: 'local',
    },
    socialLinks: [
      { icon: 'github', link: 'https://github.com/picomq/picomq' },
    ],
    outline: {
      level: [1, 3],
      label: 'On this page',
    },
    footer: {
      copyright: '© 2026 PicoMQ. Apache 2.0.',
    },
  },
});
