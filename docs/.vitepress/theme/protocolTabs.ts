const KEY = 'ss-protocol-tab';
const TABS = ['Stream Stack', 'Durable Streams'];

function labelText(label: Element): string {
  return (label.textContent ?? '').trim();
}

function applySaved(): void {
  const saved = localStorage.getItem(KEY);

  if (!saved || !TABS.includes(saved)) {
    return;
  }

  document.querySelectorAll('.vp-code-group .tabs label').forEach((label) => {
    if (labelText(label) !== saved) {
      return;
    }

    const id = label.getAttribute('for');
    const input = id ? (document.getElementById(id) as HTMLInputElement | null) : null;

    if (input && !input.checked) {
      input.click();
    }
  });
}

export function setupProtocolTabs(onAfterRouteChange: (hook: () => void) => void): void {
  window.addEventListener('click', (e) => {
    const target = e.target as HTMLElement | null;

    if (!target || !target.matches('.vp-code-group .tabs label')) {
      return;
    }

    const text = labelText(target);

    if (!TABS.includes(text)) {
      return;
    }

    localStorage.setItem(KEY, text);
    requestAnimationFrame(applySaved);
  });

  onAfterRouteChange(() => {
    requestAnimationFrame(applySaved);
  });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', applySaved);
  } else {
    applySaved();
  }
}
