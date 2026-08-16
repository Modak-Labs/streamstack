<template>
  <section class="home-tiles ss-wrap" aria-label="Highlights">
    <article class="home-tile">
      <span class="home-tile__icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none">
          <path
            d="M8 8c-2.2 0-4 1.8-4 4s1.8 4 4 4c1.3 0 2.4-.6 3.2-1.6L12 12l.8-2.4C13.6 8.6 14.7 8 16 8c2.2 0 4 1.8 4 4s-1.8 4-4 4c-1.3 0-2.4-.6-3.2-1.6"
            stroke="currentColor"
            stroke-width="1.7"
            stroke-linecap="round"
            stroke-linejoin="round"
          />
        </svg>
      </span>
      <h3>Infinite streams</h3>
      <p>Create as many as you need. Partitions are now streams, no topic tax.</p>
    </article>

    <article class="home-tile">
      <span class="home-tile__icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none">
          <path
            d="M4 7.5c0-1.4 3.6-2.5 8-2.5s8 1.1 8 2.5v9c0 1.4-3.6 2.5-8 2.5s-8-1.1-8-2.5v-9Z"
            stroke="currentColor"
            stroke-width="1.7"
          />
          <path
            d="M4 7.5c0 1.4 3.6 2.5 8 2.5s8-1.1 8-2.5"
            stroke="currentColor"
            stroke-width="1.7"
          />
        </svg>
      </span>
      <h3>Cheap storage</h3>
      <p>Cost effective with S3-compatible object storage for WAL and data.</p>
    </article>

    <article class="home-tile">
      <span class="home-tile__icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none">
          <path
            d="M3 16l5.2-5.2 3.6 3.6L21 5"
            stroke="currentColor"
            stroke-width="1.7"
            stroke-linecap="round"
            stroke-linejoin="round"
          />
        </svg>
      </span>
      <h3>High throughput</h3>
      <p>
        Battle-tested <code>S3Stream</code> storage backend. Transparent
        benchmark suite.
      </p>
    </article>

    <article class="home-tile">
      <span class="home-tile__icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none">
          <circle cx="7" cy="12" r="2.2" stroke="currentColor" stroke-width="1.7" />
          <circle cx="17" cy="7" r="2.2" stroke="currentColor" stroke-width="1.7" />
          <circle cx="17" cy="17" r="2.2" stroke="currentColor" stroke-width="1.7" />
          <path
            d="M9.1 11.1 14.9 8.1M9.1 12.9l5.8 3"
            stroke="currentColor"
            stroke-width="1.7"
            stroke-linecap="round"
          />
        </svg>
      </span>
      <h3>High availability</h3>
      <p>N nodes, one cluster. <code>JRaft</code> replicated metadata.</p>
    </article>

    <article class="home-tile">
      <span class="home-tile__icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none">
          <path
            d="M8.5 7 4 12l4.5 5M15.5 7 20 12l-4.5 5"
            stroke="currentColor"
            stroke-width="1.7"
            stroke-linecap="round"
            stroke-linejoin="round"
          />
        </svg>
      </span>
      <h3>Open source</h3>
      <p>Apache 2.0 licensed. No hidden capabilities.</p>
    </article>

    <article class="home-tile">
      <span class="home-tile__icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none">
          <rect x="4" y="5" width="16" height="3.2" rx="1" stroke="currentColor" stroke-width="1.7" />
          <rect x="4" y="10.4" width="16" height="3.2" rx="1" stroke="currentColor" stroke-width="1.7" />
          <rect x="4" y="15.8" width="16" height="3.2" rx="1" stroke="currentColor" stroke-width="1.7" />
        </svg>
      </span>
      <h3>Many protocols</h3>
      <p>
        <code>durable-streams</code> compatible today. Opinionated native API
        coming soon.
      </p>
    </article>
  </section>
</template>

<style>
.home-tiles {
  position: relative;
  z-index: 1;
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 0.75rem;
  margin-bottom: 2.5rem;
}

.home-tile {
  display: flex;
  flex-direction: column;
  gap: 0.7rem;
  min-width: 0;
  padding: 1.15rem 1.2rem 1.25rem;
  border: 1px solid rgb(255 255 255 / 0.12);
  border-radius: 2px;
  background: rgb(20 20 22 / 0.38);
}

.home-tile__icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 2.15rem;
  height: 2.15rem;
  border: 1px solid rgb(255 255 255 / 0.12);
  border-radius: 2px;
  background: rgb(12 12 13 / 0.55);
  color: var(--vp-c-brand-2);
}

.home-tile__icon svg {
  width: 1.1rem;
  height: 1.1rem;
}

.home-tile h3 {
  margin: 0;
  font-size: 1rem;
  font-weight: 650;
  line-height: 1.2;
  letter-spacing: -0.03em;
  color: var(--vp-c-text-1);
}

.home-tile p {
  margin: 0;
  font-size: 0.86rem;
  line-height: 1.5;
  color: var(--vp-c-text-2);
}

.home-tile code {
  padding: 0.08em 0.32em;
  border: 1px solid rgb(255 255 255 / 0.12);
  border-radius: 2px;
  background: rgb(12 12 13 / 0.55);
  font-family: var(--vp-font-family-mono);
  font-size: 0.86em;
  color: var(--vp-c-brand-2);
}

@media (min-width: 640px) {
  .home-tiles {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (min-width: 960px) {
  .home-tiles {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}
</style>
