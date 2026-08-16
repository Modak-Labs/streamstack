<template>
  <section class="home-panels ss-wrap" aria-label="Stream Stack">
    <header class="home-panels__intro">
      <h2>Create streams as you go.</h2>
      <p>
        Streams can match the logical granularity of each use case,<br />
        rather than collecting every record of a kind into one topic.
      </p>
    </header>

    <article class="home-panel">
      <div class="home-panel__copy">
        <h3>Unlimited streams</h3>
        <p>
          Stream data and the WAL live on S3-compatible object storage,
          so create as many as you need with no topic tax.
        </p>
      </div>
      <div class="home-panel__media">
        <div class="home-panel__terminal">
          <pre><code><span class="prompt">$</span> docker compose --env-file harness/local/.env \
    -f harness/local/docker-compose.minio.yml \
    -f harness/local/docker-compose.ds.yml \
    up -d --build
<span class="result"><span class="ok">✔</span> Node listening on 127.0.0.1:4437</span></code></pre>
        </div>
      </div>
    </article>

    <article class="home-panel">
      <div class="home-panel__media">
        <div class="home-panel__terminal">
          <pre><code><span class="prompt">$</span> java -jar cli/target/streamstack.jar ds bench \
    --endpoint http://127.0.0.1:4437 \
    -b 1024 -n 256 -w 32 -d 20
<span class="result"><span class="ok">✔</span> Write: 69.45 MiB/s, 71117 records/s</span></code></pre>
        </div>
      </div>
      <div class="home-panel__copy">
        <h3>Blazing fast</h3>
        <p>
          Battle-tested <code>S3Stream</code> storage backend for high
          throughput. Transparent, reproducible benchmarks.
        </p>
      </div>
    </article>

    <article class="home-panel">
      <div class="home-panel__copy">
        <h3>Multi-node</h3>
        <p>
          N nodes, one cluster. <code>JRaft</code> replicated metadata for
          high availability and fault tolerance.
        </p>
      </div>
      <div class="home-panel__media">
        <div class="home-panel__terminal">
          <pre><code><span class="prompt">$</span> docker compose --env-file harness/local/.env \
    -f harness/local/docker-compose.minio.yml \
    -f harness/local/docker-compose.cluster.ds.yml \
    up -d --build
<span class="result"><span class="ok">✔</span> Nodes on 127.0.0.1:4437-4439</span></code></pre>
        </div>
      </div>
    </article>

  </section>
</template>

<style>
.home-panels {
  position: relative;
  z-index: 1;
  display: grid;
  gap: 1.25rem;
  margin-bottom: 5rem;
}

.home-panels__intro {
  max-width: 42rem;
  margin-inline: auto;
  padding: 0.5rem 0 1.25rem;
  text-align: center;
}

.home-panels__intro h2 {
  margin: 0;
  font-size: clamp(1.75rem, 4vw, 2.65rem);
  font-weight: 700;
  line-height: 1.12;
  letter-spacing: -0.045em;
  color: var(--vp-c-text-1);
}

.home-panels__intro p {
  margin: 1.15rem 0 0;
  font-size: 1.05rem;
  line-height: 1.65;
  color: var(--vp-c-text-2);
}

.home-panels__label {
  margin: 1.25rem 0 0;
  font-size: 0.8rem;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--vp-c-text-2);
}

.home-panel {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  overflow: hidden;
  border: 1px solid rgb(255 255 255 / 0.12);
  border-radius: 2px;
  background: rgb(20 20 22 / 0.38);
}

.home-panel__copy,
.home-panel__media {
  min-width: 0;
  padding: 1.5rem;
}

.home-panel__copy h3 {
  margin: 0;
  font-size: clamp(1.5rem, 3vw, 2.1rem);
  font-weight: 700;
  line-height: 1.1;
  letter-spacing: -0.04em;
  color: var(--vp-c-text-1);
}

.home-panel__copy p {
  margin: 0.85rem 0 0;
  font-size: 1rem;
  line-height: 1.6;
  color: var(--vp-c-text-2);
}

.home-panel__copy code {
  padding: 0.12em 0.4em;
  border: 1px solid rgb(255 255 255 / 0.12);
  border-radius: 2px;
  background: rgb(12 12 13 / 0.55);
  font-family: var(--vp-font-family-mono);
  font-size: 0.86em;
  color: var(--vp-c-brand-2);
}

.home-panel__media {
  display: flex;
  align-items: center;
}

.home-panel__terminal {
  position: relative;
  width: 100%;
  overflow: hidden;
  border: 1px solid rgb(255 255 255 / 0.1);
  border-radius: 2px;
  background: rgb(12 12 13 / 0.28);
}

.home-panel__terminal::before {
  position: absolute;
  top: 0.45rem;
  left: 0.65rem;
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: #ff5f57;
  box-shadow:
    11px 0 0 #febc2e,
    22px 0 0 #28c840;
  content: "";
}

.home-panel__terminal::after {
  position: absolute;
  top: 1.2rem;
  right: 0;
  left: 0;
  height: 1px;
  background: rgb(255 255 255 / 0.08);
  content: "";
}

.home-panel__terminal pre {
  margin: 0;
  padding: 1.55rem 0.95rem 0.75rem;
  overflow: auto;
  font-family: var(--vp-font-family-mono);
  font-size: 0.76rem;
  line-height: 1.65;
  color: #c8c8c2;
}

.home-panel__terminal .prompt {
  color: var(--vp-c-brand-2);
}

.home-panel__terminal .ok {
  color: #9ad7b3;
}

.home-panel__terminal .result {
  display: block;
  margin-top: 0.7em;
}

.home-panel__terminal .comment {
  color: #6f6f6a;
}

@media (min-width: 860px) {
  .home-panel {
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  }

  .home-panel__copy,
  .home-panel__media {
    padding: 2rem;
  }
}
</style>
