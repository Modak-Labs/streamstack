<script setup lang="ts">
import { computed, ref } from 'vue';
import { withBase } from 'vitepress';

const NUMERALS = ['I', 'II', 'III', 'IV', 'V'] as const;

const features = [
  {
    name: 'Unlimited streams',
    tag: 'no topic tax',
    figure: 'Fig. 1',
    body: 'Create a stream per use case instead of packing every record of a kind into one topic. Each stream is independently addressable, bottomless, and can scale from idle to high throughput.',
    refs: ['/docs'],
  },
  {
    name: 'Zero-disk architecture',
    tag: 'object storage native',
    figure: 'Fig. 2',
    body: 'The WAL and stream data live on S3-compatible object storage. Inherit object-store durability and economics without cross-AZ traffic.',
    refs: ['/docs'],
  },
  {
    name: 'Decoupled layers',
    tag: 'storage, metadata, protocol',
    figure: 'Fig. 3',
    body: 'Storage, metadata, and protocol are separate layers. The data and metadata planes can evolve independently. A new protocol is a facade on the same engine without changing how bytes are stored.',
    refs: ['/docs'],
  },
  {
    name: 'High throughput',
    tag: 'S3Stream data plane',
    figure: 'Fig. 4',
    body: 'The write path is the battle-tested S3Stream storage engine. Throughput up to 100 MiB/s per stream. Experiment with the transparent benchmark suite.',
    refs: ['/docs'],
  },
  {
    name: 'Easy deployment',
    tag: 'single node, or a cluster',
    figure: 'Fig. 5',
    body: 'One process per node: storage engine, WAL, SQL metadata log, and HTTP frontend. Run locally with SQLite and a file bucket, or deploy to AWS, GCS, Fly.io and more as a single node or a multi-node cluster.',
    refs: ['/docs'],
  },
] as const;

const streams = [
  {
    name: 'session 1',
    path: '/user/ada/sessions/7f3a91',
    consume: 'replay seq=0',
    y: 60,
    // sparse: mostly idle
    dots: [150, 226, 302],
    cursor: 150,
    cursor2: null,
    live: false,
    trim: false,
    headSeq: 'seq 3',
  },
  {
    name: 'session 2',
    path: '/user/ada/sessions/b21c04',
    consume: 'live=sse',
    y: 118,
    // hot: a burst arriving at the head
    dots: [238, 252, 266, 280, 294, 308, 322, 336, 350],
    cursor: 350,
    cursor2: null,
    live: true,
    trim: false,
    headSeq: 'seq 8412',
  },
  {
    name: 'session 3',
    path: '/user/kai/sessions/9e04d2',
    consume: 'cursor live=long-poll',
    y: 176,
    // steady: small clusters, trimmed prefix, two readers
    dots: [164, 236, 250, 264, 322, 336],
    cursor: 264,
    cursor2: 336,
    live: true,
    trim: true,
    headSeq: 'seq 517',
  },
] as const;

const objectCards = [
  { x: 36, w: 88, label: 'stream set object' },
  { x: 140, w: 88, label: 'stream set object' },
  { x: 268, w: 104, label: 'stream object' },
] as const;

const active = ref(0);
const current = computed(() => features[active.value]);

function select(index: number) {
  active.value = index;
}
</script>

<template>
  <section class="engine pico-wrap" aria-label="The architecture">
    <p class="engine__eyebrow">the architecture</p>

    <div class="engine__plate">
      <div class="engine__main">
        <div class="engine__copy">
          <p class="engine__index" aria-hidden="true">
            {{ NUMERALS[active] }}
          </p>
          <h3>{{ current.name }}</h3>
          <p class="engine__body">{{ current.body }}</p>
          <div class="engine__refs">
            <a
              v-for="href in current.refs"
              :key="href"
              class="engine__ref"
              :href="withBase(href)"
            >
              <span>ref</span>
              <span>{{ href }}</span>
              <span aria-hidden="true">→</span>
            </a>
          </div>
        </div>

        <div class="engine__figure" aria-hidden="true">
          <span class="engine__fig-label">{{ current.figure }}</span>
          <span class="engine__fig-title">{{ current.tag }}</span>

          <svg
            v-if="active === 0"
            class="engine__diagram"
            viewBox="0 0 400 240"
            fill="none"
          >
            <text class="engine__diagram-caption" x="16" y="18">
              Sessions
            </text>
            <text class="engine__diagram-caption" x="384" y="18" text-anchor="end">
              one stream per session
            </text>

            <g
              v-for="stream in streams"
              :key="stream.name"
              :transform="`translate(0 ${stream.y})`"
            >
              <rect
                class="engine__diagram-source"
                x="16"
                y="-14"
                width="94"
                height="28"
              />
              <text
                class="engine__diagram-label"
                x="63"
                y="0"
                text-anchor="middle"
                dominant-baseline="middle"
              >
                {{ stream.name }}
              </text>
              <text class="engine__diagram-path" x="136" y="-14">
                {{ stream.path }}
              </text>
              <text class="engine__diagram-path" x="372" y="-6" text-anchor="end">
                {{ stream.headSeq }}
              </text>

              <template v-if="stream.trim">
                <line
                  class="engine__diagram-flow"
                  x1="110"
                  y1="0"
                  x2="134"
                  y2="0"
                  stroke-opacity="0.4"
                />
                <line
                  class="engine__diagram-tick"
                  x1="138"
                  y1="-5"
                  x2="138"
                  y2="5"
                />
              </template>
              <line
                class="engine__diagram-rail"
                :x1="stream.trim ? 138 : 110"
                y1="0"
                x2="372"
                y2="0"
              />
              <path
                d="M367 -4 372 0 367 4"
                stroke="currentColor"
                stroke-width="1"
              />

              <g
                v-for="dot in stream.dots"
                :key="dot"
                :transform="`translate(${dot} 0)`"
              >
                <rect
                  class="engine__diagram-source"
                  x="-4"
                  y="-3"
                  width="8"
                  height="6"
                />
                <path
                  d="M-4 -3 L0 1 L4 -3"
                  stroke="currentColor"
                  stroke-width="1"
                />
              </g>

              <circle
                v-if="stream.live"
                class="engine__diagram-live"
                :cx="358"
                cy="0"
                r="3"
              />

              <path
                class="engine__diagram-cursor"
                :transform="`translate(${stream.cursor} 0)`"
                d="M-4 12 L0 6 L4 12 Z"
              />
              <path
                v-if="stream.cursor2"
                class="engine__diagram-cursor"
                :transform="`translate(${stream.cursor2} 0)`"
                d="M-4 12 L0 6 L4 12 Z"
              />
              <text
                class="engine__diagram-path"
                :x="stream.cursor"
                y="22"
                :text-anchor="stream.cursor > 300 ? 'end' : 'middle'"
              >
                {{ stream.consume }}
              </text>
            </g>

            <text
              class="engine__diagram-path"
              x="63"
              y="228"
              text-anchor="middle"
            >
              ⋮
            </text>
            <text class="engine__diagram-path" x="136" y="228">
              pico create → session n
            </text>
          </svg>

          <svg
            v-else-if="active === 1"
            class="engine__diagram"
            viewBox="0 0 460 252"
            fill="none"
          >
            <text class="engine__diagram-caption" x="20" y="24">append()</text>
            <line class="engine__diagram-flow" x1="58" y1="30" x2="58" y2="90" />
            <path
              d="M54 85 58 90 62 85"
              stroke="currentColor"
              stroke-width="1"
            />
            <g transform="translate(58 58)">
              <rect
                class="engine__diagram-source"
                x="-4"
                y="-3"
                width="8"
                height="6"
              />
              <path
                d="M-4 -3 L0 1 L4 -3"
                stroke="currentColor"
                stroke-width="1"
              />
            </g>

            <g class="engine__diagram-ghost" transform="translate(336 14)">
              <ellipse cx="12" cy="4" rx="12" ry="4" />
              <path d="M0 4 V18 A12 4 0 0 0 24 18 V4" />
              <line x1="-4" y1="26" x2="28" y2="-4" />
            </g>
            <text class="engine__diagram-path" x="444" y="28" text-anchor="end">
              no local disk
            </text>

            <rect
              class="engine__diagram-source"
              x="16"
              y="48"
              width="428"
              height="188"
              stroke-dasharray="3 2"
            />
            <text class="engine__diagram-caption" x="28" y="66">
              object storage
            </text>
            <text class="engine__diagram-path" x="432" y="66" text-anchor="end">
              s3://bucket
            </text>

            <text class="engine__diagram-path" x="36" y="86">
              wal/segment-00000042
            </text>
            <rect
              class="engine__diagram-source"
              x="36"
              y="92"
              width="192"
              height="20"
            />
            <rect
              class="engine__diagram-cell-filled"
              x="36"
              y="92"
              width="128"
              height="20"
            />
            <rect
              class="engine__diagram-cell-live"
              x="164"
              y="92"
              width="16"
              height="20"
            />
            <line
              v-for="i in 11"
              :key="`wal-${i}`"
              class="engine__diagram-tick"
              :x1="36 + i * 16"
              y1="92"
              :x2="36 + i * 16"
              y2="112"
            />
            <path class="engine__diagram-cursor" d="M176 84 L180 90 L184 84 Z" />
            <text class="engine__diagram-path" x="240" y="102">
              durable ack after 1 put
            </text>
            <text class="engine__diagram-path" x="240" y="112">
              no fsync, no volume
            </text>

            <line class="engine__diagram-flow" x1="80" y1="112" x2="80" y2="150" />
            <path
              d="M76 145 80 150 84 145"
              stroke="currentColor"
              stroke-width="1"
            />
            <text class="engine__diagram-path" x="92" y="128">async upload</text>
            <text class="engine__diagram-path" x="92" y="138">then trim wal</text>

            <g
              v-for="card in objectCards"
              :key="card.x"
              :transform="`translate(${card.x} 150)`"
            >
              <rect
                class="engine__diagram-source"
                x="0"
                y="0"
                :width="card.w"
                height="64"
              />
              <rect
                v-for="ry in [5, 17, 29]"
                :key="ry"
                class="engine__diagram-block"
                x="6"
                :y="ry"
                :width="card.w - 12"
                height="9"
              />
              <rect
                class="engine__diagram-index"
                x="6"
                y="41"
                :width="card.w - 12"
                height="6"
              />
              <rect
                class="engine__diagram-footer"
                x="6"
                y="50"
                :width="card.w - 12"
                height="5"
              />
              <text
                class="engine__diagram-path"
                :x="card.w / 2"
                y="76"
                text-anchor="middle"
              >
                {{ card.label }}
              </text>
            </g>

            <line class="engine__diagram-flow" x1="124" y1="182" x2="140" y2="182" />
            <path
              d="M136 178 140 182 136 186"
              stroke="currentColor"
              stroke-width="1"
            />
            <line class="engine__diagram-flow" x1="228" y1="182" x2="268" y2="182" />
            <path
              d="M264 178 268 182 264 186"
              stroke="currentColor"
              stroke-width="1"
            />
            <text
              class="engine__diagram-path"
              x="248"
              y="174"
              text-anchor="middle"
            >
              compact
            </text>

            <line class="engine__diagram-tick" x1="372" y1="160" x2="378" y2="160" />
            <text class="engine__diagram-path" x="381" y="163">data</text>
            <line class="engine__diagram-tick" x1="372" y1="194" x2="378" y2="194" />
            <text class="engine__diagram-path" x="381" y="197">index</text>
            <line class="engine__diagram-tick" x1="372" y1="202" x2="378" y2="202" />
            <text class="engine__diagram-path" x="381" y="211">footer</text>
          </svg>

          <svg
            v-else-if="active === 2"
            class="engine__diagram"
            viewBox="0 0 400 240"
            fill="none"
          >
            <text class="engine__diagram-caption" x="16" y="16">protocol</text>

            <rect
              class="engine__diagram-source"
              x="16"
              y="22"
              width="124"
              height="30"
            />
            <text
              class="engine__diagram-label"
              x="78"
              y="33"
              text-anchor="middle"
              dominant-baseline="middle"
            >
              pico
            </text>
            <text
              class="engine__diagram-path"
              x="78"
              y="46"
              text-anchor="middle"
            >
              Pico-* headers
            </text>

            <rect
              class="engine__diagram-source"
              x="156"
              y="22"
              width="124"
              height="30"
            />
            <text
              class="engine__diagram-label"
              x="218"
              y="33"
              text-anchor="middle"
              dominant-baseline="middle"
            >
              durable-streams
            </text>
            <text
              class="engine__diagram-path"
              x="218"
              y="46"
              text-anchor="middle"
            >
              open protocol
            </text>

            <rect
              class="engine__diagram-source"
              x="296"
              y="22"
              width="88"
              height="30"
              stroke-dasharray="3 2"
            />
            <text
              class="engine__diagram-label"
              x="340"
              y="33"
              text-anchor="middle"
              dominant-baseline="middle"
            >
              (next)
            </text>
            <text
              class="engine__diagram-path"
              x="340"
              y="46"
              text-anchor="middle"
            >
              same engine
            </text>

            <line class="engine__diagram-rail" x1="78" y1="52" x2="78" y2="82" />
            <path d="M74 77 78 82 82 77" stroke="currentColor" stroke-width="1" />
            <line class="engine__diagram-rail" x1="218" y1="52" x2="218" y2="82" />
            <path
              d="M214 77 218 82 222 77"
              stroke="currentColor"
              stroke-width="1"
            />
            <line
              class="engine__diagram-flow"
              x1="340"
              y1="52"
              x2="340"
              y2="82"
            />

            <line
              class="engine__diagram-flow"
              x1="16"
              y1="66"
              x2="384"
              y2="66"
              stroke-opacity="0.35"
            />

            <rect
              class="engine__diagram-source"
              x="16"
              y="82"
              width="368"
              height="34"
            />
            <text
              class="engine__diagram-label"
              x="200"
              y="94"
              text-anchor="middle"
              dominant-baseline="middle"
            >
              server
            </text>
            <text
              class="engine__diagram-path"
              x="200"
              y="108"
              text-anchor="middle"
            >
              append, read, ownership, placement
            </text>

            <line
              class="engine__diagram-flow"
              x1="16"
              y1="132"
              x2="384"
              y2="132"
              stroke-opacity="0.35"
            />

            <line class="engine__diagram-rail" x1="104" y1="116" x2="104" y2="148" />
            <path
              d="M100 143 104 148 108 143"
              stroke="currentColor"
              stroke-width="1"
            />
            <text class="engine__diagram-path" x="112" y="128">data plane</text>

            <line class="engine__diagram-rail" x1="296" y1="116" x2="296" y2="148" />
            <path
              d="M292 143 296 148 300 143"
              stroke="currentColor"
              stroke-width="1"
            />
            <text class="engine__diagram-path" x="304" y="128">
              control plane
            </text>

            <rect
              class="engine__diagram-source"
              x="16"
              y="148"
              width="176"
              height="78"
            />
            <text class="engine__diagram-caption" x="28" y="164">s3stream</text>
            <rect
              class="engine__diagram-source"
              x="28"
              y="176"
              width="70"
              height="12"
            />
            <rect
              class="engine__diagram-cell-filled"
              x="28"
              y="176"
              width="40"
              height="12"
            />
            <rect
              class="engine__diagram-cell-live"
              x="68"
              y="176"
              width="10"
              height="12"
            />
            <line
              v-for="i in 6"
              :key="`l3-wal-${i}`"
              class="engine__diagram-tick"
              :x1="28 + i * 10"
              y1="176"
              :x2="28 + i * 10"
              y2="188"
            />
            <line class="engine__diagram-flow" x1="98" y1="182" x2="126" y2="182" />
            <path
              d="M122 178 126 182 122 186"
              stroke="currentColor"
              stroke-width="1"
            />
            <g transform="translate(126 166)">
              <rect class="engine__diagram-source" x="0" y="0" width="52" height="34" />
              <rect class="engine__diagram-block" x="5" y="5" width="42" height="6" />
              <rect class="engine__diagram-block" x="5" y="14" width="42" height="6" />
              <rect class="engine__diagram-index" x="5" y="23" width="42" height="4" />
              <rect class="engine__diagram-footer" x="5" y="29" width="42" height="3" />
            </g>
            <text class="engine__diagram-path" x="28" y="216">
              wal, objects, compaction
            </text>

            <rect
              class="engine__diagram-source"
              x="208"
              y="148"
              width="176"
              height="78"
            />
            <text class="engine__diagram-caption" x="220" y="164">metadata</text>
            <g v-for="(ry, idx) in [172, 182, 192]" :key="`row-${ry}`">
              <rect
                class="engine__diagram-index"
                x="220"
                :y="ry"
                width="14"
                height="8"
              />
              <rect
                :class="idx === 2 ? 'engine__diagram-cell-live' : 'engine__diagram-block'"
                x="236"
                :y="ry"
                width="82"
                height="8"
              />
            </g>
            <line class="engine__diagram-flow" x1="318" y1="186" x2="334" y2="186" />
            <path
              d="M330 182 334 186 330 190"
              stroke="currentColor"
              stroke-width="1"
            />
            <rect
              class="engine__diagram-source"
              x="334"
              y="176"
              width="40"
              height="20"
            />
            <text
              class="engine__diagram-label"
              x="354"
              y="186"
              text-anchor="middle"
              dominant-baseline="middle"
            >
              views
            </text>
            <text class="engine__diagram-path" x="220" y="216">
              sql log, views, leases
            </text>
          </svg>

          <svg
            v-else-if="active === 3"
            class="engine__diagram"
            viewBox="0 0 400 240"
            fill="none"
          >
            <rect
              class="engine__diagram-source"
              x="16"
              y="24"
              width="76"
              height="26"
            />
            <text
              class="engine__diagram-label"
              x="54"
              y="37"
              text-anchor="middle"
              dominant-baseline="middle"
            >
              producer
            </text>

            <line class="engine__diagram-rail" x1="92" y1="37" x2="192" y2="37" />
            <path d="M187 33 192 37 187 41" stroke="currentColor" stroke-width="1" />
            <g
              v-for="gx in [116, 140, 164]"
              :key="`flight-${gx}`"
              :transform="`translate(${gx} 37)`"
            >
              <rect
                class="engine__diagram-source"
                x="-4"
                y="-3"
                width="8"
                height="6"
              />
              <path
                d="M-4 -3 L0 1 L4 -3"
                stroke="currentColor"
                stroke-width="1"
              />
            </g>
            <text
              class="engine__diagram-path"
              x="142"
              y="28"
              text-anchor="middle"
            >
              1 append, pipelined
            </text>

            <text class="engine__diagram-caption" x="192" y="20">wal</text>
            <rect
              class="engine__diagram-source"
              x="192"
              y="27"
              width="112"
              height="20"
            />
            <rect
              class="engine__diagram-cell-filled"
              x="192"
              y="27"
              width="70"
              height="20"
            />
            <rect
              class="engine__diagram-cell-live"
              x="262"
              y="27"
              width="14"
              height="20"
            />
            <line
              v-for="i in 7"
              :key="`f4-wal-${i}`"
              class="engine__diagram-tick"
              :x1="192 + i * 14"
              y1="27"
              :x2="192 + i * 14"
              y2="47"
            />

            <line class="engine__diagram-flow" x1="192" y1="56" x2="96" y2="56" />
            <path d="M101 52 96 56 101 60" stroke="currentColor" stroke-width="1" />
            <text
              class="engine__diagram-path"
              x="144"
              y="68"
              text-anchor="middle"
            >
              2 durable ack
            </text>

            <line class="engine__diagram-rail" x1="272" y1="47" x2="272" y2="96" />
            <path
              d="M268 91 272 96 276 91"
              stroke="currentColor"
              stroke-width="1"
            />
            <text class="engine__diagram-path" x="278" y="74">hot</text>

            <text class="engine__diagram-caption" x="196" y="90">
              record cache
            </text>
            <rect
              class="engine__diagram-source"
              x="196"
              y="96"
              width="84"
              height="16"
            />
            <rect
              class="engine__diagram-cell-filled"
              x="196"
              y="96"
              width="56"
              height="16"
            />
            <rect
              class="engine__diagram-cell-live"
              x="252"
              y="96"
              width="14"
              height="16"
            />
            <line
              v-for="i in 5"
              :key="`f4-cache-${i}`"
              class="engine__diagram-tick"
              :x1="196 + i * 14"
              y1="96"
              :x2="196 + i * 14"
              y2="112"
            />

            <rect
              class="engine__diagram-source"
              x="16"
              y="91"
              width="76"
              height="26"
            />
            <text
              class="engine__diagram-label"
              x="54"
              y="104"
              text-anchor="middle"
              dominant-baseline="middle"
            >
              consumer
            </text>
            <line class="engine__diagram-rail" x1="196" y1="104" x2="96" y2="104" />
            <path d="M101 100 96 104 101 108" stroke="currentColor" stroke-width="1" />
            <text
              class="engine__diagram-path"
              x="146"
              y="98"
              text-anchor="middle"
            >
              3 tail from memory
            </text>

            <rect
              class="engine__diagram-source"
              x="16"
              y="190"
              width="368"
              height="34"
            />
            <text
              class="engine__diagram-label"
              x="200"
              y="202"
              text-anchor="middle"
              dominant-baseline="middle"
            >
              object storage
            </text>
            <text
              class="engine__diagram-path"
              x="200"
              y="216"
              text-anchor="middle"
            >
              wal segments, data objects
            </text>

            <line class="engine__diagram-flow" x1="296" y1="47" x2="296" y2="190" />
            <path
              d="M292 185 296 190 300 185"
              stroke="currentColor"
              stroke-width="1"
            />
            <text class="engine__diagram-path" x="302" y="122">4 async upload</text>

            <line class="engine__diagram-flow" x1="232" y1="190" x2="232" y2="112" />
            <path
              d="M228 117 232 112 236 117"
              stroke="currentColor"
              stroke-width="1"
            />
            <text class="engine__diagram-path" x="238" y="152">5 prefetch</text>

            <line class="engine__diagram-flow" x1="56" y1="190" x2="56" y2="117" />
            <path
              d="M52 122 56 117 60 122"
              stroke="currentColor"
              stroke-width="1"
            />
            <text class="engine__diagram-path" x="62" y="155">6 catch-up</text>
          </svg>

          <svg
            v-else-if="active === 4"
            class="engine__diagram"
            viewBox="0 0 400 240"
            fill="none"
          >
            <text class="engine__diagram-caption" x="16" y="18">laptop</text>
            <text class="engine__diagram-caption" x="136" y="18">cluster</text>
            <text class="engine__diagram-path" x="384" y="18" text-anchor="end">
              clients hit any node
            </text>

            <line
              class="engine__diagram-tick"
              x1="124"
              y1="12"
              x2="124"
              y2="224"
              stroke-dasharray="3 2"
            />

            <g
              v-for="node in [
                { x: 32, name: 'node' },
                { x: 148, name: 'n1' },
                { x: 226, name: 'n2' },
                { x: 304, name: 'n3' },
              ]"
              :key="node.name"
              :transform="`translate(${node.x} 40)`"
            >
              <line class="engine__diagram-rail" x1="32" y1="-14" x2="32" y2="-2" />
              <path
                d="M28 -6 32 -1 36 -6"
                stroke="currentColor"
                stroke-width="1"
              />
              <rect class="engine__diagram-source" x="0" y="0" width="64" height="50" />
              <text
                class="engine__diagram-label"
                x="32"
                y="10"
                text-anchor="middle"
                dominant-baseline="middle"
              >
                {{ node.name }}
              </text>
              <rect class="engine__diagram-block" x="8" y="16" width="48" height="9" />
              <text
                class="engine__diagram-path"
                x="32"
                y="23"
                text-anchor="middle"
              >
                http
              </text>
              <rect class="engine__diagram-block" x="8" y="27" width="48" height="9" />
              <text
                class="engine__diagram-path"
                x="32"
                y="34"
                text-anchor="middle"
              >
                engine
              </text>
              <rect class="engine__diagram-block" x="8" y="38" width="48" height="9" />
              <text
                class="engine__diagram-path"
                x="32"
                y="45"
                text-anchor="middle"
              >
                wal
              </text>
              <line class="engine__diagram-rail" x1="32" y1="50" x2="32" y2="130" />
              <path
                d="M28 125 32 130 36 125"
                stroke="currentColor"
                stroke-width="1"
              />
            </g>

            <rect
              class="engine__diagram-source"
              x="16"
              y="170"
              width="96"
              height="22"
            />
            <text
              class="engine__diagram-path"
              x="64"
              y="184"
              text-anchor="middle"
            >
              sqlite ./meta.db
            </text>
            <rect
              class="engine__diagram-source"
              x="16"
              y="198"
              width="96"
              height="22"
            />
            <text
              class="engine__diagram-path"
              x="64"
              y="212"
              text-anchor="middle"
            >
              file://./objects
            </text>

            <rect
              class="engine__diagram-source"
              x="136"
              y="170"
              width="248"
              height="22"
            />
            <text class="engine__diagram-caption" x="148" y="184">
              sql metadata log
            </text>
            <text
              class="engine__diagram-path"
              x="372"
              y="184"
              text-anchor="end"
            >
              postgres
            </text>
            <rect
              class="engine__diagram-source"
              x="136"
              y="198"
              width="248"
              height="22"
            />
            <text class="engine__diagram-caption" x="148" y="212">
              object storage
            </text>
            <text
              class="engine__diagram-path"
              x="372"
              y="212"
              text-anchor="end"
            >
              s3://bucket
            </text>

            <text
              class="engine__diagram-path"
              x="200"
              y="236"
              text-anchor="middle"
            >
              same binary: pico serve
            </text>
          </svg>
        </div>
      </div>

      <div class="engine__tabs" role="tablist" aria-label="Engine features">
        <button
          v-for="(feature, index) in features"
          :key="feature.name"
          class="engine__tab"
          type="button"
          role="tab"
          :class="{ active: index === active }"
          :aria-selected="index === active"
          @click="select(index)"
        >
          <span class="engine__tab-name">
            <span class="engine__tab-numeral">{{ NUMERALS[index] }}</span>
            {{ feature.name }}
          </span>
          <span class="engine__tab-tag">{{ feature.tag }}</span>
        </button>
      </div>
    </div>
  </section>
</template>

<style scoped>
.engine {
  position: relative;
  z-index: 1;
  margin: 0 auto 5rem;
}

.engine__eyebrow {
  margin: 0 0 1.25rem;
  font-family: var(--pico-font-serif);
  font-size: 1.5rem;
  font-weight: 400;
  letter-spacing: -0.005em;
  color: var(--pico-ink-1);
}

.engine__plate {
  border: 1px solid var(--pico-ink-6);
  background: var(--pico-surface-0);
}

.engine__main {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
}

.engine__copy {
  position: relative;
  display: flex;
  flex-direction: column;
  gap: 0.9rem;
  min-width: 0;
  padding: 2rem 1.5rem 1.75rem;
}

.engine__index {
  margin: 0;
  font-family: var(--pico-font-serif);
  font-size: 1rem;
  letter-spacing: 0.08em;
  color: var(--pico-ink-6);
}

.engine__copy h3 {
  margin: 0;
  font-family: var(--pico-font-serif);
  font-size: 1.6rem;
  font-weight: 400;
  line-height: 1.15;
  letter-spacing: 0;
  color: var(--pico-ink-1);
}

.engine__body {
  margin: 0;
  max-width: 36rem;
  font-size: 1rem;
  line-height: 1.62;
  color: var(--pico-ink-3);
}

.engine__refs {
  display: flex;
  flex-direction: column;
  gap: 0.3rem;
  margin-top: 0.35rem;
}

.engine__ref {
  display: inline-flex;
  align-items: center;
  gap: 0.45rem;
  font-family: var(--vp-font-family-mono);
  font-size: 0.72rem;
  color: var(--pico-ink-4);
  text-decoration: none;
}

.engine__ref:hover {
  color: var(--pico-ink-1);
}

.engine__figure {
  position: relative;
  min-height: 16rem;
  border-top: 1px solid var(--pico-ink-6);
  background:
    linear-gradient(rgb(var(--pico-ink-rgb) / 0.04) 1px, transparent 1px),
    linear-gradient(90deg, rgb(var(--pico-ink-rgb) / 0.04) 1px, transparent 1px),
    var(--pico-surface-2);
  background-size: 36px 36px, 36px 36px, auto;
}

.engine__fig-label,
.engine__fig-title {
  position: absolute;
  top: 1.15rem;
  z-index: 1;
  font-family: var(--vp-font-family-mono);
  font-size: 0.68rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--pico-ink-4);
}

.engine__fig-label {
  left: 1.15rem;
}

.engine__fig-title {
  right: 1.15rem;
}

.engine__diagram {
  position: absolute;
  inset: 2.6rem 0.75rem 0.85rem;
  width: calc(100% - 1.5rem);
  height: calc(100% - 3.45rem);
  color: var(--pico-ink-2);
}

.engine__diagram-label {
  font-family: var(--vp-font-family-mono);
  font-size: 9px;
  letter-spacing: 0.02em;
  fill: currentColor;
}

.engine__diagram-caption,
.engine__diagram-path {
  font-family: var(--vp-font-family-mono);
  fill: var(--pico-ink-4);
}

.engine__diagram-caption {
  font-size: 8px;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}

.engine__diagram-path {
  font-size: 8px;
  letter-spacing: 0.03em;
}

.engine__diagram-source {
  fill: var(--pico-surface-0);
  stroke: currentColor;
  stroke-width: 1;
}

.engine__diagram-flow {
  fill: none;
  stroke: currentColor;
  stroke-dasharray: 3 2;
  stroke-width: 1;
}

.engine__diagram-rail {
  stroke: rgb(var(--pico-ink-rgb) / 0.45);
  stroke-width: 1;
}

.engine__diagram-live {
  fill: var(--vp-c-brand-1);
}

.engine__diagram-cursor {
  fill: var(--pico-ink-4);
}

.engine__diagram-tick {
  stroke: rgb(var(--pico-ink-rgb) / 0.4);
  stroke-width: 1;
}

.engine__diagram-cell-filled {
  fill: rgb(var(--pico-ink-rgb) / 0.14);
}

.engine__diagram-cell-live {
  fill: var(--vp-c-brand-1);
  fill-opacity: 0.45;
}

.engine__diagram-block {
  fill: rgb(var(--pico-ink-rgb) / 0.08);
  stroke: rgb(var(--pico-ink-rgb) / 0.3);
  stroke-width: 0.5;
}

.engine__diagram-index {
  fill: rgb(var(--pico-ink-rgb) / 0.2);
}

.engine__diagram-footer {
  fill: rgb(var(--pico-ink-rgb) / 0.4);
}

.engine__diagram-ghost {
  fill: none;
  stroke: var(--pico-ink-4);
  stroke-width: 1;
}

.engine__tabs {
  display: grid;
  grid-template-columns: 1fr;
  gap: 1px;
  border-top: 1px solid var(--pico-ink-6);
  background: var(--pico-ink-6);
}

.engine__tab {
  display: grid;
  gap: 0.35rem;
  min-width: 0;
  padding: 0.95rem 1rem;
  border: 0;
  border-radius: 0;
  background: var(--pico-surface-0);
  color: var(--pico-ink-3);
  text-align: left;
  cursor: pointer;
}

.engine__tab:hover {
  background: var(--pico-surface-1);
  color: var(--pico-ink-1);
}

.engine__tab.active {
  background: var(--pico-ink-1);
  color: var(--pico-surface-0);
}

.engine__tab-name {
  min-width: 0;
  font-size: 0.86rem;
  font-weight: 600;
  line-height: 1.25;
}

.engine__tab-numeral {
  margin-right: 0.45rem;
  font-family: var(--pico-font-serif);
  font-weight: 400;
  letter-spacing: 0.06em;
}

.engine__tab.active .engine__tab-numeral {
  color: var(--pico-ink-6);
}

.engine__tab-tag {
  font-family: var(--vp-font-family-mono);
  font-size: 0.66rem;
  letter-spacing: 0.06em;
  text-transform: lowercase;
  color: var(--pico-ink-4);
}

.engine__tab.active .engine__tab-tag {
  color: var(--pico-ink-6);
}

@media (min-width: 860px) {
  .engine__main {
    grid-template-columns: minmax(20rem, 0.9fr) minmax(0, 1fr);
    min-height: 22rem;
  }

  .engine__copy {
    padding: 2.25rem 2rem;
  }

  .engine__figure {
    min-height: 22rem;
    border-top: 0;
    border-left: 1px solid var(--pico-ink-6);
  }

  .engine__tabs {
    grid-template-columns: repeat(5, minmax(0, 1fr));
  }
}
</style>
