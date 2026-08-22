import { useEffect, useState } from 'preact/hooks'
import {
  fetchCluster,
  fetchNodes,
  fetchReady,
  type ClusterInfo,
  type NodeInfo,
  type Readiness,
} from './api'

const POLL_INTERVAL_MS = 2000

function Pill({ state, label }: { state: 'ok' | 'warn' | 'err'; label: string }) {
  return (
    <span class={`ss-pill ${state}`}>
      <span class="dot" />
      {label}
    </span>
  )
}

function Stat({ label, value, small }: { label: string; value: unknown; small?: boolean }) {
  return (
    <div class="ss-card">
      <div class="label">{label}</div>
      <div class={small ? 'value small' : 'value'}>{String(value ?? '—')}</div>
    </div>
  )
}

function leaseLabel(cluster: ClusterInfo | null): string {
  if (!cluster || cluster.leaseHolder === null) {
    return '—'
  }
  return cluster.leaseHolder ? 'holder' : 'standby'
}

export function App() {
  const [cluster, setCluster] = useState<ClusterInfo | null>(null)
  const [nodes, setNodes] = useState<NodeInfo[]>([])
  const [ready, setReady] = useState<Readiness | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null)

  useEffect(() => {
    let alive = true

    async function poll() {
      try {
        const [c, n, r] = await Promise.all([fetchCluster(), fetchNodes(), fetchReady()])

        if (!alive) {
          return
        }

        setCluster(c)
        setNodes(n.nodes)
        setReady(r)
        setError(null)
        setUpdatedAt(new Date())
      } catch (e) {
        if (alive) {
          setError(e instanceof Error ? e.message : String(e))
        }
      }
    }

    poll()
    const timer = setInterval(poll, POLL_INTERVAL_MS)

    return () => {
      alive = false
      clearInterval(timer)
    }
  }, [])

  const readyState = ready?.ready ? 'ok' : ready ? 'warn' : 'err'
  const readyLabel = ready?.ready ? 'ready' : ready ? 'not ready' : 'unknown'
  const transfers = cluster?.pendingTransfers ?? []

  return (
    <div class="ss-app">
      <header class="ss-topbar">
        <span class="name">PicoMQ</span>
        <span class="tag">admin</span>
        <span class="spacer" />
        {error ? <Pill state="err" label="unreachable" /> : <Pill state={readyState} label={readyLabel} />}
      </header>

      <div class="ss-grid">
        <Stat label="Cluster" value={cluster?.clusterId} small />
        <Stat label="Node" value={cluster?.nodeId} />
        <Stat label="Applied index" value={cluster?.appliedIndex} />
        <Stat label="Streams" value={cluster?.streamCount} />
        <Stat label="Objects" value={cluster?.objectCount} />
        <Stat label="Maintenance lease" value={leaseLabel(cluster)} small />
      </div>

      <section class="ss-section">
        <h2>This node</h2>
        <table>
          <tbody>
            <tr>
              <th>Advertised address</th>
              <td class="mono">{cluster?.advertisedAddress ?? '—'}</td>
            </tr>
            <tr>
              <th>Node epoch</th>
              <td class="mono">{cluster?.nodeEpoch ?? '—'}</td>
            </tr>
            <tr>
              <th>Registered</th>
              <td class="mono">{cluster ? String(cluster.registered) : '—'}</td>
            </tr>
            <tr>
              <th>Destroyed object backlog</th>
              <td class="mono">{cluster?.destroyedObjectBacklog ?? '—'}</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="ss-section">
        <h2>Nodes</h2>
        {nodes.length === 0 ? (
          <div class="ss-empty">No registered nodes</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Node</th>
                <th>Advertised address</th>
                <th>Epoch</th>
                <th>Slots</th>
                <th>Opening</th>
                <th>Placed</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {nodes.map((n) => (
                <tr key={n.nodeId}>
                  <td class="mono">{n.nodeId}</td>
                  <td class="mono">{n.advertisedAddress ?? '—'}</td>
                  <td class="mono">{n.nodeEpoch}</td>
                  <td class="mono">{n.slots}</td>
                  <td class="mono">{n.openingCount}</td>
                  <td class="mono">{n.placedCount}</td>
                  <td>{n.local ? <Pill state="ok" label="this node" /> : null}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section class="ss-section">
        <h2>Pending transfers</h2>
        {transfers.length === 0 ? (
          <div class="ss-empty">No transfers in flight</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Stream</th>
                <th>From node</th>
                <th>To node</th>
              </tr>
            </thead>
            <tbody>
              {transfers.map((t) => (
                <tr key={t.streamId}>
                  <td class="mono">{t.streamId}</td>
                  <td class="mono">{t.fromNode}</td>
                  <td class="mono">{t.toNode}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <footer class="ss-footer">
        {error
          ? `Last error: ${error}`
          : updatedAt
            ? `Updated ${updatedAt.toLocaleTimeString()} · polling every ${POLL_INTERVAL_MS / 1000}s`
            : 'Loading…'}
      </footer>
    </div>
  )
}
