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

function Logo() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 11 11" role="img" aria-label="StreamStack">
      <rect width="11" height="11" rx="0.35" fill="#fff" />
      <path
        fill="#0c0c0d"
        d="M1.000 1.200A0.2 0.2 0 0 1 1.200 1.000L9.800 1.000A0.2 0.2 0 0 1 10.000 1.200L10.000 1.800A0.2 0.2 0 0 1 9.800 2.000L2.200 2.000A0.2 0.2 0 0 0 2.000 2.200L2.000 2.800A0.2 0.2 0 0 0 2.200 3.000L9.800 3.000A0.2 0.2 0 0 1 10.000 3.200L10.000 5.800A0.2 0.2 0 0 1 9.800 6.000L2.200 6.000A0.2 0.2 0 0 0 2.000 6.200L2.000 6.800A0.2 0.2 0 0 0 2.200 7.000L9.800 7.000A0.2 0.2 0 0 1 10.000 7.200L10.000 9.800A0.2 0.2 0 0 1 9.800 10.000L1.200 10.000A0.2 0.2 0 0 1 1.000 9.800L1.000 9.200A0.2 0.2 0 0 1 1.200 9.000L8.800 9.000A0.2 0.2 0 0 0 9.000 8.800L9.000 8.200A0.2 0.2 0 0 0 8.800 8.000L1.200 8.000A0.2 0.2 0 0 1 1.000 7.800L1.000 5.200A0.2 0.2 0 0 1 1.200 5.000L8.800 5.000A0.2 0.2 0 0 0 9.000 4.800L9.000 4.200A0.2 0.2 0 0 0 8.800 4.000L1.200 4.000A0.2 0.2 0 0 1 1.000 3.800Z"
      />
    </svg>
  )
}

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

  return (
    <div class="ss-app">
      <header class="ss-topbar">
        <Logo />
        <span class="name">StreamStack</span>
        <span class="tag">admin</span>
        <span class="spacer" />
        {error ? <Pill state="err" label="unreachable" /> : <Pill state={readyState} label={readyLabel} />}
      </header>

      <div class="ss-grid">
        <Stat label="Cluster" value={cluster?.clusterId} small />
        <Stat label="Node" value={cluster?.nodeId} />
        <Stat label="Raft leader" value={cluster?.raft.leader} small />
        <Stat label="Applied index" value={cluster?.raft.appliedIndex} />
        <Stat label="Streams" value={cluster?.streamCount} />
        <Stat label="Objects" value={cluster?.objectCount} />
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
              <th>Raft role</th>
              <td class="mono">{cluster ? (cluster.raft.isLeader ? 'leader' : 'follower') : '—'}</td>
            </tr>
            <tr>
              <th>Registered</th>
              <td class="mono">{cluster ? String(cluster.registered) : '—'}</td>
            </tr>
            <tr>
              <th>Apply success / fail</th>
              <td class="mono">
                {cluster ? `${cluster.raft.applySuccessCount} / ${cluster.raft.applyFailCount}` : '—'}
              </td>
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
                <th></th>
              </tr>
            </thead>
            <tbody>
              {nodes.map((n) => (
                <tr key={n.nodeId}>
                  <td class="mono">{n.nodeId}</td>
                  <td class="mono">{n.advertisedAddress ?? '—'}</td>
                  <td class="mono">{n.nodeEpoch}</td>
                  <td>{n.local ? <Pill state="ok" label="this node" /> : null}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section class="ss-section">
        <h2>Raft peers</h2>
        {!cluster?.raft.peers || cluster.raft.peers.length === 0 ? (
          <div class="ss-empty">Peer list only available on the raft leader</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Peer</th>
              </tr>
            </thead>
            <tbody>
              {cluster.raft.peers.map((p) => (
                <tr key={p}>
                  <td class="mono">{p}</td>
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
