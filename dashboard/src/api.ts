export interface ClusterInfo {
  clusterId: string
  nodeId: number
  nodeEpoch: number
  advertisedAddress: string
  registered: boolean
  raft: {
    leader: string | null
    isLeader: boolean
    appliedIndex: number
    applySuccessCount: number
    applyFailCount: number
    peers: string[] | null
  }
  streamCount: number
  objectCount: number
  destroyedObjectBacklog: number
}

export interface NodeInfo {
  nodeId: number
  nodeEpoch: number
  advertisedAddress: string | null
  local: boolean
}

export interface Readiness {
  ready: boolean
  started: boolean
  leaderKnown: boolean
  registered: boolean
}

export interface ArchivedSnapshot {
  key: string
  appliedIndex: number
  timestampMs: number
  size: number
}

export interface SnapshotArchiveInfo {
  archiveSuccessCount: number
  archiveFailureCount: number
  lastArchivedIndex: number
  snapshots: ArchivedSnapshot[]
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(path, { headers: { Accept: 'application/json' } })
  const body = (await res.json()) as T

  if (!res.ok && path !== '/ready') {
    throw new Error(`GET ${path} failed: ${res.status}`)
  }

  return body
}

export const fetchCluster = () => get<ClusterInfo>('/admin/cluster')
export const fetchNodes = () => get<{ nodes: NodeInfo[] }>('/admin/nodes')
export const fetchReady = () => get<Readiness>('/ready')

export async function fetchSnapshots(): Promise<SnapshotArchiveInfo | null> {
  const res = await fetch('/admin/snapshots', { headers: { Accept: 'application/json' } })

  if (res.status === 404) {
    return null
  }

  if (!res.ok) {
    throw new Error(`GET /admin/snapshots failed: ${res.status}`)
  }

  return (await res.json()) as SnapshotArchiveInfo
}
