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
