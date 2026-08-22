export interface PendingTransfer {
  streamId: number
  fromNode: number
  toNode: number
}

export interface ClusterInfo {
  clusterId: string
  nodeId: number
  nodeEpoch: number
  advertisedAddress: string
  registered: boolean
  appliedIndex: number
  streamCount: number
  objectCount: number
  destroyedObjectBacklog: number
  pendingTransfers: PendingTransfer[]
  leaseHolder: boolean | null
}

export interface NodeInfo {
  nodeId: number
  nodeEpoch: number
  advertisedAddress: string | null
  slots: number
  local: boolean
  openingCount: number
  placedCount: number
}

export interface Readiness {
  ready: boolean
  serving: boolean
  registered: boolean
  appliedIndex: number
  nodeId: number
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
