package io.streamstack.metadata.raft;

import java.net.ServerSocket;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class MetadataTestSupport {
    private MetadataTestSupport() {
    }

    public static int freePort() throws Exception {
        // Some RPC components reserve adjacent ports, so avoid the very top of the range.
        for (int i = 0; i < 100; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                int port = socket.getLocalPort();
                if (port < 65000) {
                    return port;
                }
            }
        }
        throw new IllegalStateException("could not allocate a usable free port");
    }

    public static MetadataNode awaitLeader(long timeout, TimeUnit unit, MetadataNode... nodes)
        throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            for (MetadataNode node : nodes) {
                if (node.isLeader()) {
                    return node;
                }
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("timed out waiting for metadata leader");
    }

    public static String stateDigest(MetadataNode node) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(node.stateMachine().stateDigestSource()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static void assertReplicasConverged(long timeout, TimeUnit unit, MetadataNode... nodes)
        throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        String mismatch = null;
        while (System.nanoTime() < deadline) {
            String first = stateDigest(nodes[0]);
            boolean allMatch = true;
            for (int i = 1; i < nodes.length; i++) {
                String other = stateDigest(nodes[i]);
                if (!first.equals(other)) {
                    allMatch = false;
                    mismatch = "node " + nodes[0].nodeId() + "=" + first
                        + " vs node " + nodes[i].nodeId() + "=" + other;
                    break;
                }
            }
            if (allMatch) {
                return;
            }
            Thread.sleep(100);
        }
        assertEquals(null, mismatch, "replicas did not converge: " + mismatch);
    }
}
