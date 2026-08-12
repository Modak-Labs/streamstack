package io.streamstack.server.ds;

import java.net.ServerSocket;

final class TestPorts {

    private TestPorts() {
    }

    static int freePort() throws Exception {
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
}
