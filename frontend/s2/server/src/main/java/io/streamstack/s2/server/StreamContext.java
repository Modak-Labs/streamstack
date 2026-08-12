package io.streamstack.s2.server;

import com.fasterxml.jackson.databind.node.ObjectNode;

record StreamContext(String basin, String stream, ObjectNode basinDoc, ObjectNode streamDoc) {

    String coreName() {
        return BasinRegistry.coreStreamName(basin, stream);
    }
}
