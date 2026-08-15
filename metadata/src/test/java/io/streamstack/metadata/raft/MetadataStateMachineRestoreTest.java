package io.streamstack.metadata.raft;

import io.streamstack.metadata.model.MetadataCommand;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MetadataStateMachineRestoreTest {

    @Test
    void restoresEncodedStateIntoEmptyStateMachine() {
        MetadataStateMachine source = new MetadataStateMachine();

        source.applyCommand(new MetadataCommand.RegisterNode(1, 10, "http://127.0.0.1:4437"));
        source.applyCommand(new MetadataCommand.CreateStream(1, 10));
        source.applyCommand(new MetadataCommand.PutKV("stream:/streams/demo",
            "meta".getBytes(StandardCharsets.UTF_8)));
        byte[] bytes = source.stateDigestSource();

        MetadataStateMachine target = new MetadataStateMachine();

        target.restore(bytes);
        assertEquals(1, target.streamControlManager().nextAssignedStreamId());
        assertEquals(1, target.streamControlManager().streamsMetadata().size());
        assertEquals(10, target.streamControlManager().nodeEpochs().get(1));
        assertEquals("http://127.0.0.1:4437", target.streamControlManager().nodeAddresses().get(1));
        assertArrayEquals("meta".getBytes(StandardCharsets.UTF_8),
            target.kvControlManager().get("stream:/streams/demo"));
    }

    @Test
    void rejectsRestoreIntoNonEmptyStateMachine() {
        MetadataStateMachine source = new MetadataStateMachine();

        source.applyCommand(new MetadataCommand.RegisterNode(1, 10, ""));
        byte[] bytes = source.stateDigestSource();

        MetadataStateMachine target = new MetadataStateMachine();

        target.applyCommand(new MetadataCommand.RegisterNode(2, 20, ""));
        assertThrows(IllegalStateException.class, () -> target.restore(bytes));
    }
}
