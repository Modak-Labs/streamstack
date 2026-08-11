package io.streamstack.metadata.raft;

import io.streamstack.api.KVClient;
import io.streamstack.api.KeyValue;
import io.streamstack.api.KeyValue.Key;
import io.streamstack.api.KeyValue.Value;
import io.streamstack.metadata.model.MetadataCommand;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public final class RaftKVClient implements KVClient {
    private final MetadataNode metadataNode;
    private final MetadataClient client;

    public RaftKVClient(MetadataNode metadataNode) {
        this.metadataNode = metadataNode;
        this.client = metadataNode.client();
    }

    @Override
    public CompletableFuture<Value> putKVIfAbsent(KeyValue keyValue) {
        byte[] value = toBytes(keyValue.value());
        return client.propose(new MetadataCommand.PutKVIfAbsent(keyValue.key().get(), value))
            .thenApply(RaftKVClient::toValue);
    }

    @Override
    public CompletableFuture<Value> putKV(KeyValue keyValue) {
        byte[] value = toBytes(keyValue.value());
        return client.propose(new MetadataCommand.PutKV(keyValue.key().get(), value))
            .thenApply(RaftKVClient::toValue);
    }

    @Override
    public CompletableFuture<Value> getKV(Key key) {
        return client.readIndex(() ->
            metadataNode.stateMachine().kvControlManager().get(key.get()))
            .thenApply(bytes -> bytes == null ? null : Value.of(bytes));
    }

    @Override
    public CompletableFuture<Value> delKV(Key key) {
        return client.propose(new MetadataCommand.DeleteKV(key.get()))
            .thenApply(result -> result == null ? null : toValue(result));
    }

    private static byte[] toBytes(Value value) {
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException("value must not be null");
        }
        ByteBuffer buffer = value.get().duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static Value toValue(Object result) {
        if (result == null) {
            return null;
        }
        return Value.of((byte[]) result);
    }
}
