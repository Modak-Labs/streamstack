package io.streamstack.metadata.stream;

import io.streamstack.metadata.MetadataException;
import io.streamstack.s3.metadata.StreamMetadata;
import io.streamstack.s3.metadata.StreamState;
import io.streamstack.s3.streams.StreamMetadataListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public final class StreamControlManager {

    private long nextAssignedStreamId;

    private final Map<Long, StreamMetadata> streamsMetadata = new HashMap<>();

    private final Map<Integer, Long> nodeEpochs = new HashMap<>();

    private final Map<Integer, String> nodeAddresses = new HashMap<>();
    private final ConcurrentHashMap<Long, CopyOnWriteArrayList<StreamMetadataListener>> listeners =
        new ConcurrentHashMap<>();

    public StreamControlManager() {
        this.nextAssignedStreamId = 0L;
    }

    public long nextAssignedStreamId() {
        return nextAssignedStreamId;
    }

    public Map<Long, StreamMetadata> streamsMetadata() {
        return streamsMetadata;
    }

    public Map<Integer, Long> nodeEpochs() {
        return nodeEpochs;
    }

    public Map<Integer, String> nodeAddresses() {
        return nodeAddresses;
    }

    public void registerNode(int nodeId, long nodeEpoch) {
        registerNode(nodeId, nodeEpoch, "");
    }

    public void registerNode(int nodeId, long nodeEpoch, String httpAddress) {
        Long current = nodeEpochs.get(nodeId);
        if (Objects.nonNull(current) && current > nodeEpoch) {
            throw MetadataException.nodeEpochMismatch(
                "node " + nodeId + " epoch " + current + " fences register with epoch " + nodeEpoch);
        }
        nodeEpochs.put(nodeId, nodeEpoch);
        if (Objects.nonNull(httpAddress) && !httpAddress.isEmpty()) {
            nodeAddresses.put(nodeId, httpAddress);
        }
    }

    public String getNodeAddress(int nodeId) {
        return nodeAddresses.get(nodeId);
    }

    public void nodeEpochCheck(int nodeId, long nodeEpoch) {
        Long current = nodeEpochs.get(nodeId);
        if (Objects.isNull(current)) {
            throw MetadataException.nodeEpochMismatch("node " + nodeId + " is not registered");
        }
        if (current != nodeEpoch) {
            throw MetadataException.nodeEpochMismatch(
                "node " + nodeId + " epoch mismatch current=" + current + " request=" + nodeEpoch);
        }
    }

    public long createStream(int nodeId, long nodeEpoch) {
        nodeEpochCheck(nodeId, nodeEpoch);
        long streamId = nextAssignedStreamId++;
        StreamMetadata metadata = new StreamMetadata(streamId, -1, 0, 0, StreamState.CLOSED);
        metadata.nodeId(nodeId);
        streamsMetadata.put(streamId, metadata);
        return streamId;
    }

    public StreamMetadata openStream(int nodeId, long nodeEpoch, long streamId, long epoch) {
        nodeEpochCheck(nodeId, nodeEpoch);
        StreamMetadata stream = requireStream(streamId);
        if (stream.epoch() > epoch) {
            throw MetadataException.streamFenced(
                "stream " + streamId + " epoch " + stream.epoch() + " fences request epoch " + epoch);
        }
        if (stream.epoch() == epoch) {
            if (stream.state() == StreamState.OPENED && stream.nodeId() == nodeId) {
                return stream;
            }
            throw MetadataException.streamFenced(
                "stream " + streamId + " epoch " + epoch + " already used");
        }
        if (stream.state() == StreamState.OPENED) {
            throw MetadataException.streamNotClosed(streamId);
        }
        stream.epoch(epoch);
        stream.state(StreamState.OPENED);
        stream.nodeId(nodeId);
        return stream;
    }

    public void trimStream(int nodeId, long nodeEpoch, long streamId, long epoch, long newStartOffset) {
        nodeEpochCheck(nodeId, nodeEpoch);
        StreamMetadata stream = requireOpenedStream(streamId, epoch);
        if (newStartOffset < stream.startOffset()) {
            throw MetadataException.unexpected(
                "stream " + streamId + " new start offset " + newStartOffset
                    + " is less than current start offset " + stream.startOffset());
        }
        if (newStartOffset > stream.endOffset()) {
            throw MetadataException.unexpected(
                "stream " + streamId + " new start offset " + newStartOffset
                    + " is greater than current end offset " + stream.endOffset());
        }
        stream.startOffset(newStartOffset);
    }

    public void closeStream(int nodeId, long nodeEpoch, long streamId, long epoch) {
        nodeEpochCheck(nodeId, nodeEpoch);
        StreamMetadata stream = requireStream(streamId);
        if (stream.state() == StreamState.CLOSED && stream.epoch() == epoch) {
            return;
        }
        if (stream.state() != StreamState.OPENED) {
            throw MetadataException.unexpected("stream " + streamId + " is not opened");
        }
        if (stream.epoch() != epoch) {
            throw MetadataException.expiredEpoch(
                "stream " + streamId + " epoch " + epoch + " is not equal to current epoch " + stream.epoch());
        }
        stream.state(StreamState.CLOSED);
    }

    public void deleteStream(int nodeId, long nodeEpoch, long streamId, long epoch) {
        nodeEpochCheck(nodeId, nodeEpoch);
        StreamMetadata stream = streamsMetadata.get(streamId);
        if (Objects.isNull(stream)) {
            return;
        }
        if (stream.state() != StreamState.CLOSED) {
            throw MetadataException.streamNotClosed(streamId);
        }
        if (stream.epoch() != epoch) {
            throw MetadataException.expiredEpoch(
                "stream " + streamId + " epoch " + epoch + " is not equal to current epoch " + stream.epoch());
        }
        streamsMetadata.remove(streamId);
        listeners.remove(streamId);
    }

    public void advanceEndOffset(long streamId, long startOffset, long endOffset, boolean compact) {
        StreamMetadata stream = requireStream(streamId);
        if (compact) {
            if (stream.endOffset() < endOffset) {
                throw MetadataException.unexpected(
                    "stream " + streamId + " end offset " + stream.endOffset() + " is lesser than request " + endOffset);
            }
            if (stream.startOffset() > startOffset) {
                throw MetadataException.unexpected(
                    "stream " + streamId + " start offset " + stream.startOffset()
                        + " is greater than request " + startOffset);
            }
            return;
        }
        if (stream.endOffset() != startOffset) {
            throw MetadataException.unexpected(
                "stream " + streamId + " end offset " + stream.endOffset()
                    + " is not equal to start offset of request " + startOffset);
        }
        stream.endOffset(endOffset);
    }

    public List<StreamMetadata> getOpeningStreams(int nodeId) {
        return streamsMetadata.values().stream()
            .filter(stream -> stream.state() == StreamState.OPENED)
            .filter(stream -> stream.nodeId() == nodeId)
            .map(StreamControlManager::copyOf)
            .collect(Collectors.toList());
    }

    public List<StreamMetadata> getStreams(List<Long> streamIds) {
        return streamIds.stream()
            .map(streamsMetadata::get)
            .filter(Objects::nonNull)
            .map(StreamControlManager::copyOf)
            .collect(Collectors.toList());
    }

    public StreamMetadata getStream(long streamId) {
        return streamsMetadata.get(streamId);
    }

    public StreamMetadataListener.Handle addMetadataListener(long streamId, StreamMetadataListener listener) {
        listeners.computeIfAbsent(streamId, id -> new CopyOnWriteArrayList<>()).add(listener);
        return () -> {
            CopyOnWriteArrayList<StreamMetadataListener> list = listeners.get(streamId);
            if (Objects.nonNull(list)) {
                list.remove(listener);
            }
        };
    }

    public Runnable notification(long streamId) {
        StreamMetadata metadata = streamsMetadata.get(streamId);
        if (Objects.isNull(metadata)) {
            return null;
        }
        CopyOnWriteArrayList<StreamMetadataListener> list = listeners.get(streamId);
        if (Objects.isNull(list) || list.isEmpty()) {
            return null;
        }
        StreamMetadata copy = copyOf(metadata);
        return () -> {
            for (StreamMetadataListener listener : list) {
                listener.onNewStreamMetadata(copy);
            }
        };
    }

    public void replaceAll(long nextAssignedStreamId, Map<Long, StreamMetadata> streams, Map<Integer, Long> nodeEpochs) {
        replaceAll(nextAssignedStreamId, streams, nodeEpochs, Map.of());
    }

    public void replaceAll(
        long nextAssignedStreamId,
        Map<Long, StreamMetadata> streams,
        Map<Integer, Long> nodeEpochs,
        Map<Integer, String> nodeAddresses) {
        this.nextAssignedStreamId = nextAssignedStreamId;
        this.streamsMetadata.clear();
        this.streamsMetadata.putAll(streams);
        this.nodeEpochs.clear();
        this.nodeEpochs.putAll(nodeEpochs);
        this.nodeAddresses.clear();
        if (Objects.nonNull(nodeAddresses)) {
            this.nodeAddresses.putAll(nodeAddresses);
        }
    }

    public List<StreamMetadata> snapshotStreams() {
        return new ArrayList<>(streamsMetadata.values());
    }

    private StreamMetadata requireOpenedStream(long streamId, long epoch) {
        StreamMetadata stream = requireStream(streamId);
        if (stream.state() != StreamState.OPENED) {
            throw MetadataException.unexpected("stream " + streamId + " is not opened");
        }
        if (stream.epoch() != epoch) {
            throw MetadataException.expiredEpoch(
                "stream " + streamId + " epoch " + epoch + " is not equal to current epoch " + stream.epoch());
        }
        return stream;
    }

    private StreamMetadata requireStream(long streamId) {
        StreamMetadata stream = streamsMetadata.get(streamId);
        if (Objects.isNull(stream)) {
            throw MetadataException.streamNotExist(streamId);
        }
        return stream;
    }

    private static StreamMetadata copyOf(StreamMetadata stream) {
        StreamMetadata copy = new StreamMetadata(
            stream.streamId(), stream.epoch(), stream.startOffset(), stream.endOffset(), stream.state());
        copy.nodeId(stream.nodeId());
        return copy;
    }
}
