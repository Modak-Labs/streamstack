package io.streamstack.server.service;

public record StreamService(
    StreamLifecycleService lifecycle,
    AppendService append,
    ReadService read,
    OwnershipService ownership) {
}
