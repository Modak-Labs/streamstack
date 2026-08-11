package io.streamstack.server.service;

import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.AppendResult;

public interface AppendService {
    AppendResult append(AppendCommand command) throws StreamServiceException;
}
