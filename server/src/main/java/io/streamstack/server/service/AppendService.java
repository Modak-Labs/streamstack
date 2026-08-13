package io.streamstack.server.service;

import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.AppendResult;
import io.streamstack.server.model.StreamServiceException;
import io.streamstack.server.model.SubmittedAppendResult;

public interface AppendService {

    AppendResult append(AppendCommand command) throws StreamServiceException;

    default SubmittedAppendResult submit(AppendCommand command) throws StreamServiceException {
        return SubmittedAppendResult.completed(append(command));
    }
}
