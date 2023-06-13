/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.work.protector;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.sabot.exec.AbstractHeapClawBackStrategy;
import com.google.common.base.Preconditions;

/**
 * Allows us to carry various types of incoming user requests in a single
 * wrapper until a particular command runner can be resolved.
 */
public class UserResult {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserResult.class);

  private final Object extraValue;
  private final QueryId queryId;
  private final QueryState state;
  private final QueryProfile profile;
  private final UserException exception;
  private final String cancelReason;
  private final boolean clientCancelled;
  private final boolean timedoutWaitingForEngine;
  private final boolean runTimeExceeded;

  private QueryResult result;

  public UserResult(Object extraValue, QueryId queryId, QueryState state, QueryProfile profile,
                    UserException exception, String cancelReason, boolean clientCancelled,
                    boolean timedoutWaitingForEngine, boolean runTimeExceeded) {
    this.extraValue = extraValue;
    this.queryId = queryId;
    this.state = state;
    this.profile = profile;
    this.exception = exception;
    this.cancelReason = cancelReason;
    this.clientCancelled = clientCancelled;
    this.timedoutWaitingForEngine = timedoutWaitingForEngine;
    this.runTimeExceeded = runTimeExceeded;
  }

  public UserException.AttemptCompletionState getAttemptCompletionState() {
    if (state == QueryState.COMPLETED) {
      // Attempt completed successfully
      return UserException.AttemptCompletionState.SUCCESS;
    }

    if (this.clientCancelled) {
      return UserException.AttemptCompletionState.CLIENT_CANCELLED;
    }

    // Query did not complete successfully and was not cancelled
    if (this.timedoutWaitingForEngine) {
      return UserException.AttemptCompletionState.ENGINE_TIMEOUT;
    }

    if (this.runTimeExceeded) {
      return UserException.AttemptCompletionState.RUNTIME_EXCEEDED;
    }

    if (exception == null) {
      return UserException.AttemptCompletionState.UNKNOWN;
    }

    UserException.AttemptCompletionState attemptCompletionState = exception.getAttemptCompletionState();
    if (attemptCompletionState != UserException.AttemptCompletionState.UNKNOWN) {
      return attemptCompletionState;
    }

    String errMessage = exception.getMessage();
    if ((errMessage != null) && (errMessage.indexOf(AbstractHeapClawBackStrategy.FAIL_CONTEXT) >= 0)) {
      return UserException.AttemptCompletionState.HEAP_MONITOR_E;
    }

    return UserException.AttemptCompletionState.DREMIO_PB_ERROR;
  }

  public QueryResult toQueryResult() {
    if (result == null) {
      QueryResult.Builder resultBuilder = QueryResult.newBuilder()
          .setQueryId(queryId)
          .setQueryState(state);

      if (exception != null) {
        resultBuilder.addError(exception.getOrCreatePBError(true));
      }

      if (state == QueryState.CANCELED && !clientCancelled) {
        // from client POV, CANCELED becomes FAILED only if server caused the cancellation
        resultBuilder.setQueryState(QueryState.FAILED);
        resultBuilder.addError(UserException.resourceError()
            .message(cancelReason)
            .build(logger)
            .getOrCreatePBError(true));
      }
      result = resultBuilder.build();
    }

    return result;
  }

  public QueryProfile getProfile() {
    return profile;
  }

  public QueryState getState() {
    return state;
  }

  public UserException getException() {
    return exception;
  }

  public String getCancelReason() {
    return cancelReason;
  }

  @SuppressWarnings("unchecked")
  public <X> X unwrap(Class<X> clazz) {
    Preconditions.checkNotNull(extraValue, "Extra value is not available.");
    Preconditions.checkArgument(clazz.isAssignableFrom(extraValue.getClass()),
      "Expected a value of type %s but was holding a value of type %s.",
      clazz.getName(), extraValue.getClass().getName());
    return (X) extraValue;
  }

  public boolean hasException() {
    return exception != null;
  }

  public UserResult withNewQueryId(QueryId newQueryId) {
    return new UserResult(extraValue, newQueryId, state, profile, exception, cancelReason, clientCancelled, timedoutWaitingForEngine, runTimeExceeded);
  }

  public UserResult withException(Exception ex) {
    UserException exception = this.exception;
    if (exception != null) {
      exception.addSuppressed(ex);
      exception = UserException.systemError(exception)
          .build(logger);
    } else {
      exception = UserException.systemError(ex)
          .build(logger);
    }

    QueryProfile profile = this.profile;
    if (profile != null) {
      QueryProfile.Builder builder = profile.toBuilder();
      builder.setState(QueryState.FAILED);
      profile = addError(exception, builder).build();
    }

    return new UserResult(extraValue, queryId, QueryState.FAILED, profile, exception, cancelReason, clientCancelled, timedoutWaitingForEngine, runTimeExceeded);
  }

  public static QueryProfile.Builder addError(UserException ex, QueryProfile.Builder profileBuilder) {
    if (ex != null) {
      profileBuilder.setError(ex.getMessage())
          .setVerboseError(ex.getVerboseMessage(false))
          .setErrorId(ex.getErrorId());
      if (ex.getErrorLocation() != null) {
        profileBuilder.setErrorNode(ex.getErrorLocation());
      }
    }
    return profileBuilder;
  }

  public UserResult replaceException(UserException e) {
    UserException exception =  UserException.systemError(e)
        .build(logger);
    return new UserResult(extraValue, queryId, QueryState.FAILED, profile, exception, cancelReason, clientCancelled, timedoutWaitingForEngine, runTimeExceeded);
  }
}
