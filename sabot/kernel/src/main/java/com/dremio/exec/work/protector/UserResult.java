/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

  private QueryResult result;

  public UserResult(Object extraValue, QueryId queryId, QueryState state, QueryProfile profile,
                    UserException exception) {
    this.extraValue = extraValue;
    this.queryId = queryId;
    this.state = state;
    this.profile = profile;
    this.exception = exception;
  }

  public QueryResult toQueryResult() {
    if (result == null) {
      QueryResult.Builder resultBuilder = QueryResult.newBuilder()
          .setQueryId(queryId)
          .setQueryState(state);

      if (exception != null) {
        resultBuilder.addError(exception.getOrCreatePBError(true));
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
    return new UserResult(extraValue, newQueryId, state, profile, exception);
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

    return new UserResult(extraValue, queryId, QueryState.FAILED, profile, exception);
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
}
