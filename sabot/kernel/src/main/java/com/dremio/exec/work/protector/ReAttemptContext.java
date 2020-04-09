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

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.AttemptId;

/**
 * Context for reattempting.
 */
public class ReAttemptContext {

  private final AttemptId attemptId;
  private final UserException exception;
  private final boolean containsHashAggregate;
  private final boolean isCTAS;

  ReAttemptContext(AttemptId attemptId, UserException exception,
                   boolean containsHashAggregate, boolean isCTAS) {
    this.attemptId = checkNotNull(attemptId);
    this.exception = exception;
    this.containsHashAggregate = containsHashAggregate;
    this.isCTAS = isCTAS;
  }

  public AttemptId getAttemptId() {
    return attemptId;
  }

  public UserException getException() {
    return exception;
  }

  public boolean containsHashAggregate() {
    return containsHashAggregate;
  }

  public boolean isCTAS() {
    return isCTAS;
  }
}
