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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.google.common.annotations.VisibleForTesting;

import io.opentracing.Span;

/**
 * This class analyses a query attempt
 */
public class AttemptAnalyser {
  private static final Logger logger = LoggerFactory.getLogger(AttemptAnalyser.class);
  private static final String ATTEMPT_STATUS_ATTRIBUTE = "dremio.attempt.status";

  // Used in unit tests to verify the state of the last attempt
  @VisibleForTesting
  public static String LAST_ATTEMPT_COMPLETION_STATE = null;

  // attempt span
  private final Span attemptSpan;

  AttemptAnalyser(Span attemptSpan) {
    this.attemptSpan = attemptSpan;
  }

  private void setAttemptCompletionAttribute(String attemptCompletionState) {
    LAST_ATTEMPT_COMPLETION_STATE = attemptCompletionState;
    attemptSpan.setTag(ATTEMPT_STATUS_ATTRIBUTE, attemptCompletionState);
  }

  protected void analyseAttemptCompletion(final UserResult result) {
    UserException.AttemptCompletionState attemptCompletionState = result.getAttemptCompletionState();
    if (attemptCompletionState != UserException.AttemptCompletionState.DREMIO_PB_ERROR) {
      setAttemptCompletionAttribute(attemptCompletionState.toString());
      return;
    }

    UserException userException = result.getException();
    ErrorType errorType = userException.getErrorType();
    setAttemptCompletionAttribute("pb_" + errorType.toString());
  }
}
