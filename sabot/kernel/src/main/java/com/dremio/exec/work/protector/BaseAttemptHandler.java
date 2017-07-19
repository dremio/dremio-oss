/*
 * Copyright (C) 2017 Dremio Corporation
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
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.work.AttemptId;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.sabot.op.screen.QueryWritableBatch;
import com.google.common.base.Preconditions;

/**
 * Base implementation of {@link ReAttemptHandler}
 */
abstract class BaseAttemptHandler implements ReAttemptHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReAttemptHandler.class);

  private static final int MAX_ATTEMPTS = 10; //TODO make this configurable ?

  private OptionManager options;

  private boolean recoveringFromOOM;

  private volatile boolean nonEmptyBatchSent; // did we send any data to the client ?

  BaseAttemptHandler(OptionManager options) {
    this.options = Preconditions.checkNotNull(options, "options shouldn't be null");
  }

  @Override
  public boolean hasOOM() {
    return recoveringFromOOM;
  }

  @Override
  public void newAttempt() {}

  protected boolean failIfNonEmptySent() {
    return true;
  }

  @Override
  public AttemptReason isRecoverable(final ReAttemptContext context) {
    final AttemptId attemptId = context.getAttemptId();
    if (nonEmptyBatchSent && failIfNonEmptySent()) {
      logger.info("{}: cannot re-attempt the query, data already sent to the client", attemptId);
      return AttemptReason.NONE;
    }

    if (attemptId.getAttemptNum() == MAX_ATTEMPTS-1) {
      logger.info("{}: reached maximum allowed number of attempts", attemptId);
      return AttemptReason.NONE; // we exceeded max allowed attempts
    }

    final UserException ex = context.getException();
    if (ex.getErrorType() == ErrorType.OUT_OF_MEMORY) {
      if (!context.containsHashAggregate() || recoveringFromOOM
        // TODO(DX-5912): check this condition after merge join is implemented
        // || !options.getOption(PlannerSettings.HASHJOIN)
        ) {
        // we are already using sort-based operations
        logger.info("{}: couldn't recover from an out of memory failure as sort-based options are already set",
                attemptId);
        return AttemptReason.NONE;
      }

      recoveringFromOOM = true;
      // we should probably check if the sort-based options aren't already set
      return AttemptReason.OUT_OF_MEMORY;
    }

    if (ex.getErrorType() == ErrorType.SCHEMA_CHANGE) {
      return AttemptReason.SCHEMA_CHANGE;
    }

    return AttemptReason.NONE;
  }

  @Override
  public QueryWritableBatch convertIfNecessary(QueryWritableBatch result) {

    if (result.getHeader().getDef().getRecordCount() > 0) {
      nonEmptyBatchSent = true;
    }

    return result; // does nothing by default
  }
}
