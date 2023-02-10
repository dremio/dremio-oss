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
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.proto.model.attempts.AttemptReason;

/**
 * Encapsulates the re-attempt logic: when re-attempts are possible, and how various re-attempt reasons should be
 * handled.
 */
public interface ReAttemptHandler {

  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReAttemptHandler.class);

  /**
   * @return true if one of the previous attempts failed with an out of memory
   */
  boolean hasOOM();

  /**
   * allows the handler to reset its internal state if needed
   */
  void newAttempt();

  /**
   * Called by Foreman before re-attempting a query to check if it's possible to recover from a particular failure
   * @param context reattempt context
   * @return attempt reason, NONE if re-attempt not possible
   */
  AttemptReason isRecoverable(final ReAttemptContext context);

  /**
   * Called whenever a data batch is about to be sent to the client.<br>
   * Can be used to check if schema changes are allowed and if future schema changes are recoverable
   * @param result data batch about to be sent to the client
   * @return data batch, patched if necessary, that should be sent to the client
   * @throws UserException if schema change is not supported
   */
  QueryWritableBatch convertIfNecessary(QueryWritableBatch result);
}
