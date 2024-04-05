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
package com.dremio.service.execselector;

import com.dremio.common.exceptions.UserException;

/** Utilities for Executor selection. */
public final class ExecutorSelectionUtils {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ExecutorSelectionUtils.class);

  public static void throwEngineOffline(String queueTag) {
    if (queueTag == null || queueTag.isEmpty()) {
      queueTag = "Default";
    }
    throw UserException.resourceError()
        .message(String.format("The %s engine is not online.", queueTag))
        .build(logger);
  }

  private ExecutorSelectionUtils() {}
}
