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
package com.dremio.exec.planner;

import com.dremio.common.exceptions.UserCancellationException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.CoordinationProtos;
import org.slf4j.Logger;

public final class ExceptionUtils {

  private ExceptionUtils() {}

  public static void throwUserException(
      String message,
      Throwable t,
      PlannerSettings plannerSettings,
      PlannerPhase phase,
      UserException.AttemptCompletionState attemptCompletionState,
      Logger logger) {
    UserException.Builder builder;
    if (t != null) {
      builder = UserException.planError(t);
    } else {
      builder = UserException.planError();
    }
    builder = builder.message(message);
    builder = builder.attemptCompletionState(attemptCompletionState);
    if (phase != null) {
      builder = builder.addContext("Planner Phase", phase.description);
    }
    CoordinationProtos.NodeEndpoint nodeEndpoint = plannerSettings.getNodeEndpoint();
    if (nodeEndpoint != null) {
      builder = builder.addIdentity(nodeEndpoint);
    }
    String cancelContext = plannerSettings.getCancelContext();
    if (cancelContext != null && !cancelContext.isEmpty()) {
      builder = builder.addContext(cancelContext);
    }
    throw builder.build(logger);
  }

  public static void throwUserCancellationException(PlannerSettings plannerSettings) {
    throw new UserCancellationException(plannerSettings.getCancelReason());
  }

  public static String collapseExceptionMessages(Throwable throwable) {
    StringBuilder sb = new StringBuilder();

    while (throwable != null) {
      String message = throwable.getMessage();
      if (message != null) {
        sb.append(message).append(' ');
      }
      Throwable cause = throwable.getCause();
      throwable = cause != throwable ? cause : null;
    }

    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }
}
