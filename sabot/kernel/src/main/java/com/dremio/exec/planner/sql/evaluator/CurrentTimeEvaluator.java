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
package com.dremio.exec.planner.sql.evaluator;

import static com.dremio.common.util.LocalTimeUtility.getMillis;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.TimeString;

public final class CurrentTimeEvaluator implements FunctionEval {
  public static final CurrentTimeEvaluator INSTANCE = new CurrentTimeEvaluator();

  private CurrentTimeEvaluator() {}

  @Override
  public RexNode evaluate(EvaluationContext cx, RexCall call) {
    java.time.ZoneId zone = com.dremio.common.util.LocalTimeUtility.getTimeZoneId();
    java.time.LocalTime currentTime =
        com.dremio.common.util.LocalTimeUtility.getTimeFromMillis(
            cx.getContextInformation().getQueryStartTime(), zone);
    TimeString timeString =
        new TimeString(currentTime.getHour(), currentTime.getMinute(), currentTime.getSecond());
    timeString.withMillis(getMillis(currentTime));
    return cx.getRexBuilder().makeTimeLiteral(timeString, call.getType().getPrecision());
  }
}
