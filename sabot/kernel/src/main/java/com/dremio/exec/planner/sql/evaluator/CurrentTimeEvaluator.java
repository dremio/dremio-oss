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

import org.apache.arrow.vector.util.DateUtility;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

import com.dremio.common.util.DateTimes;
import com.dremio.common.util.JodaDateUtility;

public final class CurrentTimeEvaluator implements FunctionEval {
  public static final CurrentTimeEvaluator INSTANCE = new CurrentTimeEvaluator();

  private CurrentTimeEvaluator() {}

  @Override
  public RexNode evaluate(EvaluationContext cx, RexCall call) {
    final int timeZoneIndex = cx.getContextInformation().getRootFragmentTimeZone();
    final DateTimeZone timeZone = DateTimeZone.forID(JodaDateUtility.getTimeZone(timeZoneIndex));
    final LocalDateTime dateTime = new LocalDateTime(cx.getContextInformation().getQueryStartTime(), timeZone);
    final long queryStartTime =
      ((long) dateTime.getHourOfDay() * DateUtility.hoursToMillis) +
        ((long) dateTime.getMinuteOfHour() * DateUtility.minutesToMillis) +
        ((long) dateTime.getSecondOfMinute() * DateUtility.secondsToMillis) +
        (dateTime.getMillisOfSecond());

    return cx.getRexBuilder().makeTimeLiteral(
      DateTimes.toDateTime(new LocalDateTime(queryStartTime, timeZone))
        .toCalendar(null), // null sets locale to default locale
      call.getType().getPrecision());
  }
}
