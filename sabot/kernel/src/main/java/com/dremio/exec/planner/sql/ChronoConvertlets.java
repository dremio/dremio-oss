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
package com.dremio.exec.planner.sql;

import java.util.stream.Collectors;

import org.apache.arrow.vector.util.DateUtility;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.joda.time.DateMidnight;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

import com.dremio.common.util.DateTimes;
import com.dremio.sabot.exec.context.ContextInformation;
import com.google.common.base.Preconditions;

/**
 * {@link SqlRexConvertlet Convertlets} related to data/time that rely on contextual information.
 */
public class ChronoConvertlets {

  private static abstract class ChronoConvertlet implements SqlRexConvertlet {

    private final ContextInformation contextInformation;

    ChronoConvertlet(ContextInformation contextInformation) {
      this.contextInformation = Preconditions.checkNotNull(contextInformation, "context information is required");
    }

    ContextInformation getContextInformation() {
      return contextInformation;
    }

    static int getReturnTypePrecision(SqlRexContext cx, SqlCall call) {
      return cx.getRexBuilder()
          .deriveReturnType(call.getOperator(),
              call.getOperandList()
                  .stream()
                  .map(cx::convertExpression)
                  .collect(Collectors.toList()))
          .getPrecision();
    }
  }

  public static final class CurrentTimeStampConvertlet extends ChronoConvertlet {

    public CurrentTimeStampConvertlet(ContextInformation contextInformation) {
      super(contextInformation);
    }

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      return cx.getRexBuilder()
          .makeTimestampLiteral(
              DateTimes.toDateTime(
                  new LocalDateTime(getContextInformation().getQueryStartTime(),
                      DateTimeZone.UTC))
                  .toCalendar(null), // null sets locale to default locale
              getReturnTypePrecision(cx, call));
    }
  }

  public static final class CurrentTimeConvertlet extends ChronoConvertlet {

    public CurrentTimeConvertlet(ContextInformation contextInformation) {
      super(contextInformation);
    }

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      final int timeZoneIndex = getContextInformation().getRootFragmentTimeZone();
      final DateTimeZone timeZone = DateTimeZone.forID(DateUtility.getTimeZone(timeZoneIndex));
      final LocalDateTime dateTime = new LocalDateTime(getContextInformation().getQueryStartTime(), timeZone);
      final long queryStartTime =
          (dateTime.getHourOfDay() * DateUtility.hoursToMillis) +
              (dateTime.getMinuteOfHour() * DateUtility.minutesToMillis) +
              (dateTime.getSecondOfMinute() * DateUtility.secondsToMillis) +
              (dateTime.getMillisOfSecond());

      return cx.getRexBuilder()
          .makeTimeLiteral(
              DateTimes.toDateTime(new LocalDateTime(queryStartTime, DateTimeZone.UTC))
                  .toCalendar(null), // null sets locale to default locale
              getReturnTypePrecision(cx, call));
    }
  }

  public static final class CurrentDateConvertlet extends ChronoConvertlet {

    public CurrentDateConvertlet(ContextInformation contextInformation) {
      super(contextInformation);
    }

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      final int timeZoneIndex = getContextInformation().getRootFragmentTimeZone();
      final DateTimeZone timeZone = DateTimeZone.forID(DateUtility.getTimeZone(timeZoneIndex));
      final LocalDateTime dateTime = new LocalDateTime(getContextInformation().getQueryStartTime(), timeZone);
      final long midNightAsMillis =
          new DateMidnight(dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth(),
              timeZone)
              .withZoneRetainFields(DateTimeZone.UTC)
              .getMillis();

      return cx.getRexBuilder()
          .makeDateLiteral(DateTimes.toDateTime(
              new LocalDateTime(midNightAsMillis, DateTimeZone.UTC))
              .toCalendar(null)); // null sets locale to default locale
    }
  }
}
