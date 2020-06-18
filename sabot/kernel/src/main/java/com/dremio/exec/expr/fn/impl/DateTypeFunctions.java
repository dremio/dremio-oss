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

package com.dremio.exec.expr.fn.impl;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.DateMilliHolder;
import org.apache.arrow.vector.holders.IntervalDayHolder;
import org.apache.arrow.vector.holders.IntervalYearHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.TimeMilliHolder;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.joda.time.chrono.ISOChronology;

import com.dremio.common.util.DateTimes;
import com.dremio.common.util.JodaDateUtility;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionSyntax;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.sabot.exec.context.ContextInformation;

public class DateTypeFunctions {

    /**
     * Function to check if a varchar value can be cast to a date.
     *
     * At the time of writing this function, several other databases were checked
     * for behavior compatibility. There was not a consensus between oracle and
     * Sql server about the expected behavior of this function, and Postgres
     * lacks it completely.
     *
     * Sql Server appears to have both a DATEFORMAT and language locale setting
     * that can change the values accepted by this function. Oracle appears to
     * support several formats, some of which are not mentioned in the Sql
     * Server docs. With the lack of standardization, we decided to implement
     * this function so that it would only consider date strings that would be
     * accepted by the cast function as valid.
     */
    @FunctionTemplate(name = "isdate", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL, costCategory = FunctionTemplate.FunctionCostCategory.COMPLEX)
    public static class IsDate implements SimpleFunction {

      @Param NullableVarCharHolder in;
      @Output NullableBitHolder out;

      @Override
      public void setup() { }

      @Override
      public void eval() {
        out.isSet = 1;
        // for a null input return false
        if (in.isSet == 0) {
          out.value = 0;
        } else {
          out.value = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.isReadableAsDate(in.buffer, in.start, in.end) ? 1 : 0;
        }
      }
    }

    @FunctionTemplate(name = "interval_year", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class IntervalYearType implements SimpleFunction {

        @Param  BigIntHolder inputYears;
        @Param  BigIntHolder inputMonths;
        @Output IntervalYearHolder out;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {

            out.value       = (int) ((inputYears.value * org.apache.arrow.vector.util.DateUtility.yearsToMonths) +
                                      (inputMonths.value));
        }
    }

    @FunctionTemplate(name = "interval_day", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class IntervalDayType implements SimpleFunction {

        @Param  BigIntHolder inputDays;
        @Param  BigIntHolder inputHours;
        @Param  BigIntHolder inputMinutes;
        @Param  BigIntHolder inputSeconds;
        @Param  BigIntHolder inputMillis;
        @Output IntervalDayHolder out;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {

            out.days  = (int) inputDays.value;
            out.milliseconds =  (int) ((inputHours.value * org.apache.arrow.vector.util.DateUtility.hoursToMillis) +
                                       (inputMinutes.value * org.apache.arrow.vector.util.DateUtility.minutesToMillis) +
                                       (inputSeconds.value * org.apache.arrow.vector.util.DateUtility.secondsToMillis) +
                                 (inputMillis.value));
        }
    }

    @FunctionTemplate(name = "datetype", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class DateType implements SimpleFunction {

        @Param  BigIntHolder inputYears;
        @Param  BigIntHolder inputMonths;
        @Param  BigIntHolder inputDays;
        @Output DateMilliHolder   out;
        @Inject FunctionErrorContext errCtx;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {
          try {
            out.value = DateTimes.toMillis(new org.joda.time.LocalDateTime((int) inputYears.value,
              (int) inputMonths.value,
              (int) inputDays.value,
              0,
              0,
              0,
              0,
              ISOChronology.getInstance(org.joda.time.DateTimeZone.UTC)));
          } catch (IllegalArgumentException e) {
            throw errCtx.error()
              .message(String.format("Unable to convert year=%d month=%d day=%d into a date", inputYears.value, inputMonths.value, inputDays.value))
              .build();
          }
        }
    }

    @FunctionTemplate(name = "timestamptype", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class TimeStampType implements SimpleFunction {

        @Param  BigIntHolder inputYears;
        @Param  BigIntHolder inputMonths;
        @Param  BigIntHolder inputDays;
        @Param  BigIntHolder inputHours;
        @Param  BigIntHolder inputMinutes;
        @Param  BigIntHolder inputSeconds;
        @Param  BigIntHolder inputMilliSeconds;
        @Output TimeStampMilliHolder out;
        @Inject FunctionErrorContext errCtx;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {
          try {
            out.value = DateTimes.toMillis(new org.joda.time.LocalDateTime((int)inputYears.value,
                                                            (int)inputMonths.value,
                                                            (int)inputDays.value,
                                                            (int)inputHours.value,
                                                            (int)inputMinutes.value,
                                                            (int)inputSeconds.value,
                                                            (int)inputMilliSeconds.value));
          } catch (IllegalArgumentException e) {
            throw errCtx.error()
              .message(String.format("Unable to convert year=%d month=%d day=%d hour=%d min=%d sec=%d ms=%d into a datetime",
                inputYears.value, inputMonths.value, inputDays.value, inputHours.value, inputMinutes.value, inputSeconds.value, inputMilliSeconds.value))
              .build();
          }
        }
    }

    @FunctionTemplate(name = "timetype", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class TimeType implements SimpleFunction {

        @Param  BigIntHolder inputHours;
        @Param  BigIntHolder inputMinutes;
        @Param  BigIntHolder inputSeconds;
        @Param  BigIntHolder inputMilliSeconds;
        @Output TimeMilliHolder   out;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {
            out.value = (int) ((inputHours.value * org.apache.arrow.vector.util.DateUtility.hoursToMillis) +
                               (inputMinutes.value * org.apache.arrow.vector.util.DateUtility.minutesToMillis) +
                               (inputSeconds.value * org.apache.arrow.vector.util.DateUtility.secondsToMillis) +
                                inputMilliSeconds.value);
        }
    }

    @FunctionTemplate(name = "current_date", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, syntax = FunctionSyntax.FUNCTION_ID)
    public static class CurrentDate implements SimpleFunction {
        @Workspace long queryStartDate;
        @Output DateMilliHolder out;
        @Inject ContextInformation contextInfo;

        @Override
        public void setup() {

            int timeZoneIndex = contextInfo.getRootFragmentTimeZone();
            org.joda.time.DateTimeZone timeZone = org.joda.time.DateTimeZone.forID(com.dremio.common.util.JodaDateUtility.getTimeZone(timeZoneIndex));
            org.joda.time.LocalDateTime now = new org.joda.time.LocalDateTime(contextInfo.getQueryStartTime(), timeZone);
            queryStartDate = (new org.joda.time.DateMidnight(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), timeZone)).
                withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis();
        }

        @Override
        public void eval() {
            out.value = queryStartDate;
        }
    }

    @FunctionTemplate(name = "current_date_utc", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, syntax = FunctionSyntax.FUNCTION_ID)
    public static class CurrentDateUTC implements SimpleFunction {
        @Workspace long queryStartDate;
        @Output DateMilliHolder out;
        @Inject ContextInformation contextInfo;

        @Override
        public void setup() {
            org.joda.time.LocalDateTime now = new org.joda.time.LocalDateTime(contextInfo.getQueryStartTime(), org.joda.time.DateTimeZone.UTC);
            queryStartDate = (new org.joda.time.DateMidnight(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), org.joda.time.DateTimeZone.UTC)).
                withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis();
        }

        @Override
        public void eval() {
            out.value = queryStartDate;
        }
    }

    @FunctionTemplate(name = "timeofday", isDeterministic = false)
    public static class TimeOfDay implements SimpleFunction {
        @Inject ArrowBuf buffer;
        @Workspace org.joda.time.format.DateTimeFormatter formatter;
        @Output NullableVarCharHolder out;

        @Override
        public void setup() {
          formatter = JodaDateUtility.formatTimeStampTZ.withZoneUTC();
        }

        @Override
        public void eval() {
            out.isSet = 1;
            String str = formatter.print(org.joda.time.DateTimeUtils.currentTimeMillis());
            out.buffer = buffer;
            out.start = 0;
            out.end = Math.min(100,  str.length()); // truncate if target type has length smaller than that of input's string
            out.buffer.setBytes(0, str.substring(0,out.end).getBytes());
        }
    }

    @FunctionTemplate(names = {"localtimestamp", "current_timestamp"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, syntax = FunctionSyntax.FUNCTION_ID)
    public static class LocalTimeStamp implements SimpleFunction {
        @Workspace long queryStartDate;
        @Output TimeStampMilliHolder out;
        @Inject ContextInformation contextInfo;

        @Override
        public void setup() {
            queryStartDate = contextInfo.getQueryStartTime();
        }

        @Override
        public void eval() {
            out.value = queryStartDate;
        }
    }

    @FunctionTemplate(names = {"now", "statement_timestamp", "transaction_timestamp"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class NowTimeStamp implements SimpleFunction {
        @Workspace long queryStartDate;
        @Output TimeStampMilliHolder out;
        @Inject ContextInformation contextInfo;

        @Override
        public void setup() {
            queryStartDate = contextInfo.getQueryStartTime();
        }

        @Override
        public void eval() {
            out.value = queryStartDate;
        }
    }

    @FunctionTemplate(names = {"current_timestamp_utc"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, syntax = FunctionSyntax.FUNCTION_ID)
    public static class CurrentTimeStampUTC implements SimpleFunction {
        @Workspace long queryStartDate;
        @Output TimeStampMilliHolder out;
        @Inject ContextInformation contextInfo;

        @Override
        public void setup() {
            queryStartDate = contextInfo.getQueryStartTime();
        }

        @Override
        public void eval() {
            out.value = queryStartDate;
        }
    }

    @FunctionTemplate(names = {"current_time", "localtime"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, syntax = FunctionSyntax.FUNCTION_ID)
    public static class CurrentTime implements SimpleFunction {
        @Workspace int queryStartTime;
        @Output TimeMilliHolder out;
        @Inject ContextInformation contextInfo;

        @Override
        public void setup() {

            int timeZoneIndex = contextInfo.getRootFragmentTimeZone();
            org.joda.time.DateTimeZone timeZone = org.joda.time.DateTimeZone.forID(com.dremio.common.util.JodaDateUtility.getTimeZone(timeZoneIndex));
            org.joda.time.LocalDateTime now = new org.joda.time.LocalDateTime(contextInfo.getQueryStartTime(), timeZone);
            queryStartTime= (now.getHourOfDay() * org.apache.arrow.vector.util.DateUtility.hoursToMillis) +
                                   (now.getMinuteOfHour() * org.apache.arrow.vector.util.DateUtility.minutesToMillis) +
                                   (now.getSecondOfMinute() * org.apache.arrow.vector.util.DateUtility.secondsToMillis) +
                                   (now.getMillisOfSecond());
        }

        @Override
        public void eval() {
            out.value = queryStartTime;
        }
    }

    @FunctionTemplate(names = {"current_time_utc"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, syntax = FunctionSyntax.FUNCTION_ID)
    public static class CurrentTimeUTC implements SimpleFunction {
        @Workspace int queryStartTime;
        @Output TimeMilliHolder out;
        @Inject ContextInformation contextInfo;

        @Override
        public void setup() {

            org.joda.time.LocalDateTime now = new org.joda.time.LocalDateTime(contextInfo.getQueryStartTime(), org.joda.time.DateTimeZone.UTC);
            queryStartTime= (now.getHourOfDay() * org.apache.arrow.vector.util.DateUtility.hoursToMillis) +
                                   (now.getMinuteOfHour() * org.apache.arrow.vector.util.DateUtility.minutesToMillis) +
                                   (now.getSecondOfMinute() * org.apache.arrow.vector.util.DateUtility.secondsToMillis) +
                                   (now.getMillisOfSecond());
        }

        @Override
        public void eval() {
            out.value = queryStartTime;
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class DateTimeAddFunction implements SimpleFunction {
    @Param DateMilliHolder left;
    @Param TimeMilliHolder right;
    @Output TimeStampMilliHolder out;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {
            out.value = left.value + right.value;
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class TimeDateAddFunction implements SimpleFunction {
        @Param TimeMilliHolder right;
        @Param DateMilliHolder left;
        @Output TimeStampMilliHolder out;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {
            out.value = left.value + right.value;
        }
    }

    @FunctionTemplate(name = "castTIME", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class CastTimeStampToTime implements SimpleFunction {
        @Param TimeStampMilliHolder in;
        @Output TimeMilliHolder out;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {
            out.value = (int) (in.value % org.apache.arrow.vector.util.DateUtility.daysToStandardMillis);
        }
    }

    @FunctionTemplate(name = "unix_timestamp", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class UnixTimeStamp implements SimpleFunction {
      @Output BigIntHolder out;
      @Workspace long queryStartDate;
      @Inject ContextInformation contextInfo;

      @Override
      public void setup() {
         queryStartDate = contextInfo.getQueryStartTime();
      }

      @Override
      public void eval() {
        out.value = queryStartDate / 1000;
      }
    }

    @FunctionTemplate(name = "unix_timestamp", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class UnixTimeStampForDate implements SimpleFunction {
      @Param VarCharHolder inputDateValue;
      @Output BigIntHolder out;
      @Workspace org.joda.time.LocalDateTime date;
      @Workspace org.joda.time.format.DateTimeFormatter formatter;

      @Override
      public void setup() {
         formatter = com.dremio.exec.expr.fn.impl.DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD HH24:MI:SS");
      }

      @Override
      public void eval() {
        String inputDate = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputDateValue.start, inputDateValue.end, inputDateValue.buffer);
        date = formatter.parseLocalDateTime(inputDate);
        out.value = com.dremio.common.util.DateTimes.toMillis(date) / 1000;
      }
    }

    @FunctionTemplate(name = "unix_timestamp", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class UnixTimeStampForDateWithPattern implements SimpleFunction {
      @Param VarCharHolder inputDateValue;
      @Param VarCharHolder inputPattern;
      @Output BigIntHolder out;
      @Workspace org.joda.time.LocalDateTime date;
      @Workspace org.joda.time.format.DateTimeFormatter formatter;

      @Override
      public void setup() {
         String pattern = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputPattern.start, inputPattern.end, inputPattern.buffer);
         formatter = com.dremio.exec.expr.fn.impl.DateFunctionsUtils.getSQLFormatterForFormatString(pattern);
      }

      @Override
      public void eval() {
          String inputDate = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputDateValue.start, inputDateValue.end, inputDateValue.buffer);
          date = formatter.parseLocalDateTime(inputDate);
          out.value = com.dremio.common.util.DateTimes.toMillis(date) / 1000;
      }
    }
}
