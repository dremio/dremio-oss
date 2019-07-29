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

package com.dremio.common.util;

import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalYearHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.types.Types;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;

/* TODO: this is temporary setup to skip GetObject calls on Arrow vectors.
        This should be removed once Dremio switches to java8 completely. */
public class DremioGetObject {
  public static Period getPeriodObject(IntervalDayVector vector, int index) {
    return  (Period)getObject(vector, index);
  }
  public static Period getPeriodObject(IntervalYearVector vector, int index) {
    return  (Period)getObject(vector, index);
  }
  public static Object getObject(ValueVector vector, int index) {
    Types.MinorType minorType = vector.getMinorType();
    switch (minorType) {
      case DATEMILLI: {
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        ((DateMilliVector)vector).get(index, holder);
        if (holder.isSet == 0) {
          return null;
        }
        else {
          long millis = holder.value;
          return new LocalDateTime(millis,
            DateTimeZone.UTC);
        }
      }
      case TIMEMILLI: {
        NullableTimeMilliHolder holder = new NullableTimeMilliHolder();
        ((TimeMilliVector)vector).get(index, holder);
        if (holder.isSet == 0) {
          return null;
        } else {
          int millis = holder.value;
          return new LocalDateTime(millis,
            DateTimeZone.UTC);
        }
      }
      case TIMESTAMPMILLI: {
        NullableTimeStampMilliHolder holder = new NullableTimeStampMilliHolder();
        ((TimeStampMilliVector)vector).get(index, holder);
        if (holder.isSet == 0) {
          return null;
        } else {
          final long millis = holder.value;
          return new LocalDateTime(millis,
            DateTimeZone.UTC);
        }
      }
      case INTERVALDAY: {
        NullableIntervalDayHolder holder = new NullableIntervalDayHolder();
        ((IntervalDayVector)vector).get(index, holder);
        if (holder.isSet == 0) {
          return null;
        } else {
          final int days = holder.days;
          final int milliseconds = holder.milliseconds;
          final Period p = new Period();
          return p.plusDays(days).plusMillis(milliseconds);
        }
      }
      case INTERVALYEAR: {
        NullableIntervalYearHolder holder = new NullableIntervalYearHolder();
        ((IntervalYearVector)vector).get(index, holder);
        if (holder.isSet == 0) {
          return null;
        } else {
          final int interval = holder.value;
          final int years = (interval / JodaDateUtility.yearsToMonths);
          final int months = (interval % JodaDateUtility.yearsToMonths);
          final Period p = new Period();
          return p.plusYears(years).plusMonths(months);
        }
      }
      default:
        return vector.getObject(index);
    }
  }
}
