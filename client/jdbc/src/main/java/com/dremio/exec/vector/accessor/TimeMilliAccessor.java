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
package com.dremio.exec.vector.accessor;

import java.sql.Time;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.TimeMilliVector;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.vector.accessor.sql.TimePrintMillis;
import com.google.common.base.Preconditions;

public class TimeMilliAccessor extends AbstractSqlAccessor {

  private static final MajorType TYPE = Types.optional(MinorType.TIME);
  private static final long DAY_MS = TimeUnit.DAYS.toMillis(1);

  private final TimeZone defaultTimeZone;
  private final TimeMilliVector ac;

  public TimeMilliAccessor(TimeMilliVector vector, TimeZone defaultTZ) {
    this.ac = vector;
    this.defaultTimeZone = Preconditions.checkNotNull(defaultTZ, "Null TimeZone supplied.");
  }

  @Override
  public MajorType getType() {
    return TYPE;
  }

  @Override
  public boolean isNull(int index) {
    return ac.isNull(index);
  }

  @Override
  public Class<?> getObjectClass() {
    return Time.class;
  }

  @Override
  public Object getObject(int index) {
    return getTime(index, defaultTimeZone);
  }

  @Override
  public Time getTime(int index, Calendar calendar) {
    Preconditions.checkNotNull(calendar, "Invalid calendar used when attempting to retrieve time.");
    return getTime(index, calendar.getTimeZone());
  }

  private Time getTime(int index, TimeZone tz) {
    if (ac.isNull(index)) {
      return null;
    }

    // Ensure that the timezones are offset correctly.
    long arrowMillis = ac.get(index);
    if (tz != defaultTimeZone) {
      arrowMillis -= tz.getOffset(arrowMillis) - defaultTimeZone.getOffset(arrowMillis);
    }

    // ofNanoOfDay only accepts positive values within a day, so adjust to ensure this range.
    if (arrowMillis > DAY_MS) {
      arrowMillis %= DAY_MS;
    } else if (arrowMillis < 0) {
      arrowMillis -= ((arrowMillis / DAY_MS) - 1) * DAY_MS;
    }

    return new TimePrintMillis(LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(arrowMillis)));
  }
}
