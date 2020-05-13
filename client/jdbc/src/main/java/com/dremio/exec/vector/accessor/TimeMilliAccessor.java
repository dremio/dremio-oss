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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.arrow.vector.TimeMilliVector;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.vector.accessor.sql.TimePrintMillis;
import com.google.common.base.Preconditions;

public class TimeMilliAccessor extends AbstractSqlAccessor {

  private static final MajorType TYPE = Types.optional(MinorType.TIME);

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

    // The Arrow datetime values are already in UTC, so adjust to the timezone of the calendar passed in to
    // ensure the reported value is correct according to the JDBC spec.
    final LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(ac.get(index)), tz.toZoneId());
    return new TimePrintMillis(date);
  }
}
