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

import org.apache.arrow.vector.TimeStampMilliVector;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.google.common.base.Preconditions;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.TimeZone;

public class TimeStampMilliAccessor extends AbstractSqlAccessor {

  private static final MajorType TYPE = Types.optional(MinorType.TIMESTAMP);

  private final TimeZone defaultTimeZone;
  private final TimeStampMilliVector ac;

  public TimeStampMilliAccessor(TimeStampMilliVector vector, TimeZone defaultTZ) {
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
    return Timestamp.class;
  }

  @Override
  public Object getObject(int index) {
    return getTimestamp(index, defaultTimeZone);
  }

  @Override
  public Timestamp getTimestamp(int index, Calendar calendar) {
    Preconditions.checkNotNull(calendar, "Invalid calendar used when attempting to retrieve timestamp.");
    return getTimestamp(index, calendar.getTimeZone());
  }

  private Timestamp getTimestamp(int index, TimeZone tz) {
    if (ac.isNull(index)) {
      return null;
    }

    LocalDateTime ldt = ac.getObject(index);
    if (tz != defaultTimeZone) {
      final long arrowMillis = ac.get(index);
      ldt = ldt.minus(tz.getOffset(arrowMillis) - defaultTimeZone.getOffset(arrowMillis), ChronoUnit.MILLIS);
    }

    return Timestamp.valueOf(ldt);
  }
}
