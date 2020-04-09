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

import java.sql.Timestamp;

import org.apache.arrow.vector.TimeStampMilliVector;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;

public class TimeStampMilliAccessor extends AbstractSqlAccessor {

  private static final MajorType TYPE = Types.optional(MinorType.TIMESTAMP);

  private final TimeStampMilliVector ac;

  public TimeStampMilliAccessor(TimeStampMilliVector vector) {
    this.ac = vector;
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
    if (ac.isNull(index)) {
      return null;
    }
    return getTimestamp(index);
  }

  @Override
  public Timestamp getTimestamp(int index) {
    if (ac.isNull(index)) {
      return null;
    }
    org.joda.time.LocalDateTime date = new org.joda.time.LocalDateTime(ac.get(index), org.joda.time.DateTimeZone.UTC);
    return new Timestamp(date.getYear() - 1900, date.getMonthOfYear() - 1, date.getDayOfMonth(), date.getHourOfDay(),
        date.getMinuteOfHour(), date.getSecondOfMinute(),
        (int) java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(date.getMillisOfSecond()));
  }

}
