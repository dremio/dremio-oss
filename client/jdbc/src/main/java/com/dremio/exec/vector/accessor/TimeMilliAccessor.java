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

import org.apache.arrow.vector.TimeMilliVector;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.vector.accessor.sql.TimePrintMillis;

public class TimeMilliAccessor extends AbstractSqlAccessor {

  private static final MajorType TYPE = Types.optional(MinorType.TIME);

  private final TimeMilliVector ac;

  public TimeMilliAccessor(TimeMilliVector vector) {
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
    return Time.class;
  }

  @Override
  public Object getObject(int index) {
    return getTime(index);
  }

  @Override
  public Time getTime(int index) {
    if (ac.isNull(index)) {
      return null;
    }
    org.joda.time.LocalDateTime time = new org.joda.time.LocalDateTime(ac.get(index), org.joda.time.DateTimeZone.UTC);
    return new TimePrintMillis(time);
  }

}
