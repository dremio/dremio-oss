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

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.common.util.DremioGetObject;
import com.dremio.common.util.DremioStringUtils;
import org.apache.arrow.vector.IntervalDayVector;

public class IntervalDayAccessor extends AbstractSqlAccessor {

  private static final MajorType TYPE = Types.optional(MinorType.INTERVALDAY);

  private final IntervalDayVector ac;

  public IntervalDayAccessor(IntervalDayVector vector) {
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
    return String.class;
  }

  @Override
  public Object getObject(int index) {
    if (ac.isNull(index)) {
      return null;
    }
    return DremioStringUtils.formatIntervalDay(DremioGetObject.getPeriodObject(ac, index));
  }

  @Override
  public String getString(int index) {
    if (ac.isNull(index)) {
      return null;
    }
    return String.valueOf(ac.getAsStringBuilder(index));
  }
}
