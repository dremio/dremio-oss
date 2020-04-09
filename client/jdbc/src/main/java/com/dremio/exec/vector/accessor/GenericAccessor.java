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

import static com.dremio.common.util.MajorTypeHelper.getMajorTypeForField;

import org.apache.arrow.vector.ValueVector;

import com.dremio.common.types.TypeProtos.MajorType;

public class GenericAccessor extends AbstractSqlAccessor {

  private ValueVector v;

  public GenericAccessor(ValueVector v) {
    this.v = v;
  }

  @Override
  public Class<?> getObjectClass() {
    return Object.class;
  }

  @Override
  public boolean isNull(int index) {
    return v.isNull(index);
  }

  @Override
  public Object getObject(int index) throws InvalidAccessException {
    return v.getObject(index);
  }

  @Override
  public MajorType getType() {
    return getMajorTypeForField(v.getField());
  }
}
