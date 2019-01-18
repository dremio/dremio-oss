/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.record;

import static com.dremio.common.util.MajorTypeHelper.getMajorTypeForField;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListVectorHelper;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.StructVectorHelper;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.UnionVectorHelper;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.TypeHelper;

public class ComplexTypeHelper extends TypeHelper {
  public static void materialize(ValueVector vector, Field field) {
    MinorType minorType = getMajorTypeForField(field).getMinorType();
    switch(minorType) {
    case STRUCT:
      new StructVectorHelper((StructVector) vector).materialize(field);
      return;
    case LIST:
      new ListVectorHelper((ListVector) vector).materialize(field);
      return;
    case UNION:
      new UnionVectorHelper((UnionVector) vector).materialize(field);
      return;
    default:
      // no op
    }
  }
}
