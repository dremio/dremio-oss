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
package com.dremio.exec.vector.complex;

import static com.dremio.common.util.MajorTypeHelper.getMinorTypeFromArrowMinorType;

import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.fn.impl.MappifyUtility;

public class MapUtility {
  private final static String TYPE_MISMATCH_ERROR = "Mappify/kvgen does not support heterogeneous value types. All values in the input map must be of the same type. The field [%s] has a differing type [%s].";

  /*
   * Function to read a value from the field reader, detect the type, construct the appropriate value holder
   * and use the value holder to write to the Map.
   */
  // TODO : This should be templatized and generated using freemarker
  public static void writeToMapFromReader(FieldReader fieldReader, BaseWriter.StructWriter structWriter) {
    try {
      MinorType valueMinorType = getMinorTypeFromArrowMinorType(fieldReader.getMinorType());

      switch (valueMinorType) {
        case TINYINT:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.tinyInt(MappifyUtility.fieldValue));
          break;
        case SMALLINT:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.smallInt(MappifyUtility.fieldValue));
          break;
        case BIGINT:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.bigInt(MappifyUtility.fieldValue));
          break;
        case INT:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.integer(MappifyUtility.fieldValue));
          break;
        case UINT1:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.uInt1(MappifyUtility.fieldValue));
          break;
        case UINT2:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.uInt2(MappifyUtility.fieldValue));
          break;
        case UINT4:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.uInt4(MappifyUtility.fieldValue));
          break;
        case UINT8:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.uInt8(MappifyUtility.fieldValue));
          break;
        case DATE:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.dateMilli(MappifyUtility.fieldValue));
          break;
        case TIME:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.timeMilli(MappifyUtility.fieldValue));
          break;
        case TIMESTAMP:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.timeStampMilli(MappifyUtility.fieldValue));
          break;
        case INTERVALDAY:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.intervalDay(MappifyUtility.fieldValue));
          break;
        case INTERVALYEAR:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.intervalYear(MappifyUtility.fieldValue));
          break;
        case FLOAT4:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.float4(MappifyUtility.fieldValue));
          break;
        case FLOAT8:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.float8(MappifyUtility.fieldValue));
          break;
        case BIT:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.bit(MappifyUtility.fieldValue));
          break;
        case VARCHAR:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.varChar(MappifyUtility.fieldValue));
          break;
        case VARBINARY:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.varBinary(MappifyUtility.fieldValue));
          break;
        case STRUCT:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.struct(MappifyUtility.fieldValue));
          break;
        case LIST:
          ComplexCopier.copy(fieldReader, (FieldWriter) structWriter.list(MappifyUtility.fieldValue));
          break;
        default:
          throw new IllegalArgumentException(String.format("kvgen does not support input of type: %s", valueMinorType));
      }
    } catch (ClassCastException e) {
      final Field field = fieldReader.getField();
      throw new RuntimeException(String.format(TYPE_MISMATCH_ERROR, field.getName(), field.getType()), e);
    }
  }
}
