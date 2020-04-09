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

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.Types.MinorType;

public class SqlAccessorBuilder {

  private SqlAccessorBuilder() {}

  public static SqlAccessor getSqlAccessor(ValueVector vector){
    final MinorType type = org.apache.arrow.vector.types.Types.getMinorTypeForArrowType(vector.getField().getType());
    switch(type){
    case UNION:
      return new UnionSqlAccessor((UnionVector) vector);
    case TINYINT:
      return new TinyIntAccessor((TinyIntVector) vector);
    case UINT1:
      return new UInt1Accessor((UInt1Vector) vector);
    case UINT2:
      return new UInt2Accessor((UInt2Vector) vector);
    case SMALLINT:
      return new SmallIntAccessor((SmallIntVector) vector);
    case INT:
      return new IntAccessor((IntVector) vector);
    case UINT4:
      return new UInt4Accessor((UInt4Vector) vector);
    case FLOAT4:
      return new Float4Accessor((Float4Vector) vector);
    case INTERVALYEAR:
      return new IntervalYearAccessor((IntervalYearVector) vector);
    case TIMEMILLI:
      return new TimeMilliAccessor((TimeMilliVector) vector);
    case BIGINT:
      return new BigIntAccessor((BigIntVector) vector);
    case UINT8:
      return new UInt8Accessor((UInt8Vector) vector);
    case FLOAT8:
      return new Float8Accessor((Float8Vector) vector);
    case DATEMILLI:
      return new DateMilliAccessor((DateMilliVector) vector);
    case TIMESTAMPMILLI:
      return new TimeStampMilliAccessor((TimeStampMilliVector) vector);
    case INTERVALDAY:
      return new IntervalDayAccessor((IntervalDayVector) vector);
    case DECIMAL:
      return new DecimalAccessor((DecimalVector) vector);
    case FIXEDSIZEBINARY:
      return new FixedSizeBinaryAccessor((FixedSizeBinaryVector) vector);
    case VARBINARY:
      return new VarBinaryAccessor((VarBinaryVector) vector);
    case VARCHAR:
      return new VarCharAccessor((VarCharVector) vector);
    case BIT:
      return new BitAccessor((BitVector) vector);
    case STRUCT:
    case LIST:
      return new GenericAccessor(vector);
    }
    throw new UnsupportedOperationException(String.format("Unable to find sql accessor for minor type [%s]", type));
  }

}
