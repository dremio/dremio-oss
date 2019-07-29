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
package com.dremio.dac.explore.udfs;

import java.util.List;

import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.dac.proto.model.dataset.CardExamplePosition;
import com.dremio.exec.expr.fn.OutputDerivation;

/**
 * common utilities for UDFS in Dremio
 */
public class DremioUDFUtils {
  private static final String OFFSET_FIELD = "offset";
  private static final String LENGTH_FIELD = "length";


  public static void writeCardExample(ComplexWriter writer, CardExamplePosition... positions) {
    org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter.ListWriter list = writer.rootAsList();
    list.startList();
    for (CardExamplePosition position : positions) {
      org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter.StructWriter positionWriter = list.struct();
      positionWriter.start();
      positionWriter.integer(OFFSET_FIELD).writeInt(position.getOffset());
      positionWriter.integer(LENGTH_FIELD).writeInt(position.getLength());
      positionWriter.end();
    }
    list.endList();
  }

  /**
   * {@link OutputDerivation} for example position generation UDFs in Dremio
   */
  public static final class ExampleUDFOutputDerivation implements OutputDerivation {

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      return new CompleteType(
          ArrowType.List.INSTANCE,
          new CompleteType(
              ArrowType.Struct.INSTANCE,
              CompleteType.INT.toField(OFFSET_FIELD),
              CompleteType.INT.toField(LENGTH_FIELD)
          ).toField(ListVector.DATA_VECTOR_NAME)
      );
    }
  }
}
