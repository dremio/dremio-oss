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
package com.dremio.exec.expr.fn.impl;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.dremio.exec.vector.complex.fn.WorkingBuffer;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class MapFunctions {

  public static final String LAST_MATCHING_ENTRY_FUNC = "last_matching_map_entry_for_key";

  @FunctionTemplate(
      names = {"map_keys"},
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.NULL_IF_NULL,
      derivation = ListOfKeys.class)
  public static class GetMapKeys implements SimpleFunction {
    @Param FieldReader input;

    @Output BaseWriter.ComplexWriter out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      org.apache.arrow.vector.complex.impl.UnionMapReader mapReader =
          (org.apache.arrow.vector.complex.impl.UnionMapReader) input;
      org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
      listWriter.startList();
      while (mapReader.next()) {
        org.apache.arrow.vector.complex.impl.ComplexCopier.copy(
            mapReader.key(), (org.apache.arrow.vector.complex.writer.FieldWriter) listWriter);
      }
      listWriter.endList();
    }
  }

  @FunctionTemplate(
      names = {"map_values"},
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.NULL_IF_NULL,
      derivation = ListOfValues.class)
  public static class GetMapValues implements SimpleFunction {
    @Param FieldReader input;

    @Output BaseWriter.ComplexWriter out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      org.apache.arrow.vector.complex.impl.UnionMapReader mapReader =
          (org.apache.arrow.vector.complex.impl.UnionMapReader) input;
      org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
      listWriter.startList();
      while (mapReader.next()) {
        org.apache.arrow.vector.complex.impl.ComplexCopier.copy(
            mapReader.value(), (org.apache.arrow.vector.complex.writer.FieldWriter) listWriter);
      }
      listWriter.endList();
    }
  }

  @FunctionTemplate(
      names = {"size"},
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL)
  public static class GetMapSize implements SimpleFunction {
    @Param FieldReader input;

    @Output NullableIntHolder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      if (input.isSet()) {
        out.value = input.size();
        out.isSet = 1;
        return;
      }
      out.value = -1;
      out.isSet = 1;
    }
  }

  public static class KeyValueOutputLastMatching implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Field entryStruct = getEntryStruct(args, "GetLastMatchingMapEntryForKey");
      return CompleteType.fromField(entryStruct);
    }
  }

  @FunctionTemplate(
      name = "dremio_internal_buildmap",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL,
      derivation = MapElements.class)
  public static class MapBuilder implements SimpleFunction {
    @Param FieldReader keysReader;
    @Param FieldReader valuesReader;
    @Output BaseWriter.ComplexWriter out;
    @Inject FunctionErrorContext errCtx;
    @Inject ArrowBuf buffer;
    @Workspace WorkingBuffer wb;

    @Override
    public void setup() {
      wb = new WorkingBuffer(buffer);
    }

    @Override
    public void eval() {
      if (keysReader.getMinorType()
          != com.dremio.common.util.MajorTypeHelper.getArrowMinorType(
              com.dremio.common.types.TypeProtos.MinorType.LIST)) {
        throw errCtx
            .error()
            .message("The buildmap function can only be used when operating against arrays.")
            .build();
      }
      org.apache.arrow.vector.complex.impl.UnionMapWriter mapWriter =
          (org.apache.arrow.vector.complex.impl.UnionMapWriter) out.rootAsMap(false);
      mapWriter.startMap();
      while (keysReader.next()) {
        valuesReader.next();
        mapWriter.startEntry();
        com.dremio.exec.expr.fn.impl.MapFunctions.copyPrimitive(
            keysReader.reader(), mapWriter.key());
        com.dremio.exec.expr.fn.impl.MapFunctions.copyPrimitive(
            valuesReader.reader(), mapWriter.value());
        mapWriter.endEntry();
      }
      mapWriter.endMap();
    }
  }

  public static void copyPrimitive(FieldReader reader, MapWriter writer) {
    Types.MinorType mt = reader.getMinorType();
    FieldWriter fieldWriter;
    switch (mt) {
      case BIT:
        fieldWriter = (FieldWriter) writer.bit();
        break;
      case INT:
        fieldWriter = (FieldWriter) writer.integer();
        break;
      case BIGINT:
        fieldWriter = (FieldWriter) writer.bigInt();
        break;
      case FLOAT4:
        fieldWriter = (FieldWriter) writer.float4();
        break;
      case FLOAT8:
        fieldWriter = (FieldWriter) writer.float8();
        break;
      case VARCHAR:
        fieldWriter = (FieldWriter) writer.varChar();
        break;
      case VARBINARY:
        fieldWriter = (FieldWriter) writer.varBinary();
        break;
      case DECIMAL:
        fieldWriter = (FieldWriter) writer.decimal();
        break;
      default:
        throw new IllegalArgumentException(String.format("Unsupported type %s", mt));
    }

    ComplexCopier.copy(reader, fieldWriter);
  }

  public static class ListOfKeys implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Field entryStruct = getEntryStruct(args, "getMapKeys");
      return CompleteType.fromField(entryStruct.getChildren().get(0)).asList();
    }
  }

  public static class ListOfValues implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Field entryStruct = getEntryStruct(args, "getMapValues");
      return CompleteType.fromField(entryStruct.getChildren().get(1)).asList();
    }
  }

  private static Field getEntryStruct(List<LogicalExpression> args, String functionName) {
    CompleteType mapType = args.get(0).getCompleteType();
    if (!mapType.isMap()) {
      throw UserException.functionError()
          .message(
              "The %s function can only be used when operating against maps. The type you were attempting to apply it to was a %s.",
              functionName, mapType.toString())
          .build();
    }
    if (mapType.getChildren().size() != 1) {
      throw new IllegalArgumentException(
          String.format("Unexpected map structure %s", mapType.toString()));
    }
    Field entryStruct = mapType.getChildren().get(0);
    if (entryStruct.getChildren().size() != 2) {
      throw new IllegalArgumentException(
          String.format("Unexpected entry in map structure %s", entryStruct.toString()));
    }
    return entryStruct;
  }

  public static class MapElements implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() >= 2 && args.size() % 2 == 0);
      CompleteType keyType = args.get(0).getCompleteType().getOnlyChildType();
      CompleteType valueType = args.get(1).getCompleteType().getOnlyChildType();
      List<Field> children =
          Arrays.asList(
              Field.notNullable("key", keyType.getType()),
              Field.nullable("value", valueType.getType()));
      return new CompleteType(
          CompleteType.MAP.getType(),
          CompleteType.struct(children).toField(MapVector.DATA_VECTOR_NAME, false));
    }
  }
}
