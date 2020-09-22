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

import static com.dremio.common.util.MajorTypeHelper.getMinorTypeFromArrowMinorType;

import java.util.List;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.UnionHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.dremio.exec.resolver.TypeCastRules;
import com.google.common.base.Preconditions;

/**
 * The class contains additional functions for union types in addition to those in GUnionFunctions
 */
public class UnionFunctions {

  /**
   * Returns zero if the inputs have equivalent types. Two numeric types are considered equivalent, as are a combination
   * of date/timestamp. If not equivalent, returns a value determined by the numeric value of the MinorType enum
   */
  @FunctionTemplate(names = {"compareType"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.INTERNAL)
  public static class CompareType implements SimpleFunction {

    @Param FieldReader input1;
    @Param FieldReader input2;
    @Output NullableIntHolder out;

    public void setup() {}

    public void eval() {
      org.apache.arrow.vector.types.Types.MinorType type1;
      if (input1.isSet()) {
        type1 = input1.getMinorType();
      } else {
        type1 = org.apache.arrow.vector.types.Types.MinorType.NULL;
      }
      org.apache.arrow.vector.types.Types.MinorType type2;
      if (input2.isSet()) {
        type2 = input2.getMinorType();
      } else {
        type2 = org.apache.arrow.vector.types.Types.MinorType.NULL;
      }

      out.isSet = 1;
      out.value = com.dremio.exec.expr.fn.impl.UnionFunctions.compareTypes(type1, type2);
    }
  }

  public static int compareTypes(MinorType type1, MinorType type2) {
    int typeValue1 = getTypeValue(type1);
    int typeValue2 = getTypeValue(type2);
    return typeValue1 - typeValue2;
  }

  /**
   * Gives a type ordering modeled after the behavior of MongoDB
   * Numeric types are first, folowed by string types, followed by binary, then boolean, then date, then timestamp
   * Any other times will be sorted after that
   * @param type
   * @return
   */
  private static int getTypeValue(MinorType type) {
    if (TypeCastRules.isNumericType(getMinorTypeFromArrowMinorType(type))) {
      return 0;
    }
    switch (type) {
    case TINYINT:
    case SMALLINT:
    case INT:
    case BIGINT:
    case UINT1:
    case UINT2:
    case UINT4:
    case UINT8:
    case DECIMAL:
    case FLOAT4:
    case FLOAT8:
      return 0;
    case VARCHAR:
      return 1;
    case VARBINARY:
      return 2;
    case BIT:
      return 3;
    case DATEMILLI:
      return 4;
    case TIMESTAMPMILLI:
      return 5;
    default:
      return 6 + type.ordinal();
    }
  }

  @FunctionTemplate(names = {"typeOf"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.INTERNAL)
  public static class GetType implements SimpleFunction {

    @Param
    FieldReader input;
    @Output
    NullableVarCharHolder out;
    @Inject
    ArrowBuf buf;

    public void setup() {}

    public void eval() {
      out.isSet = 1;

      byte[] type;
      if (input.isSet()) {
         type = input.getMinorType().name().getBytes();
      } else {
        type = org.apache.arrow.vector.types.Types.MinorType.NULL.name().getBytes();
      }
      buf = buf.reallocIfNeeded(type.length);
      buf.setBytes(0, type);
      out.buffer = buf;
      out.start = 0;
      out.end = type.length;
    }
  }

  @FunctionTemplate(names = {"castUNION", "castToUnion"})
  public static class CastUnionToUnion implements SimpleFunction{

    @Param FieldReader in;
    @Output
    UnionHolder out;

    public void setup() {}

    public void eval() {
      out.reader = in;
      out.isSet = in.isSet() ? 1 : 0;
    }
  }

  @FunctionTemplate(name = "ASSERT_LIST", derivation =  AssertListOutputDerivation.class)
  public static class CastUnionList implements SimpleFunction {

    @Param UnionHolder in;
    @Output ComplexWriter out;
    @Inject FunctionErrorContext errorContext;

    public void setup() {}

    public void eval() {
      if (in.isSet == 1) {
        if (in.reader.getMinorType() != org.apache.arrow.vector.types.Types.MinorType.LIST) {
          throw errorContext.error()
              .message("The input is not a LIST type")
              .build();
        }

        org.apache.arrow.vector.complex.impl.ComplexCopier.copy(in.reader,
            (org.apache.arrow.vector.complex.writer.FieldWriter) out.rootAsList());
      }
    }
  }

  public static class AssertListOutputDerivation implements OutputDerivation {

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() == 1, "ASSERT_LIST requires only one argument");
      final LogicalExpression expression = args.get(0);

      CompleteType listType = null;
      if (expression.getCompleteType().isList()) {
        listType = expression.getCompleteType();
      } else if (expression.getCompleteType().isUnion()) {
        for (Field field : expression.getCompleteType().getChildren()) {
          final CompleteType fieldType = CompleteType.fromField(field);
          if (fieldType.isList()) {
            listType = fieldType;
            break; // there will be exactly one
          }
        }
      }

      if (listType == null) {
        throw UserException.validationError()
            .message("The field must be a list of values or a mixed type that contains a list of values")
            .addContext("field type", expression.getCompleteType().toString())
            .build();
      }
      return listType;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "IS_LIST", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
  public static class UnionIsList implements SimpleFunction {

    @Param UnionHolder in;
    @Output NullableBitHolder out;

    public void setup() {}

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 1) {
        out.value = in.getMinorType() == org.apache.arrow.vector.types.Types.MinorType.LIST ? 1 : 0;
      } else {
        out.value = 0;
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = "ASSERT_STRUCT", scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL, derivation =  AssertStructOutputDerivation.class)
  public static class CastUnionStruct implements SimpleFunction {

    @Param UnionHolder in;
    @Output ComplexWriter out;
    @Inject FunctionErrorContext errorContext;

    public void setup() {}

    public void eval() {
      if (in.isSet == 1) {
        if (in.reader.getMinorType() != org.apache.arrow.vector.types.Types.MinorType.STRUCT) {
          throw errorContext.error()
              .message("The input is not a STRUCT type")
              .build();
        }

        org.apache.arrow.vector.complex.impl.ComplexCopier.copy(in.reader,
            (org.apache.arrow.vector.complex.writer.FieldWriter) out.rootAsStruct());
      }
    }
  }

  public static class AssertStructOutputDerivation implements OutputDerivation {

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() == 1, "ASSERT_STRUCT requires only one argument");
      final LogicalExpression expression = args.get(0);

      CompleteType structType = null;
      if (expression.getCompleteType().isStruct()) {
        structType = expression.getCompleteType();
      } else if (expression.getCompleteType().isUnion()) {
        for (Field field : expression.getCompleteType().getChildren()) {
          final CompleteType fieldType = CompleteType.fromField(field);
          if (fieldType.isStruct()) {
            structType = fieldType;
            break; // there will be exactly one
          }
        }
      }

      if (structType == null) {
        throw UserException.validationError()
            .message("The field must be of struct type or a mixed type that contains a struct type")
            .addContext("field type", expression.getCompleteType().toString())
            .build();
      }
      return structType;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = "IS_STRUCT", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
  public static class UnionIsStruct implements SimpleFunction {

    @Param UnionHolder in;
    @Output NullableBitHolder out;

    public void setup() {}

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 1) {
        out.value = in.getMinorType() == org.apache.arrow.vector.types.Types.MinorType.STRUCT ? 1 : 0;
      } else {
        out.value = 0;
      }
    }
  }

  @FunctionTemplate(names = {"isnotnull", "is not null"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class IsNotNull implements SimpleFunction {

    @Param UnionHolder input;
    @Output NullableBitHolder out;

    public void setup() { }

    public void eval() {
      out.isSet = 1;
      out.value = input.isSet == 1 ? 1 : 0;
    }
  }

  @FunctionTemplate(names = {"isnull", "is null"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class IsNull implements SimpleFunction {

    @Param UnionHolder input;
    @Output NullableBitHolder out;

    public void setup() { }

    public void eval() {
      out.isSet = 1;
      out.value = input.isSet == 1 ? 0 : 1;
    }
  }

}
