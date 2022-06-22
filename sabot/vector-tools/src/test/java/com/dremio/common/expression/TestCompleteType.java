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
package com.dremio.common.expression;

import static com.dremio.common.expression.CompleteType.BIGINT;
import static com.dremio.common.expression.CompleteType.BIT;
import static com.dremio.common.expression.CompleteType.DATE;
import static com.dremio.common.expression.CompleteType.DECIMAL;
import static com.dremio.common.expression.CompleteType.DOUBLE;
import static com.dremio.common.expression.CompleteType.FLOAT;
import static com.dremio.common.expression.CompleteType.INT;
import static com.dremio.common.expression.CompleteType.LIST;
import static com.dremio.common.expression.CompleteType.NULL;
import static com.dremio.common.expression.CompleteType.STRUCT;
import static com.dremio.common.expression.CompleteType.TIME;
import static com.dremio.common.expression.CompleteType.TIMESTAMP;
import static com.dremio.common.expression.CompleteType.VARCHAR;
import static com.dremio.common.expression.CompleteType.struct;
import static com.dremio.common.expression.CompleteType.union;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collection;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.dremio.common.types.SupportsTypeCoercionsAndUpPromotions;
import com.dremio.exec.exception.NoSupportedUpPromotionOrCoercionException;

@RunWith(value = Enclosed.class)
public class TestCompleteType {
  @RunWith(value = Parameterized.class)
  public static class SupportedUpPromotionTests implements SupportsTypeCoercionsAndUpPromotions {
    private final CompleteType fileType;
    private final CompleteType tableType;
    private final CompleteType finalType;

    public SupportedUpPromotionTests(CompleteType fileType, CompleteType tableType, CompleteType finalType) {
      this.fileType = fileType;
      this.tableType = tableType;
      this.finalType = finalType;
    }

    @Parameters(name = "testSuccessfulUpPromotion_{1}To{0}")
    public static Collection<CompleteType[]> data() {
      return asList(new CompleteType[][]{
        {INT, INT, INT},
        {BIGINT, BIGINT, BIGINT},
        {FLOAT, FLOAT, FLOAT},
        {DOUBLE, DOUBLE, DOUBLE},
        {VARCHAR, VARCHAR, VARCHAR},
        {BIT, BIT, BIT},
        {DECIMAL, DECIMAL, DECIMAL},
        {BIGINT, INT, BIGINT},
        {FLOAT, INT, DOUBLE},
        {FLOAT, BIGINT, DOUBLE},
        {DOUBLE, INT, DOUBLE},
        {DOUBLE, BIGINT, DOUBLE},
        {DOUBLE, FLOAT, DOUBLE},
        {DOUBLE, DECIMAL, DOUBLE},
        {DECIMAL, INT, DECIMAL},
        {DECIMAL, BIGINT, DECIMAL},
        {DECIMAL, FLOAT, DECIMAL},
        {VARCHAR, BIT, VARCHAR},
        {VARCHAR, INT, VARCHAR},
        {VARCHAR, BIGINT, VARCHAR},
        {VARCHAR, FLOAT, VARCHAR},
        {VARCHAR, DOUBLE, VARCHAR},
        {VARCHAR, DECIMAL, VARCHAR},
        {VARCHAR, DATE, VARCHAR},
        {VARCHAR, TIME, VARCHAR},
        {VARCHAR, TIMESTAMP, VARCHAR},
        {INT, NULL, INT},
        {BIGINT, NULL, BIGINT},
        {FLOAT, NULL, FLOAT},
        {DOUBLE, NULL, DOUBLE},
        {VARCHAR, NULL, VARCHAR},
        {BIT, NULL, BIT},
        {DECIMAL, NULL, DECIMAL},
        {TIMESTAMP, NULL, TIMESTAMP},
        {TIME, NULL, TIME},
        {DATE, NULL, DATE},
        {NULL, NULL, NULL}
      });
    }

    @Test
    public void testSuccessfulUpPromotion() {
      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType()).isEqualTo(finalType.getType());
    }
  }

  public static class UnsupportedUpPromotionTests implements SupportsTypeCoercionsAndUpPromotions {
    @Test(expected = NoSupportedUpPromotionOrCoercionException.class)
    public void testUnsupportedUpPromotionForComplexTypes() {
      LIST.mergeFieldListsWithUpPromotionOrCoercion(STRUCT, this);
      LIST.mergeFieldListsWithUpPromotionOrCoercion(INT, this);
      STRUCT.mergeFieldListsWithUpPromotionOrCoercion(LIST, this);
      STRUCT.mergeFieldListsWithUpPromotionOrCoercion(INT, this);
    }
  }

  public static class SupportedUpPromotionTestsForComplexTypes implements SupportsTypeCoercionsAndUpPromotions {

    @Test
    public void testNullTypePromotionForComplexTypes() {
      NULL.mergeFieldListsWithUpPromotionOrCoercion(STRUCT, this);
      NULL.mergeFieldListsWithUpPromotionOrCoercion(LIST, this);
    }

    @Test
    public void testBooleanToVarcharUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", BIT.getType()));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", VARCHAR.getType()));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType()).isEqualTo(VARCHAR.getType());
    }

    @Test
    public void testIntToBigintUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(64, true)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.Int.class);
      assertThat(((ArrowType.Int) upPromotedType.getOnlyChild().getType()).getBitWidth()).isEqualTo(64);
    }

    @Test
    public void testIntToBigintUpPromotionInStruct() {
      CompleteType fileType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new Int(64, true)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(STRUCT.getType().getClass());
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.Int.class);
      assertThat(((ArrowType.Int) upPromotedType.getOnlyChild().getType()).getBitWidth()).isEqualTo(64);
    }

    @Test
    public void testIntToDoubleUpPromotionInListWithFloat() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", new FloatingPoint(SINGLE)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.FloatingPoint.class);
      assertThat(((ArrowType.FloatingPoint) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(FloatingPointPrecision.DOUBLE);
    }

    @Test
    public void testIntToDoubleUpPromotionInStructWithFloat() {
      CompleteType fileType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new FloatingPoint(SINGLE)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(STRUCT.getType().getClass());
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.FloatingPoint.class);
      assertThat(((ArrowType.FloatingPoint) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(FloatingPointPrecision.DOUBLE);
    }

    @Test
    public void testIntToDoubleUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", new FloatingPoint(FloatingPointPrecision.DOUBLE)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.FloatingPoint.class);
      assertThat(((ArrowType.FloatingPoint) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(FloatingPointPrecision.DOUBLE);
    }

    @Test
    public void testIntToDoubleUpPromotionInStruct() {
      CompleteType fileType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new FloatingPoint(FloatingPointPrecision.DOUBLE)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(STRUCT.getType().getClass());
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.FloatingPoint.class);
      assertThat(((ArrowType.FloatingPoint) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(FloatingPointPrecision.DOUBLE);
    }

    @Test
    public void testIntToDecimalUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", new ArrowType.Decimal(6, 2, 128)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.Decimal.class);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(6);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getScale()).isEqualTo(2);
    }

    @Test
    public void testIntToVarcharUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", INT.getType()));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", VARCHAR.getType()));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType()).isEqualTo(VARCHAR.getType());
    }

    @Test
    public void testBigIntToDoubleUpPromotionInListWithFloat() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", BIGINT.getType()));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", FLOAT.getType()));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.FloatingPoint.class);
      assertThat(((ArrowType.FloatingPoint) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(FloatingPointPrecision.DOUBLE);
    }

    @Test
    public void testBigIntToDoubleUpPromotionInStructWithFloat() {
      CompleteType fileType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new Int(64, true)));
      CompleteType tableType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new FloatingPoint(SINGLE)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(STRUCT.getType().getClass());
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.FloatingPoint.class);
      assertThat(((ArrowType.FloatingPoint) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(FloatingPointPrecision.DOUBLE);
    }

    @Test
    public void testBigIntToDoubleUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(64, true)));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", new FloatingPoint(FloatingPointPrecision.DOUBLE)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.FloatingPoint.class);
      assertThat(((ArrowType.FloatingPoint) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(FloatingPointPrecision.DOUBLE);
    }

    @Test
    public void testBigIntToDoubleUpPromotionInStruct() {
      CompleteType fileType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new Int(64, true)));
      CompleteType tableType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new FloatingPoint(FloatingPointPrecision.DOUBLE)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(STRUCT.getType().getClass());
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.FloatingPoint.class);
      assertThat(((ArrowType.FloatingPoint) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(FloatingPointPrecision.DOUBLE);
    }

    @Test
    public void testBigIntToDecimalUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(64, true)));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", new ArrowType.Decimal(6, 2, 128)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.Decimal.class);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(6);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getScale()).isEqualTo(2);
    }

    @Test
    public void testBigIntToVarcharUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", BIGINT.getType()));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", VARCHAR.getType()));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType()).isEqualTo(VARCHAR.getType());
    }

    @Test
    public void testFloatToDoubleUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", FLOAT.getType()));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", DOUBLE.getType()));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType()).isEqualTo(DOUBLE.getType());
      assertThat(upPromotedType.getOnlyChild().getType()).isNotEqualTo(FLOAT.getType());
    }

    @Test
    public void testFloatToDecimalUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", FLOAT.getType()));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", new ArrowType.Decimal(6, 2, 128)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.Decimal.class);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(6);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getScale()).isEqualTo(2);
    }

    @Test
    public void testFloatToVarcharUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", FLOAT.getType()));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", VARCHAR.getType()));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType()).isEqualTo(VARCHAR.getType());
    }

    @Test
    public void testDoubleToVarcharUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", DOUBLE.getType()));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", VARCHAR.getType()));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType()).isEqualTo(VARCHAR.getType());
    }

    @Test
    public void testDecimalToDoubleUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", new ArrowType.Decimal(6, 2, 128)));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", DOUBLE.getType()));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType()).isEqualTo(DOUBLE.getType());
    }

    @Test
    public void testDecimalToVarcharUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", DECIMAL.getType()));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", VARCHAR.getType()));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType()).isEqualTo(VARCHAR.getType());
    }

    @Test
    public void testDecimalToDecimalUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", new ArrowType.Decimal(3, 1, 128)));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", new ArrowType.Decimal(6, 2, 128)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.Decimal.class);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(6);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getScale()).isEqualTo(2);
    }

    @Test
    public void testDecimalToDecimalUpPromotionInListWithTruncation() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", new ArrowType.Decimal(38, 35, 128)));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", new ArrowType.Decimal(38, 1, 128)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.Decimal.class);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(38);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getScale()).isEqualTo(1);
    }

    @Test
    public void testDecimalToDecimalUpPromotionInStruct() {
      CompleteType fileType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new ArrowType.Decimal(3, 1, 128)));
      CompleteType tableType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new ArrowType.Decimal(6, 2, 128)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(STRUCT.getType().getClass());
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.Decimal.class);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(6);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getScale()).isEqualTo(2);
    }

    @Test
    public void testDecimalToDecimalUpPromotionInStructWithTruncation() {
      CompleteType fileType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new ArrowType.Decimal(38, 35, 128)));
      CompleteType tableType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new ArrowType.Decimal(38, 1, 128)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(STRUCT.getType().getClass());
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.Decimal.class);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getPrecision()).isEqualTo(38);
      assertThat(((ArrowType.Decimal) upPromotedType.getOnlyChild().getType()).getScale()).isEqualTo(1);
    }

    @Test
    public void testMultiplePromotionsInStruct() {
      CompleteType fileType = new CompleteType(ArrowType.Struct.INSTANCE, Field.nullable("col1", BIT.getType()), Field.nullable("col2", INT.getType()));
      CompleteType tableType = new CompleteType(ArrowType.Struct.INSTANCE, Field.nullable("col1", VARCHAR.getType()), Field.nullable("col2", DOUBLE.getType()));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(ArrowType.Struct.class);
      assertThat(upPromotedType.getChildren().get(0).getType()).isEqualTo(VARCHAR.getType());
      assertThat(upPromotedType.getChildren().get(1).getType()).isEqualTo(DOUBLE.getType());
    }
  }

  @RunWith(value = Parameterized.class)
  public static class SupportedCoercionTests implements SupportsTypeCoercionsAndUpPromotions {
    private final CompleteType fileType;
    private final CompleteType tableType;
    private final CompleteType finalType;

    public SupportedCoercionTests(CompleteType fileType, CompleteType tableType, CompleteType finalType) {
      this.fileType = fileType;
      this.tableType = tableType;
      this.finalType = finalType;
    }

    @Parameters(name = "testSuccessfulCoercion_{1}To{0}")
    public static Collection<CompleteType[]> data() {
      return asList(new CompleteType[][]{
        {BIT, VARCHAR, VARCHAR},
        {INT, BIGINT, BIGINT},
        {INT, FLOAT, DOUBLE},
        {INT, DOUBLE, DOUBLE},
        {INT, DECIMAL, DECIMAL},
        {INT, VARCHAR, VARCHAR},
        {BIGINT, FLOAT, DOUBLE},
        {BIGINT, DOUBLE, DOUBLE},
        {BIGINT, VARCHAR, VARCHAR},
        {BIGINT, DECIMAL, DECIMAL},
        {FLOAT, DOUBLE, DOUBLE},
        {FLOAT, DECIMAL, DECIMAL},
        {FLOAT, VARCHAR, VARCHAR},
        {DOUBLE, VARCHAR, VARCHAR},
        {DECIMAL, VARCHAR, VARCHAR},
        {DECIMAL, DOUBLE, DOUBLE},
        {new CompleteType(new ArrowType.Decimal(10, 10, 128)), DECIMAL, DECIMAL},
        {DATE, VARCHAR, VARCHAR},
        {TIME, VARCHAR, VARCHAR},
        {TIMESTAMP, VARCHAR, VARCHAR},
        {NULL, INT, INT},
        {NULL, BIGINT, BIGINT},
        {NULL, FLOAT, FLOAT},
        {NULL, DOUBLE, DOUBLE},
        {NULL, VARCHAR, VARCHAR},
        {NULL, BIT, BIT},
        {NULL, DECIMAL, DECIMAL},
        {NULL, TIMESTAMP, TIMESTAMP},
        {NULL, TIME, TIME},
        {NULL, DATE, DATE}
      });
    }

    @Test
    public void testSuccessfulUpPromotion() {
      CompleteType coercedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType, this);
      assertThat(coercedType.getType()).isEqualTo(finalType.getType());
      assertThat(coercedType.getType()).isNotEqualTo(fileType.getType());
    }
  }

  public static class SupportedCoercionTestsForComplexTypes implements SupportsTypeCoercionsAndUpPromotions {
    @Test
    public void testIntToBigintCoercionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(64, true)));

      CompleteType upPromotedType = fileType.mergeFieldListsWithUpPromotionOrCoercion(tableType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(List.class);
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.Int.class);
      assertThat(((ArrowType.Int) upPromotedType.getOnlyChild().getType()).getBitWidth()).isEqualTo(64);
    }

    @Test
    public void testIntToBigintCoercionInStruct() {
      CompleteType fileType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new Int(64, true)));

      CompleteType upPromotedType = fileType.mergeFieldListsWithUpPromotionOrCoercion(tableType, this);
      assertThat(upPromotedType.getType().getClass()).isEqualTo(STRUCT.getType().getClass());
      assertThat(upPromotedType.getOnlyChild().getType().getClass()).isEqualTo(ArrowType.Int.class);
      assertThat(((ArrowType.Int) upPromotedType.getOnlyChild().getType()).getBitWidth()).isEqualTo(64);
    }
  }

  @RunWith(value = Parameterized.class)
  public static class UnionRemovalTests implements SupportsTypeCoercionsAndUpPromotions {
    private final CompleteType unionType;
    private final CompleteType finalType;

    public UnionRemovalTests(CompleteType unionType, CompleteType finalType) {
      this.unionType = unionType;
      this.finalType = finalType;
    }

    @Parameters(name = "{0}")
    public static Collection<CompleteType[]> data() {
      String name = "col1";
      return asList(new CompleteType[][]{
        {union(BIT.toField(name), VARCHAR.toField(name)), VARCHAR},
        {union(INT.toField(name), BIGINT.toField(name)), BIGINT},
        {union(INT.toField(name), FLOAT.toField(name)), DOUBLE},
        {union(INT.toField(name), DOUBLE.toField(name)), DOUBLE},
        {union(INT.toField(name), DECIMAL.toField(name)), DECIMAL},
        {union(INT.toField(name), VARCHAR.toField(name)), VARCHAR},
        {union(BIGINT.toField(name), FLOAT.toField(name)), DOUBLE},
        {union(BIGINT.toField(name), DOUBLE.toField(name)), DOUBLE},
        {union(BIGINT.toField(name), VARCHAR.toField(name)), VARCHAR},
        {union(BIGINT.toField(name), DECIMAL.toField(name)), DECIMAL},
        {union(FLOAT.toField(name), DOUBLE.toField(name)), DOUBLE},
        {union(FLOAT.toField(name), DECIMAL.toField(name)), DECIMAL},
        {union(FLOAT.toField(name), VARCHAR.toField(name)), VARCHAR},
        {union(DOUBLE.toField(name), VARCHAR.toField(name)), VARCHAR},
        {union(DECIMAL.toField(name), VARCHAR.toField(name)), VARCHAR},
        {union(DECIMAL.toField(name), DOUBLE.toField(name)), DOUBLE},
      });
    }

    @Test
    public void testUnionRemoval() {
      CompleteType typeContainingUnions = CompleteType.removeUnions(unionType, this);
      assertThat(typeContainingUnions.getType()).isEqualTo(finalType.getType());
    }
  }

  public static class UnionRemovalTestsInComplexTypes implements SupportsTypeCoercionsAndUpPromotions {
    private final String name = "col1";

    @Test
    public void testUnionRemovalInNestedListOfListOfStruct() {
      Field unionField = union(INT.toField(name), FLOAT.toField(name), DECIMAL.toField(name), VARCHAR.toField(name)).toField(name);
      Field structField = new Field("structField", FieldType.nullable(STRUCT.getType()), singletonList(unionField));
      Field listField = new Field("listField", FieldType.nullable(LIST.getType()), singletonList(structField));
      CompleteType typeWithUnion = new CompleteType(LIST.getType(), listField);

      CompleteType actualType = typeWithUnion.mergeFieldListsWithUpPromotionOrCoercion(typeWithUnion, this);
      assertThat(actualType.getType()).isEqualTo(LIST.getType());
      java.util.List<Field> children = actualType.getOnlyChild().getChildren().get(0).getChildren();
      assertThat(children.size()).isEqualTo(1);
      assertThat(children.get(0).getType()).isEqualTo(VARCHAR.getType());
    }

    @Test
    public void testUnionCannotBeRemovedBitIntVarcharComplex() {
      Field unionField = union(BIT.toField(name), FLOAT.toField(name), VARCHAR.toField(name)).toField(name);
      Field structField = new Field("structField", FieldType.nullable(STRUCT.getType()), singletonList(unionField));
      Field listField = new Field("listField", FieldType.nullable(LIST.getType()), singletonList(structField));
      CompleteType typeWithUnion = new CompleteType(LIST.getType(), listField);
      assertThatThrownBy(() -> typeWithUnion.mergeFieldListsWithUpPromotionOrCoercion(typeWithUnion, this))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("Unable to coerce from the file's data type \"float\" to the column's data type \"boolean\", column \"col1\"");
    }

    @Test
    public void testUnionCannotBeRemovedStructIntVarcharComplex() {
      Field unionField = union(struct(VARCHAR.toField("a"), INT.toField("b")).toField(name), INT.toField(name), VARCHAR.toField(name)).toField(name);
      Field structField = new Field("structField", FieldType.nullable(STRUCT.getType()), singletonList(unionField));
      Field listField = new Field("listField", FieldType.nullable(LIST.getType()), singletonList(structField));
      CompleteType typeWithUnion = new CompleteType(LIST.getType(), listField);
      assertThatThrownBy(() -> typeWithUnion.mergeFieldListsWithUpPromotionOrCoercion(typeWithUnion, this))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("Unable to coerce from the file's data type \"int32\" to the column's data type \"struct<a::varchar, b::int32>\", column \"col1\"");
    }

    @Test
    public void testUnionRemovalInNestedStructOfStructOfList() {
      Field unionField = union(INT.toField(name), FLOAT.toField(name), DECIMAL.toField(name), VARCHAR.toField(name)).toField(name);
      Field listField = new Field("listField", FieldType.nullable(LIST.getType()), singletonList(unionField));
      Field structField = new Field("structField", FieldType.nullable(STRUCT.getType()), singletonList(listField));
      CompleteType typeWithUnion = new CompleteType(STRUCT.getType(), structField);

      CompleteType actualType = typeWithUnion.mergeFieldListsWithUpPromotionOrCoercion(typeWithUnion, this);
      assertThat(actualType.getType()).isEqualTo(STRUCT.getType());
      java.util.List<Field> children = actualType.getOnlyChild().getChildren().get(0).getChildren();
      assertThat(children.size()).isEqualTo(1);
      assertThat(children.get(0).getType()).isEqualTo(VARCHAR.getType());
    }
  }
}
