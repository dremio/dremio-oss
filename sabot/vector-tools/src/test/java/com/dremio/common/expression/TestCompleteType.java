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
import static com.dremio.common.expression.CompleteType.DECIMAL;
import static com.dremio.common.expression.CompleteType.DOUBLE;
import static com.dremio.common.expression.CompleteType.FLOAT;
import static com.dremio.common.expression.CompleteType.INT;
import static com.dremio.common.expression.CompleteType.LIST;
import static com.dremio.common.expression.CompleteType.STRUCT;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collection;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Enclosed.class)
public class TestCompleteType {
  @RunWith(value = Parameterized.class)
  public static class SupportedUpPromotionTests {
    private final CompleteType fileType;
    private final CompleteType tableType;

    public SupportedUpPromotionTests(CompleteType fileType, CompleteType tableType) {
      this.fileType = fileType;
      this.tableType = tableType;
    }

    @Parameters(name = "testSuccessfulUpPromotion_{1}To{0}")
    public static Collection<CompleteType[]> data() {
      return Arrays.asList(new CompleteType[][]{
        {BIGINT, INT},
        {FLOAT, INT},
        {FLOAT, BIGINT},
        {DOUBLE, INT},
        {DOUBLE, BIGINT},
        {DOUBLE, FLOAT},
        {DOUBLE, DECIMAL},
        {DECIMAL, INT},
        {DECIMAL, BIGINT},
        {DECIMAL, FLOAT},
      });
    }

    @Test
    public void testSuccessfulUpPromotion() {
      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType);
      assertThat(upPromotedType.getType(), is(fileType.getType()));
      assertThat(upPromotedType.getType(), is(not(tableType.getType())));
    }
  }

  public static class UnsupportedUpPromotionTests {
    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedUpPromotion() {
      LIST.mergeFieldListsWithUpPromotionOrCoercion(STRUCT);
      STRUCT.mergeFieldListsWithUpPromotionOrCoercion(LIST);
    }
  }

  public static class SupportedUpPromotionTestsForComplexTypes {
    @Test
    public void testIntToBigintUpPromotionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(64, true)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType);
      assertThat(upPromotedType.getType().getClass(), is(List.class));
      assertThat(upPromotedType.getOnlyChild().getType().getClass(), is(ArrowType.Int.class));
      assertThat(((ArrowType.Int) upPromotedType.getOnlyChild().getType()).getBitWidth(), is(64));
    }

    @Test
    public void testIntToBigintUpPromotionInStruct() {
      CompleteType fileType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new Int(64, true)));

      CompleteType upPromotedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType);
      assertThat(upPromotedType.getType().getClass(), is(STRUCT.getType().getClass()));
      assertThat(upPromotedType.getOnlyChild().getType().getClass(), is(ArrowType.Int.class));
      assertThat(((ArrowType.Int) upPromotedType.getOnlyChild().getType()).getBitWidth(), is(64));
    }
  }

  @RunWith(value = Parameterized.class)
  public static class SupportedCoercionTests {
    private final CompleteType fileType;
    private final CompleteType tableType;

    public SupportedCoercionTests(CompleteType fileType, CompleteType tableType) {
      this.fileType = fileType;
      this.tableType = tableType;
    }

    @Parameters(name = "testSuccessfulCoercion_{1}To{0}")
    public static Collection<CompleteType[]> data() {
      return Arrays.asList(new CompleteType[][]{
        {INT, BIGINT},
        {FLOAT, DOUBLE},
        {new CompleteType(new ArrowType.Decimal(10, 10)), DECIMAL},
      });
    }

    @Test
    public void testSuccessfulUpPromotion() {
      CompleteType coercedType = tableType.mergeFieldListsWithUpPromotionOrCoercion(fileType);
      assertThat(coercedType.getType(), is(tableType.getType()));
      assertThat(coercedType.getType(), is(not(fileType.getType())));
    }
  }

  public static class SupportedCoercionTestsForComplexTypes {
    @Test
    public void testIntToBigintCoercionInList() {
      CompleteType fileType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(List.INSTANCE, Field.nullable("col1", new Int(64, true)));

      CompleteType upPromotedType = fileType.mergeFieldListsWithUpPromotionOrCoercion(tableType);
      assertThat(upPromotedType.getType().getClass(), is(List.class));
      assertThat(upPromotedType.getOnlyChild().getType().getClass(), is(ArrowType.Int.class));
      assertThat(((ArrowType.Int) upPromotedType.getOnlyChild().getType()).getBitWidth(), is(64));
    }

    @Test
    public void testIntToBigintCoercionInStruct() {
      CompleteType fileType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new Int(32, true)));
      CompleteType tableType = new CompleteType(STRUCT.getType(), Field.nullable("col1", new Int(64, true)));

      CompleteType upPromotedType = fileType.mergeFieldListsWithUpPromotionOrCoercion(tableType);
      assertThat(upPromotedType.getType().getClass(), is(STRUCT.getType().getClass()));
      assertThat(upPromotedType.getOnlyChild().getType().getClass(), is(ArrowType.Int.class));
      assertThat(((ArrowType.Int) upPromotedType.getOnlyChild().getType()).getBitWidth(), is(64));
    }
  }
}