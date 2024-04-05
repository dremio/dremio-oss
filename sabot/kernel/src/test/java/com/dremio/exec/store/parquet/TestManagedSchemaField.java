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

package com.dremio.exec.store.parquet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.common.expression.CompleteType;
import org.junit.Test;

/** Tests for {@link ManagedSchemaField} */
public class TestManagedSchemaField {
  @Test
  public void testUnboundedFieldLen() {
    ManagedSchemaField varcharField =
        ManagedSchemaField.newUnboundedLenField("varchar_col", "varchar");
    assertTrue(varcharField.isUnbounded());
    assertEquals(CompleteType.DEFAULT_VARCHAR_PRECISION, varcharField.getLength());

    ManagedSchemaField decimalField =
        ManagedSchemaField.newUnboundedLenField("decimal_col", "decimal");
    assertTrue(decimalField.isUnbounded());
    assertEquals(CompleteType.MAX_DECIMAL_PRECISION, decimalField.getLength());
  }

  @Test
  public void testUnboundedMaxPrecision() {
    ManagedSchemaField varcharField =
        ManagedSchemaField.newFixedLenField(
            "varchar_col", "varchar(65536)", CompleteType.DEFAULT_VARCHAR_PRECISION, 0);
    assertTrue(varcharField.isUnbounded());
    assertEquals(CompleteType.DEFAULT_VARCHAR_PRECISION, varcharField.getLength());

    ManagedSchemaField decimalField =
        ManagedSchemaField.newFixedLenField(
            "decimal_col", "decimal(38,0)", CompleteType.MAX_DECIMAL_PRECISION, 0);
    assertTrue(decimalField.isUnbounded());
    assertEquals(CompleteType.MAX_DECIMAL_PRECISION, decimalField.getLength());
  }

  @Test
  public void testUnboundedNoPrecision() {
    ManagedSchemaField varcharField =
        ManagedSchemaField.newFixedLenField("varchar_col", "varchar(0)", 0, 0);
    assertTrue(varcharField.isUnbounded());

    ManagedSchemaField decimalField =
        ManagedSchemaField.newFixedLenField("decimal_col", "decimal(0,0)", 0, 0);
    assertTrue(decimalField.isUnbounded());
  }

  @Test
  public void testIsTextField() {
    ManagedSchemaField varcharField =
        ManagedSchemaField.newFixedLenField("varchar_col", "varchar(20)", 20, 0);
    assertTrue(varcharField.isTextField());

    ManagedSchemaField charField =
        ManagedSchemaField.newFixedLenField("char_col", "char(20)", 20, 0);
    assertTrue(charField.isTextField());

    ManagedSchemaField stringField =
        ManagedSchemaField.newFixedLenField(
            "string_col", "String", CompleteType.DEFAULT_VARCHAR_PRECISION, 0);
    assertTrue(stringField.isTextField());

    ManagedSchemaField decimalField =
        ManagedSchemaField.newUnboundedLenField("decimal_col", "decimal");
    assertFalse(decimalField.isTextField());
  }
}
