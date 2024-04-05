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
package com.dremio.connector.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.connector.metadata.PartitionValue.PartitionValueType;
import java.nio.ByteBuffer;
import org.junit.Test;

/** Test class for PartitionValues */
public class TestPartitionValues {

  @Test
  public void testInt() {
    int value1 = 1;
    int value2 = 2;

    assertTrue(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value1)));
    assertTrue(
        PartitionValue.of("x", value1).hashCode() == PartitionValue.of("x", value1).hashCode());

    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value2)));
    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("y", value1)));
    assertFalse(
        PartitionValue.of("x", value1)
            .equals(PartitionValue.of("x", value1, PartitionValueType.INVISIBLE)));
  }

  @Test
  public void testLong() {
    long value1 = 1;
    long value2 = 2;

    assertTrue(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value1)));
    assertTrue(
        PartitionValue.of("x", value1).hashCode() == PartitionValue.of("x", value1).hashCode());

    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value2)));
    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("y", value1)));
  }

  @Test
  public void testFloat() {
    float value1 = 1;
    float value2 = 2;

    assertTrue(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value1)));
    assertTrue(
        PartitionValue.of("x", value1).hashCode() == PartitionValue.of("x", value1).hashCode());

    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value2)));
    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("y", value1)));
  }

  @Test
  public void testDouble() {
    double value1 = 1;
    double value2 = 2;

    assertTrue(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value1)));
    assertTrue(
        PartitionValue.of("x", value1).hashCode() == PartitionValue.of("x", value1).hashCode());

    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value2)));
    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("y", value1)));
  }

  @Test
  public void testBoolean() {
    boolean value1 = true;
    boolean value2 = false;

    assertTrue(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value1)));
    assertTrue(
        PartitionValue.of("x", value1).hashCode() == PartitionValue.of("x", value1).hashCode());

    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value2)));
    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("y", value1)));
  }

  @Test
  public void testString() {
    String value1 = "1";
    String value2 = "2";

    assertTrue(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value1)));
    assertTrue(
        PartitionValue.of("x", value1).hashCode() == PartitionValue.of("x", value1).hashCode());

    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value2)));

    // test nulls
    assertTrue(PartitionValue.of("x", (String) null).equals(PartitionValue.of("x", (String) null)));
    assertFalse(PartitionValue.of("x", (String) null).equals(PartitionValue.of("x", value2)));
    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("x", (String) null)));
  }

  @Test
  public void testBinary() {
    ByteBuffer value1 = ByteBuffer.wrap("1".getBytes());
    ByteBuffer value2 = ByteBuffer.wrap("2".getBytes());

    assertTrue(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value1)));
    assertTrue(
        PartitionValue.of("x", value1).hashCode() == PartitionValue.of("x", value1).hashCode());

    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value2)));

    // test nulls
    assertTrue(
        PartitionValue.of("x", (ByteBuffer) null)
            .equals(PartitionValue.of("x", (ByteBuffer) null)));
    assertFalse(PartitionValue.of("x", (ByteBuffer) null).equals(PartitionValue.of("x", value2)));
    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("x", (ByteBuffer) null)));
  }

  @Test
  public void testNoValue() {
    assertTrue(PartitionValue.of("x").equals(PartitionValue.of("x")));
    assertFalse(PartitionValue.of("x").equals(PartitionValue.of("y")));
  }

  @Test
  public void testMixed() {
    double value1 = 1;
    float value2 = 2;

    assertFalse(PartitionValue.of("x", value1).equals(PartitionValue.of("x", value2)));
    assertFalse(PartitionValue.of("x").equals(PartitionValue.of("y", value2)));
  }
}
