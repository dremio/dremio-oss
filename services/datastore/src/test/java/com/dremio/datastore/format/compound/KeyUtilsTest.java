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
package com.dremio.datastore.format.compound;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.Test;

/**
 * {@link KeyUtils} tests
 */
public class KeyUtilsTest extends FormatTestArtifacts {

  @Test
  public void testEqualsReturnsFalseAllNull() {
    assertTrue(KeyUtils.equals(null, null));
  }

  @Test
  public void testEqualsReturnsFalseLeftNull() {
    assertFalse(KeyUtils.equals(null, Boolean.TRUE));
  }

  @Test
  public void testEqualsReturnsFalseRightNull() {
    assertFalse(KeyUtils.equals(Boolean.TRUE, null));
  }

  @Test
  public void testEqualsReturnsTrueWithBooleanTrueTrue() {
    assertTrue(KeyUtils.equals(Boolean.TRUE, Boolean.TRUE));
  }

  @Test
  public void testEqualsReturnsTrueWithBooleanFalseFalse() {
    assertTrue(KeyUtils.equals(Boolean.FALSE, Boolean.FALSE));
  }

  @Test
  public void testEqualsReturnsFalseWithBooleanTrueFalse() {
    assertFalse(KeyUtils.equals(Boolean.TRUE, Boolean.FALSE));
  }

  @Test
  public void testEqualsReturnsFalseWithBooleanFalseTrue() {
    assertFalse(KeyUtils.equals(Boolean.FALSE, Boolean.TRUE));
  }

  @Test
  public void testEqualsReturnsTrueWithByteArrays() {
    byte[] left = new byte[]{0, 1, 2, 3, 4};
    byte[] right = new byte[]{0, 1, 2, 3, 4};

    assertTrue(KeyUtils.equals(left, right));
  }

  @Test
  public void testEqualsReturnsFalseWithByteArrays() {
    byte[] left = new byte[]{0, 1, 2, 3, 4};
    byte[] right = new byte[]{0, 1, 2, 3, 5};

    assertFalse(KeyUtils.equals(left, right));
  }

  @Test
  public void testEqualsReturnsFalseWithByteArraysDifferentLength() {
    byte[] left = new byte[]{0, 1, 2, 3, 4};
    byte[] right = new byte[]{0, 1, 2, 3};

    assertFalse(KeyUtils.equals(left, right));
  }

  @Test
  public void testEqualsReturnsTrueByteProtoObject() {
    assertTrue(KeyUtils.equals(
      new KeyPair<>(
        TEST_STRING.getBytes(UTF_8),
        "some other silly string".getBytes(UTF_8)),
      new KeyPair<>(
        TEST_STRING.getBytes(UTF_8),
        "some other silly string".getBytes(UTF_8))));
  }

  @Test
  public void testEqualsReturnsFalseByteProtoObject() {
    assertFalse(KeyUtils.equals(
      new KeyPair<>(
        TEST_STRING.getBytes(UTF_8),
        "some other silly string".getBytes(UTF_8)),
      new KeyPair<>(
        TEST_STRING.getBytes(UTF_8),
        "some other silly".getBytes(UTF_8))));
  }

  @Test
  public void testEqualsReturnsFalseByteProtoObjectWithNull() {
    assertFalse(KeyUtils.equals(
      new KeyPair<>(
        TEST_STRING.getBytes(UTF_8),
        "some other silly string".getBytes(UTF_8)),
      new KeyPair<>(
        TEST_STRING.getBytes(UTF_8),
        null)));
  }

  @Test
  public void testEqualsReturnsFalseWithProtoObjectsDifferentUUID() {
    assertFalse(KeyUtils.equals(
      new KeyPair<>(
        PROTOBUFF_ORIGINAL_STRING,
        UUID.randomUUID()),
      new KeyTriple<>(
        "Some string",
        PROTOBUFF_ORIGINAL_STRING,
        UUID.randomUUID())));
  }

  @Test
  public void testEqualsReturnsFalseWithNestedProtoObjectsDifferentUUID() {
    assertFalse(KeyUtils.equals(
      new KeyTriple<>(
        new KeyPair<>(PROTOBUFF_ORIGINAL_STRING, UUID.randomUUID()),
        new KeyTriple<>("Some string", PROTOBUFF_ORIGINAL_STRING, UUID.randomUUID()),
        new KeyTriple<>("Some string", PROTOBUFF_ORIGINAL_STRING, UUID.randomUUID())),
      new KeyTriple<>(
        new KeyPair<>(PROTOBUFF_ORIGINAL_STRING, UUID.randomUUID()),
        new KeyTriple<>("Some string", PROTOBUFF_ORIGINAL_STRING, UUID.randomUUID()),
        new KeyTriple<>("Some string", PROTOBUFF_ORIGINAL_STRING, UUID.randomUUID()))));
  }

  @Test
  public void testToStringNull() {
    assertNull(KeyUtils.toString(null));
  }

  @Test
  public void testToStringBytes() {
    assertEquals("[1, 2, 3, 4]", KeyUtils.toString(new byte[]{1, 2, 3, 4}));
  }

  @Test
  public void testToStringString() {
    assertEquals("[1, 2, 3, 4]", KeyUtils.toString("[1, 2, 3, 4]"));
  }

  @Test
  public void testToStringProtoStringBytesKeyPair() {
    assertEquals("KeyPair{ key1=somestring, key2=[1, 23, 43, 5]}", KeyUtils.toString(
      new KeyPair<>("somestring", new byte[]{1, 23, 43, 5})));
  }

  @Test
  public void testToStringProtoByteKeyPairByteKeyTriple() {
    assertEquals("KeyTriple{ key1=[123, 127, 9, 0], key2=KeyPair{ key1=somestring, key2=[1, 23, 43, 5]}, key3=[0, 0, 0, 0]}",
      KeyUtils.toString(
        new KeyTriple<>(
          new byte[]{123, 127, 9, 0},
          new KeyPair<>("somestring", new byte[]{1, 23, 43, 5}),
          new byte[]{0, 0, 0, 0})));
  }

  @Test
  public void testHashBytes() {
    assertEquals(918073283, KeyUtils.hash(new byte[]{1, 2, 3, 4, 5, 6}));
  }

  @Test
  public void testHashBytesAndString() {
    assertEquals(214780274, KeyUtils.hash(new byte[]{1, 2, 3, 4, 5, 6}, "test hash string"));
  }

  @Test
  public void testHashBytesAndStringAndNull() {
    assertEquals(-1931746098, KeyUtils.hash(new byte[]{1, 2, 3, 4, 5, 6}, "test hash string", null));
  }

  @Test
  public void testHashString() {
    assertEquals(1819279604, KeyUtils.hash("test hash string"));
  }

  @Test
  public void testHashNull() {
    assertEquals(0, KeyUtils.hash(null));
  }
}
