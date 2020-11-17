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
package com.dremio.datastore.indexed;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.datastore.api.DocumentWriter;

/**
 * Tests for DocumentWriter
 */
public abstract class AbstractTestDocumentWriter<D extends DocumentWriter> {
  private D writer;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    writer = createDocumentWriter();
  }

  protected abstract D createDocumentWriter();

  protected abstract void verifySingleIndexValue(D writer, IndexKey index, Object expectedValue);
  protected abstract void verifyMultiIndexValue(D writer, IndexKey index, Object... expectedValues);
  protected abstract void verifyNoValues(D writer, IndexKey index);

  @Test
  public void testString() {
    final IndexKey indexKey = newIndexKey(String.class, true);
    writer.write(indexKey, "testString1", "testString2");

    verifyMultiIndexValue(writer, indexKey,  "testString1", "testString2");
  }

  @Test
  public void testDouble() {
    final IndexKey indexKey = newIndexKey(Double.class, false);
    writer.write(indexKey, Double.NaN);

    verifySingleIndexValue(writer, indexKey, Double.NaN);
  }

  @Test
  public void testInt() {
    final IndexKey indexKey = newIndexKey(Integer.class, false);
    writer.write(indexKey, 100);

    verifySingleIndexValue(writer, indexKey,100);
  }

  @Test
  public void testLong() {
    final IndexKey indexKey = newIndexKey(Long.class, false);
    writer.write(indexKey, Long.MAX_VALUE);

    verifySingleIndexValue(writer, indexKey, Long.MAX_VALUE);
  }

  @Test
  public void testBytes() {
    final IndexKey indexKey = newIndexKey(String.class, true);
    final byte[] firstArray = new byte[] {0x00};
    final byte[] secondArray = new byte[] {0x01};
    writer.write(indexKey, firstArray, secondArray);

    verifyMultiIndexValue(writer, indexKey, firstArray, secondArray);
  }

  @Test
  public void testMultiString() {
    final IndexKey testKeyMultiple = IndexKey.newBuilder("testmulti","TEST_MULTI", String.class)
      .setCanContainMultipleValues(true)
      .build();

    writer.write(testKeyMultiple, "foo");
    writer.write(testKeyMultiple, "bar");
    writer.write(testKeyMultiple, "baz", "zap");

    verifyMultiIndexValue(writer, testKeyMultiple, "foo", "bar", "baz", "zap");
  }

  @Test
  public void testSingleStringMultiValueAtOnce() {
    final IndexKey testKey = IndexKey.newBuilder("test", "TEST", String.class)
      .build();

    // Expect an IllegalStateException because testKey is a single key index and we are trying to
    // write multiple values to it.
    thrown.expect(IllegalStateException.class);
    writer.write(testKey, "foo", "bar");
  }

  @Test
  public void testSingleStringMultiValueIteratively() {
    final IndexKey testKey = IndexKey.newBuilder("test", "TEST", String.class)
      .build();

    writer.write(testKey, "foo");

    // Expect an IllegalStateException because testKey is a single key index and we are trying to
    // write multiple values to it (by issuing multiple calls to write, rather than a single write call with
    // multiple values).
    thrown.expect(IllegalStateException.class);
    writer.write(testKey, "bar");
  }

  @Test
  public void testMultiNumeric() {
    final IndexKey testKeyMultiple = IndexKey.newBuilder("testmulti", "TEST_MULTI", Integer.class)
      .setCanContainMultipleValues(true)
      .build();

    writer.write(testKeyMultiple, 1);
    writer.write(testKeyMultiple, 2);

    verifyMultiIndexValue(writer, testKeyMultiple, 1, 2);
  }

  @Test
  public void testSingleNumericMultiValue() {
    final IndexKey testKey = IndexKey.newBuilder("test", "TEST", Integer.class)
      .build();
    writer.write(testKey, 1);

    // Expect an IllegalStateException because testKey is a single key index and we are trying to
    // write multiple values to it (by issuing multiple calls to write, rather than a single write call with
    // multiple values).
    thrown.expect(IllegalStateException.class);
    writer.write(testKey, 2);
  }

  @Test
  public void testMultiByte() {
    final IndexKey testKeyMultiple = IndexKey.newBuilder("testmulti","TEST_MULTI", String.class)
      .setCanContainMultipleValues(true)
      .build();

    writer.write(testKeyMultiple, new byte[] {0x01});
    writer.write(testKeyMultiple, new byte[] {0x02});
    writer.write(testKeyMultiple, new byte[] {0x03}, new byte[] {0x04});

    verifyMultiIndexValue(writer, testKeyMultiple,
      new byte[] {0x01},
      new byte[] {0x02},
      new byte[] {0x03},
      new byte[] {0x04});
  }

  @Test
  public void testSingleByteMultiValueAtOnce() {
    final IndexKey testKey = IndexKey.newBuilder("test", "TEST", String.class)
      .build();

    // Expect an IllegalStateException because testKey is a single key index and we are trying to
    // write multiple values to it.
    thrown.expect(IllegalStateException.class);
    writer.write(testKey, new byte[] {0x03}, new byte[] {0x04});
  }

  @Test
  public void testSingleByteMultiValueIteratively() {
    final IndexKey testKey = IndexKey.newBuilder("test", "TEST", String.class)
      .build();

    writer.write(testKey, new byte[] {0x01});

    // Expect an IllegalStateException because testKey is a single key index and we are trying to
    // write multiple values to it (by issuing multiple calls to write, rather than a single write call with
    // multiple values).
    thrown.expect(IllegalStateException.class);
    writer.write(testKey, new byte[] {0x01});
  }

  @Test
  public void testWritingNullNumeric() {
    final IndexKey indexKey = newIndexKey(Long.class, true);
    writer.write(indexKey, (Long) null);
    verifyNoValues(writer, indexKey);
  }

  @Test(expected = NullPointerException.class)
  public void testWritingNullStringArray() {
    final IndexKey indexKey = newIndexKey(String.class, true);
    writer.write(indexKey, (String[]) null);
  }

  @Test
  public void testWritingNullString() {
    final IndexKey indexKey = newIndexKey(String.class, true);
    writer.write(indexKey, (String) null);
    verifyNoValues(writer, indexKey);
  }

  @Test(expected = NullPointerException.class)
  public void testWritingNullByteArrayArray() {
    final IndexKey indexKey = newIndexKey(String.class, true);
    writer.write(indexKey, (byte[][]) null);
  }

  @Test
  public void testWritingNullByteArray() {
    final IndexKey indexKey = newIndexKey(String.class, true);
    writer.write(indexKey, (byte[]) null);
    verifyNoValues(writer, indexKey);
  }

  protected static void verifyHelper(Object expected, Object actual) {
    if (expected instanceof Double && actual instanceof Double) {
      assertEquals((double)expected, (double)actual, 0.0000);
    } else if (expected instanceof byte[] && actual instanceof byte[]) {
      assertArrayEquals((byte[]) expected, (byte[]) actual);
    } else {
      assertEquals(expected, actual);
    }
  }

  protected static IndexKey newIndexKey(Class<?> type, boolean multiValue) {
    return IndexKey.newBuilder("value", "value", type)
      .setCanContainMultipleValues(multiValue)
      .build();
  }
}
