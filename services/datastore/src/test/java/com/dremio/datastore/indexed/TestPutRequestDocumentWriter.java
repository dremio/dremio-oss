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
import static org.junit.Assert.assertTrue;

/** Tests for PutRequestDocumentWriter. */
public class TestPutRequestDocumentWriter
    extends AbstractTestDocumentWriter<PutRequestDocumentWriter> {
  @Override
  protected PutRequestDocumentWriter createDocumentWriter() {
    return new PutRequestDocumentWriter();
  }

  @Override
  protected void verifySingleIndexValue(
      PutRequestDocumentWriter writer, IndexKey index, Object expectedValue) {
    verifySingleIndexValueAtPosition(writer, index, expectedValue, 0);
  }

  @Override
  protected void verifyMultiIndexValue(
      PutRequestDocumentWriter writer, IndexKey index, Object... expectedValues) {
    for (int i = 0; i < expectedValues.length; i++) {
      verifySingleIndexValueAtPosition(writer, index, expectedValues, i);
    }
  }

  @Override
  protected void verifyNoValues(PutRequestDocumentWriter writer, IndexKey index) {
    assertTrue(writer.byteArrayMap.isEmpty());
    assertTrue(writer.longMap.isEmpty());
    assertTrue(writer.integerMap.isEmpty());
    assertTrue(writer.doubleMap.isEmpty());
    assertTrue(writer.stringMap.isEmpty());
  }

  private static void verifySingleIndexValueAtPosition(
      PutRequestDocumentWriter writer, IndexKey index, Object expectedValue, int position) {
    if (expectedValue instanceof String) {
      assertEquals(expectedValue, writer.stringMap.get(index).get(position));
    } else if (expectedValue instanceof Double) {
      assertEquals((double) expectedValue, writer.doubleMap.get(index).get(position), 0.000001);
    } else if (expectedValue instanceof Integer) {
      assertEquals(expectedValue, writer.integerMap.get(index).get(position));
    } else if (expectedValue instanceof Long) {
      assertEquals(expectedValue, writer.longMap.get(index).get(position));
    } else if (expectedValue instanceof byte[]) {
      assertArrayEquals((byte[]) expectedValue, writer.byteArrayMap.get(index).get(position));
    }
  }
}
