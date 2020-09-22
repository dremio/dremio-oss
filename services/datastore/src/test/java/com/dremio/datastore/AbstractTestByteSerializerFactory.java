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
package com.dremio.datastore;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

import com.dremio.datastore.format.Format;
import com.dremio.datastore.format.compound.FormatTestArtifacts;

/**
 * Validates that functioning serializers are returned from the CoreStoreSerializerFactory.
 * <p>
 * Validation is performed by taking some type, serializing, and deserializing it back to the original value.
 * <p>
 * We do not test protostuff because there is no protostuff objects in this module. We did not want to find a module
 * with protostuff objects that doesn't depend on datastore nor specify a test only proto.
 */
public abstract class AbstractTestByteSerializerFactory extends FormatTestArtifacts {

  @SuppressWarnings("unchecked")
  protected <T> Serializer<T, byte[]> getSerializer(Format<T> format) {
    return (Serializer<T, byte[]>) format.apply(ByteSerializerFactory.INSTANCE);
  }

  protected abstract <T> void runCircularTest(Format<T> format, T original) throws IOException;

  @Test
  public void stringFormat() throws DatastoreFatalException, IOException {
    runCircularTest(Format.ofString(), TEST_STRING);
  }

  @Test
  public void bytesFormat() throws DatastoreFatalException, IOException {
    runCircularTest(Format.ofBytes(), TEST_STRING.getBytes(UTF_8));
  }

  @Test
  public void uuidFormat() throws DatastoreFatalException, IOException {
    runCircularTest(Format.ofUUID(), UUID.randomUUID());
  }

  /**
   * Wraps a string so that we may test the wrapped format.
   */
  private static class Nesting {
    private final String inner;

    Nesting(String inner) {
      this.inner = inner;
    }

    @Override
    public boolean equals(Object that) {
      if (!(that instanceof Nesting)) {
        return false;
      }

      final Nesting other = (Nesting) that;
      return this.inner.equals(other.inner);
    }

    @Override
    public int hashCode() {
      // Doesn't matter. Not used.
      // Just to pass checkstyle.
      return -1;
    }

    /**
     * Used by the wrapped format to convert the Nesting to a string.
     */
    private static class NestingConverter extends Converter<Nesting, String> {

      @Override
      public String convert(Nesting n) {
        return n.inner;
      }

      @Override
      public Nesting revert(String n) {
        return new Nesting(n);
      }
    }
  }

  @Test
  public void wrappedFormat() throws DatastoreFatalException, IOException {
    final Format<Nesting> format = Format.wrapped(Nesting.class, new Nesting.NestingConverter(), Format.ofString());

    runCircularTest(format, new Nesting(TEST_STRING));
  }

  @Test
  public void protobufFormat() throws DatastoreFatalException, IOException {
    runCircularTest(PROTOBUFF_FORMAT, PROTOBUFF_ORIGINAL_STRING);
  }
}
