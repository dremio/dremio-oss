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
package com.dremio.datastore.format;

import com.dremio.datastore.format.compound.KeyPair;
import com.dremio.datastore.format.compound.KeyTriple;
import java.util.Arrays;
import java.util.Objects;

/** Tests for expected classes returned from {@link Format#getRepresentedClass()} */
public class WrappedCompoundFormats {
  /** Wraps a KeyPair object. */
  public static class KeyPairStringContainer {
    private final KeyPair<String, String> containedObject;

    public KeyPairStringContainer(KeyPair<String, String> containedObject) {
      this.containedObject = containedObject;
    }

    public KeyPair<String, String> getContainedObject() {
      return containedObject;
    }

    @Override
    public boolean equals(Object obj) {
      return Objects.deepEquals(this, obj);
    }

    @Override
    public int hashCode() {
      return Objects.hash(containedObject);
    }
  }

  /** Wraps a KeyTriple object. */
  public static class KeyTripleStringContainer {
    private final KeyTriple<String, String, String> containedObject;

    public KeyTripleStringContainer(KeyTriple<String, String, String> containedObject) {
      this.containedObject = containedObject;
    }

    public KeyTriple<String, String, String> getContainedObject() {
      return containedObject;
    }

    @Override
    public boolean equals(Object obj) {
      return Objects.deepEquals(this, obj);
    }

    @Override
    public int hashCode() {
      return Objects.hash(containedObject);
    }
  }

  /** Wraps a KeyPair object. */
  public static class KeyPairBytesContainer {
    private final KeyPair<byte[], byte[]> containedObject;

    public KeyPairBytesContainer(KeyPair<byte[], byte[]> containedObject) {
      this.containedObject = containedObject;
    }

    public KeyPair<byte[], byte[]> getContainedObject() {
      return containedObject;
    }

    @Override
    public boolean equals(Object obj) {
      return Objects.deepEquals(this, obj);
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(containedObject.toArray());
    }
  }

  /** Wraps a KeyTriple object. */
  public static class KeyTripleBytesContainer {
    private final KeyTriple<byte[], byte[], byte[]> containedObject;

    public KeyTripleBytesContainer(KeyTriple<byte[], byte[], byte[]> containedObject) {
      this.containedObject = containedObject;
    }

    public KeyTriple<byte[], byte[], byte[]> getContainedObject() {
      return containedObject;
    }

    @Override
    public boolean equals(Object obj) {
      return Objects.deepEquals(this, obj);
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(containedObject.toArray());
    }
  }
}
