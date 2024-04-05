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

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/** KVStore tuple of (value, serialized value and version) These are lazily loaded and cached. */
public class KVStoreTuple<T> {

  private T object;
  private byte[] serializedBytes;
  private String tag;
  private boolean isNull = true;

  private final Serializer<T, byte[]> serializer;

  private volatile boolean objectLoaded = false;
  private volatile boolean serializedBytesLoaded = false;

  public KVStoreTuple(Serializer<T, byte[]> serializer) {
    this.serializer = serializer;
  }

  public KVStoreTuple<T> setObject(T object) {
    if (object != null) {
      isNull = false;
    }
    this.object = object;
    objectLoaded = true;
    return this;
  }

  public KVStoreTuple<T> setSerializedBytes(byte[] serializedBytes) {
    if (serializedBytes != null) {
      isNull = false;
    }
    this.serializedBytes = serializedBytes;
    serializedBytesLoaded = true;
    return this;
  }

  public KVStoreTuple<T> setTag(String tag) {
    if (!serializedBytesLoaded && !objectLoaded) {
      throw new IllegalArgumentException(
          "Can not set version in KVStoreTuple without setting actual value or serialized value first.");
    }
    this.tag = tag;
    return this;
  }

  public byte[] getSerializedBytes() {
    if (!serializedBytesLoaded) {
      loadSerializedValue();
    }
    return serializedBytes;
  }

  public boolean isNull() {
    return isNull;
  }

  public T getObject() {
    if (!objectLoaded) {
      loadValue();
      objectLoaded = true;
    }
    return object;
  }

  private void loadSerializedValue() {
    if (object != null) {
      serializedBytes = serializer.serialize(object);
    }
    serializedBytesLoaded = true;
  }

  private void loadValue() {
    if (serializedBytes != null) {
      object = serializer.deserialize(serializedBytes);
    }
    objectLoaded = true;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof KVStoreTuple)) {
      return false;
    }
    KVStoreTuple<?> other = (KVStoreTuple<?>) o;

    return Objects.equals(tag, other.tag)
        && Arrays.equals(serializedBytes, other.serializedBytes)
        && Objects.deepEquals(object, other.object);
  }

  @Override
  public int hashCode() {
    return Arrays.deepHashCode(new Object[] {object, serializedBytes, tag});
  }

  public String toJson() throws IOException {
    return serializer.toJson(getObject());
  }

  public T fromJson(String v) throws IOException {
    return serializer.fromJson(v);
  }
}
