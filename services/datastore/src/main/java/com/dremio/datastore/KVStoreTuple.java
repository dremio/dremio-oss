/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.util.Objects;

/**
 * KVStore tuple of (value, serialized value and version) These are lazily loaded and cached.
 */
public class KVStoreTuple<T> {

  private T object;
  private byte[] serializedBytes;
  private Long version;
  private boolean isNull = true;

  private final Serializer<T> serializer;
  private final VersionExtractor<T> versionExtractor;

  private volatile boolean objectLoaded = false;
  private volatile boolean serializedBytesLoaded = false;
  private volatile boolean versionLoaded = false;


  public KVStoreTuple(Serializer<T> serializer, VersionExtractor<T> versionExtractor) {
    this.serializer = serializer;
    this.versionExtractor = versionExtractor;
  }

  public KVStoreTuple(Serializer<T> serializer) {
    this.serializer = serializer;
    versionExtractor = null;
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

  public KVStoreTuple<T> setVersion(Long version) {
    if (!serializedBytesLoaded && !objectLoaded) {
      throw new IllegalArgumentException("Can not set version in KVStoreTuple without setting actual value or serialized value first.");
    }
    this.version = version;
    versionLoaded = true;
    return this;
  }

  public byte[] getSerializedBytes() {
    if (!serializedBytesLoaded) {
      loadSerializedValue();
    }
    return serializedBytes;
  }

  public Long getVersion() {
    if (!versionLoaded) {
      loadVersion();
      versionLoaded = true;
    }
    return version;
  }

  public boolean isNull() {
    return isNull;
  }

  /**
   * Increment old version and reload new version.
   * @return
   */
  public Long incrementVersion() {
    if (!versionLoaded) {
      loadVersion();
      versionLoaded = true;
    }
    Long previousVersion = versionExtractor.incrementVersion(object);
    // cache next version and reload serialized bytes
    version = versionExtractor.getVersion(object);
    loadSerializedValue();
    return previousVersion;
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

  private void loadVersion() {
    if (versionExtractor == null) {
      version = null;
    } else {
      if (!objectLoaded) {
        loadValue();
      }
      if (object != null) {
        version = versionExtractor.getVersion(object);
      }
    }
    versionLoaded = true;
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

    return Objects.equals(version, other.version) &&
      Objects.equals(serializedBytes, other.serializedBytes) &&
      Objects.equals(object, other.object);
  }

  @Override
  public int hashCode() {
    return Objects.hash(object, serializedBytes, version);
  }

  public String toJson() throws IOException {
    return serializer.toJson(getObject());
  }

  public T fromJson(String v) throws IOException {
    return serializer.fromJson(v);
  }
}
