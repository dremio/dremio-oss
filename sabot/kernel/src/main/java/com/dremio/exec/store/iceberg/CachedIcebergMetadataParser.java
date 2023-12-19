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
package com.dremio.exec.store.iceberg;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Uses caches to avoid re-loading of the metadata json for repeated operations between the operators.
 */
public final class CachedIcebergMetadataParser {

  private static Cache<Key, Snapshot> CACHE = Caffeine.newBuilder()
      .expireAfterAccess(30, TimeUnit.MINUTES).maximumSize(10_000)
      .build();

  private CachedIcebergMetadataParser() {
    // Not to be instantiated
  }

  public static Snapshot loadSnapshot(String metadataJsonPath, long snapshotId, FileIO fileIO) {
    return CACHE.get(new Key(metadataJsonPath, snapshotId),
        k -> TableMetadataParser.read(fileIO, metadataJsonPath).snapshot(snapshotId));
  }

  public static void cacheSnapshot(String metadataJsonPath, long snapshotId, Snapshot snapshot) {
    CACHE.put(new Key(metadataJsonPath, snapshotId), snapshot);
  }

  private static class Key {
    private final String metadataJsonPath;
    private final long snapshot;

    private Key(String metadataJsonPath, long snapshot) {
      this.metadataJsonPath = metadataJsonPath;
      this.snapshot = snapshot;
    }

    public String getMetadataJsonPath() {
      return metadataJsonPath;
    }

    public long getSnapshot() {
      return snapshot;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Key key = (Key) o;
      return snapshot == key.snapshot && Objects.equals(metadataJsonPath,
          key.metadataJsonPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(metadataJsonPath, snapshot);
    }
  }
}
