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
package com.dremio.test.mongo;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.config.Defaults;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.extract.ExtractedFileSet;
import de.flapdoodle.embed.process.store.IArtifactStore;

final class StaticArtifactStore implements IArtifactStore, Closeable {
  /*
   * use of cache to serialize requests for the same distribution
   */
  private final LoadingCache<Distribution, Optional<ExtractedFileSet>> distributions = CacheBuilder.newBuilder()
      .removalListener(new RemovalListener<Distribution, Optional<ExtractedFileSet>>() {
        @Override
        public void onRemoval(RemovalNotification<Distribution, Optional<ExtractedFileSet>> notification) {
          store.removeFileSet(notification.getKey(), notification.getValue().get());
        }
      })
      .build(new CacheLoader<Distribution, Optional<ExtractedFileSet>>() {
       @Override
        public Optional<ExtractedFileSet> load(Distribution key) throws IOException {
         return store.extractFileSet(key);
        }
      });

  private final IArtifactStore store;

  private StaticArtifactStore(IArtifactStore store) {
    this.store = store;
  }

  public static StaticArtifactStore forCommand(Command command) {
    IArtifactStore store = Defaults.extractedArtifactStoreFor(command);

    return new StaticArtifactStore(store);
  }

  @Override
  public Optional<ExtractedFileSet> extractFileSet(Distribution distribution) throws IOException {
    Optional<ExtractedFileSet> fileSet = getFileSet(distribution);
    if (!fileSet.isPresent()) {
      throw new IllegalArgumentException("No file set found for distribution " + distribution);
    }
    return fileSet;
  }

  @Override
  public void removeFileSet(Distribution distribution, ExtractedFileSet files) {
    // do nothing
  }

  private Optional<ExtractedFileSet> getFileSet(Distribution distribution) throws IOException {
    try {
      return distributions.get(distribution);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public void close() throws IOException {
    distributions.invalidateAll();
    distributions.cleanUp();
  }
}
