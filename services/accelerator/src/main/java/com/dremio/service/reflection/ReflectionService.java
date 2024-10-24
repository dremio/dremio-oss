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
package com.dremio.service.reflection;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationListManager.ReflectionLineageInfo;
import com.dremio.exec.store.sys.accel.AccelerationManager.ExcludedReflectionsProvider;
import com.dremio.service.Service;
import com.dremio.service.reflection.MaterializationCache.CacheViewer;
import com.dremio.service.reflection.analysis.ReflectionSuggester.ReflectionSuggestionType;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import javax.inject.Provider;

/** Reflection service */
public interface ReflectionService extends Service, ReflectionAdministrationService {

  Iterable<ExternalReflection> getAllExternalReflections();

  Optional<ReflectionEntry> getEntry(ReflectionId reflectionId);

  ExcludedReflectionsProvider getExcludedReflectionsProvider();

  void saveEntry(ReflectionEntry reflectionEntry);

  @Override
  Optional<Materialization> getLastDoneMaterialization(ReflectionId reflectionId);

  Materialization getLastMaterialization(ReflectionId reflectionId);

  Iterable<Materialization> getMaterializations(ReflectionId reflectionId);

  /** wakes up the reflection manager if it isn't already running. */
  Future<?> wakeupManager(String reason);

  Iterator<AccelerationListManager.DependencyInfo> getReflectionDependencies();

  Optional<Materialization> getMaterialization(MaterializationId materializationId);

  Iterable<Refresh> getRefreshes(Materialization materialization);

  Provider<CacheViewer> getCacheViewerProvider();

  Optional<ReflectionManager> getReflectionManager();

  void addDatasetRemovedListener(DatasetRemovedHandler listener);

  /** Admin action to clean reflection collections to get reflection management healthy */
  void clean();

  /** mainly useful to reduce conflicts on the implementation when we update this interface */
  class BaseReflectionService implements ReflectionService {
    @Override
    public Iterable<ReflectionGoal> getAllReflections() {
      return Collections.emptyList();
    }

    @Override
    public Iterable<ReflectionGoal> getReflectionsByDatasetPath(CatalogEntityKey path) {
      return Collections.emptyList();
    }

    @Override
    public Iterable<ReflectionGoal> getReflectionsByDatasetId(String datasetId) {
      return Collections.emptyList();
    }

    @Override
    public ReflectionId create(ReflectionGoal goal) {
      return null;
    }

    @Override
    public ReflectionId createExternalReflection(
        String name, List<String> datasetPath, List<String> targetDatasetPath) {
      return null;
    }

    @Override
    public Iterable<ExternalReflection> getAllExternalReflections() {
      return Collections.emptyList();
    }

    @Override
    public Optional<ExternalReflection> getExternalReflectionById(String id) {
      return Optional.empty();
    }

    @Override
    public Iterable<ExternalReflection> getExternalReflectionByDatasetPath(
        List<String> datasetPath) {
      return Collections.emptyList();
    }

    @Override
    public void dropExternalReflection(String idOrName) {}

    @Override
    public void update(ReflectionGoal goal) {}

    @Override
    public void setSubstitutionEnabled(boolean enable) {}

    @Override
    public boolean isSubstitutionEnabled() {
      return false;
    }

    @Override
    public void remove(ReflectionGoal goal) {}

    @Override
    public Optional<ReflectionEntry> getEntry(ReflectionId reflectionId) {
      return Optional.empty();
    }

    @Override
    public void saveEntry(ReflectionEntry reflectionEntry) {}

    @Override
    public Optional<ReflectionGoal> getGoal(ReflectionId reflectionId) {
      return Optional.empty();
    }

    @Override
    public Optional<Materialization> getLastDoneMaterialization(ReflectionId reflectionId) {
      return Optional.empty();
    }

    @Override
    public Materialization getLastMaterialization(ReflectionId reflectionId) {
      return null;
    }

    @Override
    public Iterable<Materialization> getMaterializations(ReflectionId reflectionId) {
      return null;
    }

    @Override
    public void start() {}

    @Override
    public void close() throws Exception {}

    @Override
    public void clearAll() {}

    @Override
    public void retryUnavailable() {}

    @Override
    public void clean() {}

    @Override
    public Iterator<AccelerationListManager.DependencyInfo> getReflectionDependencies() {
      throw new UnsupportedOperationException("getReflectionDependencies");
    }

    @Override
    public Optional<Materialization> getMaterialization(MaterializationId materializationId) {
      return Optional.empty();
    }

    @Override
    public Iterable<Refresh> getRefreshes(Materialization materialization) {
      return Collections.emptyList();
    }

    @Override
    public List<ReflectionGoal> getRecommendedReflections(
        String datasetId, ReflectionSuggestionType type) {
      return Collections.emptyList();
    }

    @Override
    public ReflectionSettings getReflectionSettings() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public int getEnabledReflectionCountForDataset(String datasetId) {
      return 0;
    }

    @Override
    public boolean isReflectionIncremental(ReflectionId reflectionId) {
      return false;
    }

    @Override
    public void requestRefresh(String datasetId) {}

    @Override
    public Future<?> wakeupManager(String reason) {
      return new FutureTask<Void>(null, null);
    }

    @Override
    public Provider<CacheViewer> getCacheViewerProvider() {
      return null;
    }

    @Override
    public ExcludedReflectionsProvider getExcludedReflectionsProvider() {
      return new ExcludedReflectionsProvider() {
        @Override
        public List<String> getExcludedReflections(String rId) {
          return ImmutableList.of();
        }
      };
    }

    @Override
    public Optional<ReflectionManager> getReflectionManager() {
      return Optional.empty();
    }

    @Override
    public Iterator<ReflectionLineageInfo> getReflectionLineage(ReflectionGoal reflectionGoal) {
      throw new UnsupportedOperationException("getReflectionLineage");
    }

    @Override
    public void addDatasetRemovedListener(DatasetRemovedHandler listener) {}
  }

  /** Reflection related entity not found. */
  class NotFoundException extends RuntimeException {
    NotFoundException(String msg) {
      super(msg);
    }
  }
}
