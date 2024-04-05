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
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import java.util.List;
import java.util.Optional;

/** Interface for administrating reflections. */
public interface ReflectionAdministrationService {
  Iterable<ReflectionGoal> getAllReflections();

  /** Returns all reflections on the given dataset, if it exists. */
  Iterable<ReflectionGoal> getReflectionsByDatasetPath(CatalogEntityKey path);

  /** Returns all reflections on the given dataset, if it exists. */
  Iterable<ReflectionGoal> getReflectionsByDatasetId(String datasetId);

  ReflectionId create(ReflectionGoal goal);

  void update(ReflectionGoal goal);

  void remove(ReflectionGoal goal);

  Optional<ReflectionGoal> getGoal(ReflectionId reflectionId);

  List<ReflectionGoal> getRecommendedReflections(String datasetId);

  Optional<Materialization> getLastDoneMaterialization(ReflectionId reflectionId);

  ReflectionId createExternalReflection(
      String name, List<String> datasetPath, List<String> targetDatasetPath);

  Iterable<ExternalReflection> getExternalReflectionByDatasetPath(List<String> datasetPath);

  void dropExternalReflection(String id);

  /**
   * Set whether to expose available materializations for substitution.
   *
   * @param enable True if you want substitution enabled.
   */
  void setSubstitutionEnabled(boolean enable);

  boolean isSubstitutionEnabled();

  ReflectionSettings getReflectionSettings();

  void clearAll();

  void retryUnavailable();

  void requestRefresh(String datasetId);

  int getEnabledReflectionCountForDataset(String datasetId);

  boolean isReflectionIncremental(ReflectionId reflectionId);

  /** Factory for {@ReflectionAdministrationService} */
  interface Factory {
    ReflectionAdministrationService get(ReflectionContext context);
  }
}
