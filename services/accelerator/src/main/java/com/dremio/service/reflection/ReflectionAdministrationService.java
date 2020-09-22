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

import java.util.List;

import com.dremio.exec.ops.ReflectionContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.google.common.base.Optional;

/**
 * Interface for administrating reflections.
 */
public interface ReflectionAdministrationService {
  Iterable<ReflectionGoal> getAllReflections();

  Iterable<ReflectionGoal> getReflectionsByDatasetPath(NamespaceKey path);

  Iterable<ReflectionGoal> getReflectionsByDatasetId(String datasetid);

  ReflectionId create(ReflectionGoal goal);

  void update(ReflectionGoal goal);

  void remove(ReflectionGoal goal);

  Optional<ReflectionGoal> getGoal(ReflectionId reflectionId);

  List<ReflectionGoal> getRecommendedReflections(String datasetId);

  boolean doesReflectionHaveAnyMaterializationDone(ReflectionId reflectionId);

  long getTotalReflectionSize(ReflectionId reflectionId);

  ReflectionId createExternalReflection(String name, List<String> dataset, List<String> targetDataset);

  Iterable<ExternalReflection> getExternalReflectionByDatasetPath(List<String> datasetPath);

  void dropExternalReflection(String id);

  /**
   * Set whether to expose available materializations for substitution.
   *
   * @param enable True if you want substitution enabled.
   */
  void setSubstitutionEnabled(boolean enable);

  boolean isSubstitutionEnabled();

  long getReflectionSize(ReflectionId reflectionId);

  ReflectionSettings getReflectionSettings();

  void clearAll();

  void requestRefresh(String datasetId);

  int getEnabledReflectionCountForDataset(String datasetid);

  boolean isReflectionIncremental(ReflectionId reflectionId);

  /**
   * Factyory for {@ReflectionAdministrationService}
   */
  interface Factory {
    ReflectionAdministrationService get(ReflectionContext context);
  }
}
