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

import com.dremio.exec.planner.acceleration.descriptor.UnexpandedMaterializationDescriptor;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationPlan;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;

/** A factory to create {@code MaterializationDescriptor} instances */
public interface MaterializationDescriptorFactory {
  /** Create a new materialization descriptor */
  UnexpandedMaterializationDescriptor getMaterializationDescriptor(
      final ReflectionGoal reflectionGoal,
      final ReflectionEntry reflectionEntry,
      final Materialization materialization,
      final MaterializationPlan plan,
      double originalCost,
      final CatalogService catalogService);
}
