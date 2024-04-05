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

import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionState;
import org.immutables.value.Value;

/**
 * Support class represent cached ReflectionEntry and Materialization information that
 * ReflectionManager.sync requires when resolving information of a reflection on its dependencies.
 */
@Value.Immutable
public interface MaterializationInfo {
  MaterializationId getMaterializationId();

  Long getSnapshotId();

  /**
   * Flag marking if the materialization has been vacuumed or not. Vacuum is called after an
   * incremental reflection is refreshed and when: - all its successors in the DependencyGraph are
   * dependent on its latest snapshot. - this flag is not set; if a materialization has already been
   * vacuumed it cannot be vacuumed again.
   */
  Boolean isVacuumed();

  String getReflectionName();

  ReflectionState getReflectionState();

  Long getLastSubmittedRefresh();

  Long getLastSuccessfulRefresh();

  static ImmutableMaterializationInfo.Builder builder() {
    return new ImmutableMaterializationInfo.Builder();
  }
}
