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

import java.util.Optional;

import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.RefreshRequest;

/**
 * DependencyResolutionContext interfaces controls how a ReflectionManager.sync
 * resolves information it needs on its dependencies such as dataset reflection
 * settings and refresh requests.  Lifecycle tied to just a single sync operation
 * and then discarded.
 */
public interface DependencyResolutionContext extends AutoCloseable {
  Optional<Long> getLastSuccessfulRefresh(ReflectionId id);
  AccelerationSettings getReflectionSettings(NamespaceKey key);
  RefreshRequest getRefreshRequest(String datasetId);

  /**
   * @return true if acceleration settings have changed in the system between the current context
   * and the last context.
   */
  boolean hasAccelerationSettingsChanged();
  @Override
  void close();
}
