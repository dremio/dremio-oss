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
package com.dremio.service.reflection;

import com.dremio.exec.proto.ReflectionRPC;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.service.reflection.proto.ReflectionId;

/**
 * Computes the reflection status for a reflections and external reflections
 */
public interface ReflectionStatusService {

  ReflectionStatus getReflectionStatus(ReflectionId reflectionId);

  ExternalReflectionStatus getExternalReflectionStatus(ReflectionId reflectionId);

  Iterable<AccelerationListManager.ReflectionInfo> getReflections();

  Iterable<ReflectionRPC.RefreshInfo> getRefreshInfos();

  ReflectionStatusService NOOP = new ReflectionStatusService() {

    @Override
    public ReflectionStatus getReflectionStatus(ReflectionId reflectionId) {
      throw new UnsupportedOperationException("getReflectionStatus");
    }

    @Override
    public ExternalReflectionStatus getExternalReflectionStatus(ReflectionId reflectionId) {
      throw new UnsupportedOperationException("getExternalReflectionStatus");
    }

    @Override
    public Iterable<AccelerationListManager.ReflectionInfo> getReflections() {
      throw new UnsupportedOperationException("getReflections");
    }

    @Override
    public Iterable<ReflectionRPC.RefreshInfo> getRefreshInfos() {
      throw new UnsupportedOperationException("getRefreshInfos");
    }
  };
}
