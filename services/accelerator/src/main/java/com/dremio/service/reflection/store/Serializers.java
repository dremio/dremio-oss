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
package com.dremio.service.reflection.store;

import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionDependencies;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshId;
import com.dremio.service.reflection.proto.RefreshRequest;

/**
 * reflection related {@link SchemaSerializer}s
 */
class Serializers {
  /**
   * {@link ReflectionId} serializer
   */
  static class ReflectionIdSerializer extends SchemaSerializer<ReflectionId> {
    ReflectionIdSerializer() {
      super(ReflectionId.getSchema());
    }
  }

  /**
   * {@link ReflectionGoal} serializer
   */
  static class ReflectionGoalSerializer extends SchemaSerializer<ReflectionGoal> {
    ReflectionGoalSerializer() {
      super(ReflectionGoal.getSchema());
    }
  }

  /**
   * {@link ReflectionEntry} serializer
   */
  static class ReflectionEntrySerializer extends SchemaSerializer<ReflectionEntry> {
    ReflectionEntrySerializer() {
      super(ReflectionEntry.getSchema());
    }
  }

  /**
   * {@link MaterializationId} serializer
   */
  static class MaterializationIdSerializer extends SchemaSerializer<MaterializationId> {
    MaterializationIdSerializer() {
      super(MaterializationId.getSchema());
    }
  }

  /**
   * {@link RefreshId} serializer
   */
  static class RefreshIdSerializer extends SchemaSerializer<RefreshId> {
    RefreshIdSerializer() {
      super(RefreshId.getSchema());
    }
  }

  /**
   * {@link Refresh} serializer
   */
  static class RefreshSerializer extends SchemaSerializer<Refresh> {
    RefreshSerializer() {
      super(Refresh.getSchema());
    }
  }

  /**
   * {@link Materialization} serializer
   */
  static class MaterializationSerializer extends SchemaSerializer<Materialization> {
    MaterializationSerializer() {
      super(Materialization.getSchema());
    }
  }

  /**
   * {@link ReflectionDependencies} serializer
   */
  static class ReflectionDependenciesSerializer extends SchemaSerializer<ReflectionDependencies> {
    ReflectionDependenciesSerializer() {
      super(ReflectionDependencies.getSchema());
    }
  }

  /**
   * {@link ExternalReflection} serializer
   */
  static class ExternalReflectionSerializer extends SchemaSerializer<ExternalReflection> {
    public ExternalReflectionSerializer() {
      super(ExternalReflection.getSchema());
    }
  }

  /**
   * {@link RefreshRequest} kvStore serializer
   */
  static class RefreshRequestSerializer extends SchemaSerializer<RefreshRequest> {
    public RefreshRequestSerializer() {
      super(RefreshRequest.getSchema());
    }
  }
}
