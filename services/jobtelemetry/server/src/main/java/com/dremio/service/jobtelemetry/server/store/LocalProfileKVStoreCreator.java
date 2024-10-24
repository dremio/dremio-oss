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
package com.dremio.service.jobtelemetry.server.store;

import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.AttemptIdUtils;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobtelemetry.IntermediateQueryProfile;

public class LocalProfileKVStoreCreator {

  // TODO: switch to "profiles" after removing this store in LocalJobsService
  public static final String PROFILES_NAME = "profiles";

  private static final String INTERMEDIATE_PROFILES = "intermediate_profiles";

  /** Creator for full profiles kvstore. */
  public static final class KVProfileStoreCreator
      implements LegacyKVStoreCreationFunction<AttemptId, UserBitShared.QueryProfile> {
    @Override
    public LegacyKVStore<AttemptId, UserBitShared.QueryProfile> build(
        LegacyStoreBuildingFactory factory) {
      return factory
          .<AttemptId, UserBitShared.QueryProfile>newStore()
          .name(PROFILES_NAME)
          .keyFormat(
              Format.wrapped(
                  AttemptId.class,
                  AttemptIdUtils::toString,
                  AttemptIdUtils::fromString,
                  Format.ofString()))
          .valueFormat(Format.ofProtobuf(UserBitShared.QueryProfile.class))
          .build();
    }
  }

  /** Creator for intermediate profiles kvstore. */
  public static final class IntermediateProfileStoreCreator
      implements KVStoreCreationFunction<String, IntermediateQueryProfile> {
    @Override
    public KVStore<String, IntermediateQueryProfile> build(StoreBuildingFactory factory) {
      return factory
          .<String, IntermediateQueryProfile>newStore()
          .name(INTERMEDIATE_PROFILES)
          .keyFormat(Format.ofString())
          .valueFormat(Format.ofProtobuf(IntermediateQueryProfile.class))
          .build();
    }
  }
}
