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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.nodes.EndpointHelper;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.AttemptIdUtils;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Implementation of profile store, keeps all profiles except the full-profile in-memory.
 * The full profile goes to local kvstore.
 */
public class LocalProfileStore implements ProfileStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalProfileStore.class);

  // TODO: switch to "profiles" after removing this store in LocalJobsService
  public static final String PROFILES_NAME = "profiles";

  private final LegacyKVStoreProvider kvStoreProvider;
  private final Map<UserBitShared.QueryId, UserBitShared.QueryProfile> planningProfiles =
    new HashMap<>();
  private final Map<UserBitShared.QueryId, UserBitShared.QueryProfile> tailProfiles = new HashMap<>();
  private final Map<UserBitShared.QueryId, Map<String, CoordExecRPC.ExecutorQueryProfile>> executorMap =
    new HashMap<>();
  private LegacyKVStore<AttemptId, UserBitShared.QueryProfile> fullProfileStore;

  // to ensure we don't save sub-profiles after a query has terminated.
  private Cache<UserBitShared.QueryId, Boolean> deletedQueryIds = CacheBuilder.newBuilder()
    .expireAfterWrite(5, TimeUnit.MINUTES)
    .build();

  public LocalProfileStore(LegacyKVStoreProvider kvStoreProvider) {
    this.kvStoreProvider = kvStoreProvider;
  }

  @Override
  public void start() throws Exception {
    fullProfileStore = kvStoreProvider.getStore(KVProfileStoreCreator.class);
  }

  @Override
  public synchronized void putPlanningProfile(UserBitShared.QueryId queryId,
                                              UserBitShared.QueryProfile profile) {
    if (deletedQueryIds.asMap().containsKey(queryId)) {
      return;
    }
    planningProfiles.put(queryId, profile);
  }

  @Override
  public synchronized Optional<UserBitShared.QueryProfile> getPlanningProfile(UserBitShared.QueryId queryId) {
    return Optional.ofNullable(planningProfiles.get(queryId));
  }

  @Override
  public synchronized void putTailProfile(UserBitShared.QueryId queryId, UserBitShared.QueryProfile profile) {
    if (deletedQueryIds.asMap().containsKey(queryId)) {
      return;
    }
    tailProfiles.put(queryId, profile);
  }

  @Override
  public synchronized Optional<UserBitShared.QueryProfile> getTailProfile(UserBitShared.QueryId queryId) {
    return Optional.ofNullable(tailProfiles.get(queryId));
  }

  @Override
  public void putFullProfile(UserBitShared.QueryId queryId,
                             UserBitShared.QueryProfile profile) {
    fullProfileStore.put(AttemptId.of(queryId), profile);
  }

  @Override
  public Optional<UserBitShared.QueryProfile> getFullProfile(UserBitShared.QueryId queryId) {
    return Optional.ofNullable(fullProfileStore.get(AttemptId.of(queryId)));
  }

  @Override
  public synchronized void putExecutorProfile(UserBitShared.QueryId queryId,
                                              CoordinationProtos.NodeEndpoint endpoint,
                                              CoordExecRPC.ExecutorQueryProfile profile) {
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Updating profile store for query id {}", QueryIdHelper.getQueryId(queryId));
    }

    if (deletedQueryIds.asMap().containsKey(queryId)) {
      return;
    }
    executorMap.compute(queryId,
      (key, value) -> {
        if (value == null)  {
          value = new HashMap<>();
        }
        value.put(EndpointHelper.getMinimalString(endpoint), profile);
        return value;
      });
  }

  @Override
  public synchronized Stream<CoordExecRPC.ExecutorQueryProfile> getAllExecutorProfiles(UserBitShared.QueryId queryId) {
    Map<String, CoordExecRPC.ExecutorQueryProfile> innerMap = executorMap.get(queryId);

    return innerMap == null ? Stream.empty() :
      new ArrayList<>(innerMap.values()).stream();
  }

  @Override
  public synchronized void deleteSubProfiles(UserBitShared.QueryId queryId) {
    deletedQueryIds.put(queryId, Boolean.TRUE);
    planningProfiles.remove(queryId);
    tailProfiles.remove(queryId);
    executorMap.remove(queryId);
  }

  @Override
  public void deleteProfile(UserBitShared.QueryId queryId) {
    deleteSubProfiles(queryId);
    fullProfileStore.delete(AttemptId.of(queryId));
  }

  @Override
  public void close() {
  }

  /**
   * Delete specified old profile.
   *
   * Exposed as static so that cleanup tasks can do this without needing to start a service
   *
   * @param provider kvStore provider.
   * @param attemptId attemptId
   */
  public static void deleteOldProfile(LegacyKVStoreProvider provider,
                                      AttemptId attemptId) {
    LegacyKVStore<AttemptId, UserBitShared.QueryProfile> legacyProfileStore =
      provider.getStore(KVProfileStoreCreator.class);
    legacyProfileStore.delete(attemptId);
  }

  /**
   * Creator for full profiles kvstore.
   */
  public static final class KVProfileStoreCreator implements LegacyKVStoreCreationFunction<AttemptId, UserBitShared.QueryProfile> {
    @Override
    public LegacyKVStore<AttemptId, UserBitShared.QueryProfile> build(LegacyStoreBuildingFactory factory) {
      return factory
        .<AttemptId, UserBitShared.QueryProfile>newStore()
        .name(PROFILES_NAME)
        .keyFormat(Format.wrapped(AttemptId.class, AttemptIdUtils::toString,
          AttemptIdUtils::fromString,
          Format.ofString()))
        .valueFormat(Format.ofProtobuf(UserBitShared.QueryProfile.class))
        .build();
    }
  }
}
