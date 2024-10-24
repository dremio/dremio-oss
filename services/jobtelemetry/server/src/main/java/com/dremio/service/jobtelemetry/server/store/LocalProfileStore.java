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

import com.dremio.common.util.Retryer;
import com.dremio.common.utils.ProfilesPathUtils;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.ImmutableFindByRange;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.options.ImmutableVersionOption;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobtelemetry.IntermediateQueryProfile;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of profile store, keeps all profiles in local kvstore. */
public class LocalProfileStore implements ProfileStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalProfileStore.class);

  private static final byte[] MAX_UTF8_VALUE =
      new byte[] {(byte) 0xef, (byte) 0xbf, (byte) 0xbf}; // U+FFFF

  // TODO: switch to "profiles" after removing this store in LocalJobsService

  private final KVStoreProvider kvStoreProvider;

  private KVStore<AttemptId, UserBitShared.QueryProfile> fullProfileStore;

  private KVStore<String, IntermediateQueryProfile> intermediateProfileStore;

  private final Retryer retryer =
      Retryer.newBuilder().retryIfExceptionOfType(RuntimeException.class).build();

  // To ensure we don't create sub-profiles after a query has terminated,
  // as in DX-30198, where we have seen queries take more than 5 minutes to cancel.
  // To be on the safe side and prevent memory leaks, retaining deleted Query ID's
  // for 10 minutes after last access.

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  private Cache<UserBitShared.QueryId, Boolean> deletedQueryIds =
      CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();

  public LocalProfileStore(KVStoreProvider kvStoreProvider) {
    this.kvStoreProvider = kvStoreProvider;
  }

  @Override
  public void start() throws Exception {
    fullProfileStore =
        kvStoreProvider.getStore(LocalProfileKVStoreCreator.KVProfileStoreCreator.class);
    intermediateProfileStore =
        kvStoreProvider.getStore(LocalProfileKVStoreCreator.IntermediateProfileStoreCreator.class);
  }

  @Override
  public void putPlanningProfile(
      UserBitShared.QueryId queryId, UserBitShared.QueryProfile profile) {
    if (deletedQueryIds.asMap().containsKey(queryId)) {
      return;
    }

    String path = ProfilesPathUtils.buildPlanningProfilePath(queryId);
    IntermediateQueryProfile intermediateQueryProfile =
        IntermediateQueryProfile.newBuilder().setPlanningProfile(profile).build();
    putIntermediateProfile(path, intermediateQueryProfile);
  }

  @Override
  public Optional<UserBitShared.QueryProfile> getPlanningProfile(UserBitShared.QueryId queryId) {
    String path = ProfilesPathUtils.buildPlanningProfilePath(queryId);
    Document<String, IntermediateQueryProfile> queryProfile = intermediateProfileStore.get(path);
    return queryProfile == null
        ? Optional.empty()
        : Optional.of(queryProfile.getValue().getPlanningProfile());
  }

  @Override
  public void putTailProfile(UserBitShared.QueryId queryId, UserBitShared.QueryProfile profile) {
    if (deletedQueryIds.asMap().containsKey(queryId)) {
      return;
    }

    String path = ProfilesPathUtils.buildTailProfilePath(queryId);
    IntermediateQueryProfile intermediateQueryProfile =
        IntermediateQueryProfile.newBuilder().setTailProfile(profile).build();
    putIntermediateProfile(path, intermediateQueryProfile);
  }

  @Override
  public Optional<UserBitShared.QueryProfile> getTailProfile(UserBitShared.QueryId queryId) {
    String path = ProfilesPathUtils.buildTailProfilePath(queryId);
    Document<String, IntermediateQueryProfile> queryProfile = intermediateProfileStore.get(path);
    return queryProfile == null
        ? Optional.empty()
        : Optional.of(queryProfile.getValue().getTailProfile());
  }

  @Override
  public void putFullProfile(UserBitShared.QueryId queryId, UserBitShared.QueryProfile profile) {
    fullProfileStore.put(AttemptId.of(queryId), profile);
  }

  @Override
  public Optional<UserBitShared.QueryProfile> getFullProfile(UserBitShared.QueryId queryId) {
    Document<AttemptId, UserBitShared.QueryProfile> profile =
        fullProfileStore.get(AttemptId.of(queryId));
    return profile == null ? Optional.empty() : Optional.of(profile.getValue());
  }

  @Override
  public void putExecutorProfile(
      UserBitShared.QueryId queryId,
      CoordinationProtos.NodeEndpoint endpoint,
      CoordExecRPC.ExecutorQueryProfile profile,
      boolean isFinal) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Updating profile store for query id {}", QueryIdHelper.getQueryId(queryId));
    }

    if (deletedQueryIds.asMap().containsKey(queryId)) {
      return;
    }

    String path = ProfilesPathUtils.buildExecutorProfilePath(queryId, endpoint, isFinal);
    IntermediateQueryProfile intermediateQueryProfile =
        IntermediateQueryProfile.newBuilder().setExecutorProfile(profile).build();
    putIntermediateProfile(path, intermediateQueryProfile);
  }

  @Override
  public Stream<CoordExecRPC.ExecutorQueryProfile> getAllExecutorProfiles(
      UserBitShared.QueryId queryId) {

    String start = ProfilesPathUtils.buildExecutorProfilePrefix(queryId);
    String end = buildRangeEndKey(start);
    FindByRange<String> range =
        new ImmutableFindByRange.Builder<String>()
            .setStart(start)
            .setIsStartInclusive(false)
            .setEnd(end)
            .setIsEndInclusive(false)
            .build();
    try {
      return this.retryer.call(
          () -> {
            Iterable<Document<String, IntermediateQueryProfile>> allProfiles =
                intermediateProfileStore.find(range);

            Map<String, CoordExecRPC.ExecutorQueryProfile> filteredProfiles =
                StreamSupport.stream(allProfiles.spliterator(), false)
                    .filter(
                        it -> it.getKey().endsWith(ProfilesPathUtils.FINAL_EXECUTOR_PROFILE_SUFFIX))
                    .collect(
                        Collectors.toMap(
                            Document::getKey, entry -> entry.getValue().getExecutorProfile()));
            allProfiles.forEach(
                it -> {
                  if (!it.getKey().endsWith(ProfilesPathUtils.FINAL_EXECUTOR_PROFILE_SUFFIX)) {
                    if (!filteredProfiles.containsKey(
                        it.getKey() + ProfilesPathUtils.FINAL_EXECUTOR_PROFILE_SUFFIX)) {
                      filteredProfiles.put(it.getKey(), it.getValue().getExecutorProfile());
                    }
                  }
                });
            return filteredProfiles.values().stream();
          });
    } catch (Exception e) {
      LOGGER.warn(
          "Get all executor profile call failed for {}", QueryIdHelper.getQueryId(queryId), e);
    }
    return Stream.empty();
  }

  @Override
  public void deleteSubProfiles(UserBitShared.QueryId queryId) {
    deletedQueryIds.put(queryId, Boolean.TRUE);
    String qId = QueryIdHelper.getQueryId(queryId);
    FindByRange<String> range =
        new ImmutableFindByRange.Builder<String>()
            .setStart(qId)
            .setIsStartInclusive(false)
            .setEnd(buildRangeEndKey(qId))
            .setIsEndInclusive(false)
            .build();

    try {
      this.retryer.run(
          () -> {
            Iterable<Document<String, IntermediateQueryProfile>> temp =
                intermediateProfileStore.find(range);
            temp.forEach(t -> intermediateProfileStore.delete(t.getKey()));
          });
    } catch (Exception e) {
      LOGGER.warn(
          "Not able to delete intermediate profiles for {}", QueryIdHelper.getQueryId(queryId), e);
    }
  }

  @Override
  public void deleteProfile(UserBitShared.QueryId queryId) {
    deleteSubProfiles(queryId);
    fullProfileStore.delete(AttemptId.of(queryId));
  }

  @Override
  public void close() {}

  /**
   * Delete specified old profile.
   *
   * <p>Exposed as static so that cleanup tasks can do this without needing to start a service
   *
   * @param provider kvStore provider.
   * @param attemptId attemptId
   */
  public static void deleteOldProfile(KVStoreProvider provider, AttemptId attemptId) {
    KVStore<AttemptId, UserBitShared.QueryProfile> profileStore =
        provider.getStore(LocalProfileKVStoreCreator.KVProfileStoreCreator.class);
    profileStore.delete(attemptId);
  }

  /**
   * Delete all intermediate profiles.
   *
   * <p>Exposed as static so that cleanup tasks can do this without needing to start a service
   *
   * @param provider kvStore provider.
   */
  public static String deleteAllIntermediateProfiles(KVStoreProvider provider) {
    KVStore<String, IntermediateQueryProfile> store =
        provider.getStore(LocalProfileKVStoreCreator.IntermediateProfileStoreCreator.class);
    Iterator<Document<String, IntermediateQueryProfile>> temp = store.find().iterator();
    long count = 0;
    while (temp.hasNext()) {
      store.delete(temp.next().getKey());
      count++;
    }
    StringBuilder sb = new StringBuilder("Deleted total ");
    sb.append(count).append(" intermediate profiles");
    return sb.toString();
  }

  private static String buildRangeEndKey(String rangeStartKey) {
    return rangeStartKey + new String(MAX_UTF8_VALUE, StandardCharsets.UTF_8);
  }

  private void putIntermediateProfile(
      String path, IntermediateQueryProfile intermediateQueryProfile) {
    Document<String, IntermediateQueryProfile> document = intermediateProfileStore.get(path);
    if (document == null) {
      intermediateProfileStore.put(path, intermediateQueryProfile, KVStore.PutOption.CREATE);
    } else {
      final ImmutableVersionOption versionOption =
          new ImmutableVersionOption.Builder().setTag(document.getTag()).build();
      intermediateProfileStore.put(path, intermediateQueryProfile, versionOption);
    }
  }
}
