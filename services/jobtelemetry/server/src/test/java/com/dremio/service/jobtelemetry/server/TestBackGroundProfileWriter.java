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
package com.dremio.service.jobtelemetry.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobtelemetry.server.store.LocalProfileStore;
import com.dremio.service.jobtelemetry.server.store.ProfileStore;

import io.opentracing.noop.NoopTracerFactory;

/**
 * Tests background profile writer.
 */
public class TestBackGroundProfileWriter {
  private LegacyKVStoreProvider kvStoreProvider;

  @Before
  public void setUp() throws Exception {
    kvStoreProvider = TempLegacyKVStoreProviderCreator.create();
  }

  @Test
  public void testWrite() throws Exception {
    final UserBitShared.QueryId queryId = UserBitShared.QueryId.newBuilder()
      .setPart1(1050)
      .setPart2(2000)
      .build();

    final ProfileStore profileStore = new LocalProfileStore(kvStoreProvider);
    profileStore.start();

    final BackgroundProfileWriter backgroundProfileWriter =
      new BackgroundProfileWriter(profileStore, NoopTracerFactory.create());

    UserBitShared.QueryProfile profile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setCancelReason("Cancel plan")
        .setState(UserBitShared.QueryResult.QueryState.COMPLETED)
        .build();

    // shouldn't be present in the profile store.
    assertFalse(profileStore.getFullProfile(queryId).isPresent());

    // fire a background write.
    Optional<CompletableFuture<Void>> future = backgroundProfileWriter
      .tryWriteAsync(queryId, profile);

    // wait for write to complete.
    future.get().get();

    // verify store has the profile.
    assertEquals(profile, profileStore.getFullProfile(queryId).get());

    AutoCloseables.close(backgroundProfileWriter, profileStore);
  }

  @Test
  public void testConflicts() throws Exception {
    final UserBitShared.QueryId queryId = UserBitShared.QueryId.newBuilder()
      .setPart1(1050)
      .setPart2(2010)
      .build();

    final CountDownLatch latch = new CountDownLatch(1);
    final ProfileStore profileStore = new ProfileStoreWithLatch(
      new LocalProfileStore(kvStoreProvider), latch);
    profileStore.start();

    final BackgroundProfileWriter backgroundProfileWriter =
      new BackgroundProfileWriter(profileStore, NoopTracerFactory.create());

    UserBitShared.QueryProfile profile =
      UserBitShared.QueryProfile.newBuilder()
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.COMPLETED)
        .build();

    // fire a background write.
    Optional<CompletableFuture<Void>> future1 = backgroundProfileWriter
      .tryWriteAsync(queryId, profile);

    Optional<CompletableFuture<Void>> future2 = backgroundProfileWriter
      .tryWriteAsync(queryId, profile);
    assertFalse(future2.isPresent());

    // wait for write to complete.
    latch.countDown();
    future1.get().get();

    // verify store has the profile.
    assertEquals(profile, profileStore.getFullProfile(queryId).get());
    profileStore.close();
  }

  @Test
  public void testBacklog() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final ProfileStore profileStore = new ProfileStoreWithLatch(
      new LocalProfileStore(kvStoreProvider), latch);
    final BackgroundProfileWriter backgroundProfileWriter =
      new BackgroundProfileWriter(profileStore, NoopTracerFactory.create());

    profileStore.start();
    UserBitShared.QueryProfile profile =
      UserBitShared.QueryProfile.newBuilder()
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.COMPLETED)
        .build();

    // fire background writes up to the limit.
    List<CompletableFuture> futureList = new ArrayList<>();
    for (int i = 0; i < BackgroundProfileWriter.MAX_BACKGROUND_WRITES; ++i) {
      final UserBitShared.QueryId queryId = UserBitShared.QueryId.newBuilder()
        .setPart1(1050)
        .setPart2(3000 + i)
        .build();

      Optional<CompletableFuture<Void>> future = backgroundProfileWriter
        .tryWriteAsync(queryId, profile);
      assertTrue(future.isPresent());
      futureList.add(future.get());
    }

    // no writes should pile up after the limit.
    final UserBitShared.QueryId queryIdExtra = UserBitShared.QueryId.newBuilder()
      .setPart1(1050)
      .setPart2(4000)
      .build();

    Optional<CompletableFuture<Void>> future = backgroundProfileWriter
      .tryWriteAsync(queryIdExtra, profile);
    assertFalse(future.isPresent());

    // wait for back log to finish.
    latch.countDown();
    CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()])).get();

    // now, background writes should proceed.
    Optional<CompletableFuture<Void>> futurePostBacklog = backgroundProfileWriter
      .tryWriteAsync(queryIdExtra, profile);
    assertTrue(futurePostBacklog.isPresent());
    futurePostBacklog.get();
    profileStore.close();
  }

  /**
   * decorator to induce artificial delay in writing profiles.
   */
  private static class ProfileStoreWithLatch implements ProfileStore {
    private ProfileStore inner;
    private CountDownLatch latch;

    ProfileStoreWithLatch(ProfileStore store, CountDownLatch latch) {
      this.inner = store;
      this.latch = latch;
    }

    @Override
    public void start() throws Exception {
      inner.start();
    }

    @Override
    public void putFullProfile(UserBitShared.QueryId queryId,
                               UserBitShared.QueryProfile planningProfile) {
      try {
        latch.await();
        inner.putFullProfile(queryId, planningProfile);
      } catch (InterruptedException e) {
      }
    }

    @Override
    public Optional<UserBitShared.QueryProfile> getFullProfile(UserBitShared.QueryId queryId) {
      return inner.getFullProfile(queryId);
    }

    @Override
    public void putPlanningProfile(UserBitShared.QueryId queryId,
                                   UserBitShared.QueryProfile planningProfile) {
      throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public Optional<UserBitShared.QueryProfile> getPlanningProfile(UserBitShared.QueryId queryId) {
      throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public void putTailProfile(UserBitShared.QueryId queryId,
                               UserBitShared.QueryProfile profile) {
      throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public Optional<UserBitShared.QueryProfile> getTailProfile(UserBitShared.QueryId queryId) {
      throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public void putExecutorProfile(UserBitShared.QueryId queryId,
                                   CoordinationProtos.NodeEndpoint endpoint,
                                   CoordExecRPC.ExecutorQueryProfile profile,
                                   boolean isFinal) {
      throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public Stream<CoordExecRPC.ExecutorQueryProfile> getAllExecutorProfiles(UserBitShared.QueryId queryId) {
      throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public void deleteSubProfiles(UserBitShared.QueryId queryId) {
      throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public void deleteProfile(UserBitShared.QueryId queryId) {
      throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public void close() throws Exception {
      inner.close();
    }
  }
}
