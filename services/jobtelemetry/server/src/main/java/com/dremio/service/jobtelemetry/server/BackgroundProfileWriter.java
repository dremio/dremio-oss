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

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.rpc.CloseableThreadPool;
import com.dremio.service.jobtelemetry.server.store.ProfileStore;

/**
 * Background writer for merged profiles.
 * - Enforces a bound on the number of in-progress jobs.
 * - Ensures that two jobs do not attempt to write to the same path (can still happen
 *   across JTS instances).
 */
public class BackgroundProfileWriter implements AutoCloseable {
  static final int MAX_BACKGROUND_WRITES = 100;

  private final CloseableThreadPool executor = new CloseableThreadPool("bg-profile-writer");
  private final Set<UserBitShared.QueryId> inProgressWrites = ConcurrentHashMap.newKeySet();
  private final ProfileStore profileStore;

  BackgroundProfileWriter(ProfileStore profileStore) {
    this.profileStore = profileStore;
  }

  Optional<CompletableFuture<Void>> tryWriteAsync(UserBitShared.QueryId queryId,
                                                  UserBitShared.QueryProfile profile) {
    if (inProgressWrites.size() + 1 > MAX_BACKGROUND_WRITES) {
      // too much backlog
      return Optional.empty();
    }

    if (!inProgressWrites.add(queryId)) {
      // there is another in-progress write for the same query.
      return Optional.empty();
    }

    CompletableFuture<Void> future = CompletableFuture.runAsync(
      () -> profileStore.putFullProfile(queryId, profile), executor)
      .whenComplete((ret, ex) -> {
        inProgressWrites.remove(queryId);
      });
    return Optional.of(future);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(executor);
  }
}
