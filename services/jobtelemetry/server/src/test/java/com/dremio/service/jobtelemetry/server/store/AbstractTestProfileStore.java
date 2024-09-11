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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.google.protobuf.Message;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests the profile store. */
public abstract class AbstractTestProfileStore {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AbstractTestProfileStore.class);

  private ProfileStore profileStore;

  @Before
  public void setUp() throws Exception {
    profileStore = getProfileStore();
    profileStore.start();
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(profileStore);
  }

  protected abstract ProfileStore getProfileStore() throws Exception;

  @Test
  public void testPlanningProfile() {
    final UserBitShared.QueryId queryId =
        UserBitShared.QueryId.newBuilder().setPart1(1020).setPart2(2000).build();

    UserBitShared.QueryProfile planningProfile =
        UserBitShared.QueryProfile.newBuilder()
            .setPlan("PLAN_VALUE")
            .setQuery("Select * from plan")
            .setCancelReason("Cancel plan")
            .setState(UserBitShared.QueryResult.QueryState.COMPLETED)
            .build();

    profileStore.putPlanningProfile(queryId, planningProfile);
    assertEquals(planningProfile, profileStore.getPlanningProfile(queryId).get());

    // overwrite and verify.
    planningProfile = planningProfile.toBuilder().setPlanningEnd(100).build();
    profileStore.putPlanningProfile(queryId, planningProfile);
    assertEquals(planningProfile, profileStore.getPlanningProfile(queryId).get());
  }

  @Test
  public void testPlanningProfileNonExistent() {
    final UserBitShared.QueryId queryId =
        UserBitShared.QueryId.newBuilder().setPart1(1020).setPart2(0xffff).build();

    assertFalse(profileStore.getPlanningProfile(queryId).isPresent());
  }

  @Test
  public void testTailProfile() {
    final UserBitShared.QueryId queryId =
        UserBitShared.QueryId.newBuilder().setPart1(1020).setPart2(2010).build();

    UserBitShared.QueryProfile tailProfile =
        UserBitShared.QueryProfile.newBuilder()
            .setErrorNode("ERROR_NODE")
            .setCancelReason("Cancel tail")
            .build();

    profileStore.putTailProfile(queryId, tailProfile);
    assertEquals(tailProfile, profileStore.getTailProfile(queryId).get());

    // overwrite and verify.
    tailProfile =
        UserBitShared.QueryProfile.newBuilder()
            .setErrorNode("ERROR_NODE")
            .setCancelReason("Some other reason")
            .build();

    profileStore.putTailProfile(queryId, tailProfile);
    assertEquals(tailProfile, profileStore.getTailProfile(queryId).get());
  }

  @Test
  public void testTailProfileNonExistent() {
    final UserBitShared.QueryId queryId =
        UserBitShared.QueryId.newBuilder().setPart1(1020).setPart2(0xffff).build();

    assertFalse(profileStore.getTailProfile(queryId).isPresent());
  }

  @Test
  public void testFullProfile() {
    final UserBitShared.QueryId queryId =
        UserBitShared.QueryId.newBuilder().setPart1(1020).setPart2(2020).build();

    UserBitShared.QueryProfile fullProfile =
        UserBitShared.QueryProfile.newBuilder()
            .setPlan("PLAN_VALUE")
            .setQuery("Select * from plan")
            .setCancelReason("Cancel plan")
            .setState(UserBitShared.QueryResult.QueryState.COMPLETED)
            .build();

    profileStore.putFullProfile(queryId, fullProfile);
    assertEquals(fullProfile, profileStore.getFullProfile(queryId).get());

    // overwrite and verify.
    fullProfile = fullProfile.toBuilder().setPlanningEnd(100).build();
    profileStore.putFullProfile(queryId, fullProfile);
    assertEquals(fullProfile, profileStore.getFullProfile(queryId).get());
  }

  @Test
  public void testFullProfileNonExistent() {
    final UserBitShared.QueryId queryId =
        UserBitShared.QueryId.newBuilder().setPart1(1020).setPart2(0xffff).build();

    assertFalse(profileStore.getFullProfile(queryId).isPresent());
  }

  @Test
  public void testExecutorProfile() {
    final UserBitShared.QueryId queryId =
        UserBitShared.QueryId.newBuilder().setPart1(1020).setPart2(2030).build();
    final CoordinationProtos.NodeEndpoint e1 =
        CoordinationProtos.NodeEndpoint.newBuilder().setAddress("10.10.20.20").build();
    final CoordinationProtos.NodeEndpoint e2 =
        CoordinationProtos.NodeEndpoint.newBuilder().setAddress("10.10.20.21").build();

    CoordExecRPC.ExecutorQueryProfile executorQueryProfile1 =
        CoordExecRPC.ExecutorQueryProfile.newBuilder().build();

    CoordExecRPC.ExecutorQueryProfile executorQueryProfile2 =
        CoordExecRPC.ExecutorQueryProfile.newBuilder().build();

    // should be empty initially.
    assertEquals(0, profileStore.getAllExecutorProfiles(queryId).count());

    // add one executor profile, should return 1 profile.
    profileStore.putExecutorProfile(queryId, e1, executorQueryProfile1, false);
    assertTrue(
        compareUnordered(
            Stream.of(executorQueryProfile1), profileStore.getAllExecutorProfiles(queryId)));

    // add second executor profile, should return 2 profiles.
    profileStore.putExecutorProfile(queryId, e2, executorQueryProfile2, false);
    assertTrue(
        compareUnordered(
            Stream.of(executorQueryProfile1, executorQueryProfile2),
            profileStore.getAllExecutorProfiles(queryId)));

    // overwrite second executor profile, should still return 2 profiles.
    executorQueryProfile2 = CoordExecRPC.ExecutorQueryProfile.newBuilder().build();
    profileStore.putExecutorProfile(queryId, e2, executorQueryProfile2, false);
    assertTrue(
        compareUnordered(
            Stream.of(executorQueryProfile1, executorQueryProfile2),
            profileStore.getAllExecutorProfiles(queryId)));
  }

  private static <T extends Message> boolean compareUnordered(Stream<T> left, Stream<T> right) {
    Set<T> leftSet = left.collect(Collectors.toSet());
    Set<T> rightSet = right.collect(Collectors.toSet());
    return leftSet.containsAll(rightSet) && rightSet.containsAll(leftSet);
  }

  @Test
  public void testDelete() {
    final UserBitShared.QueryId queryId =
        UserBitShared.QueryId.newBuilder().setPart1(1020).setPart2(2050).build();

    // write planning profile
    UserBitShared.QueryProfile planningProfile =
        UserBitShared.QueryProfile.newBuilder()
            .setPlan("PLAN_VALUE")
            .setQuery("Select * from plan")
            .setCancelReason("Cancel plan")
            .setState(UserBitShared.QueryResult.QueryState.COMPLETED)
            .build();
    profileStore.putPlanningProfile(queryId, planningProfile);
    assertEquals(planningProfile, profileStore.getPlanningProfile(queryId).get());

    // write tail profile
    UserBitShared.QueryProfile tailProfile = planningProfile;
    profileStore.putTailProfile(queryId, tailProfile);
    assertEquals(tailProfile, profileStore.getTailProfile(queryId).get());

    // write full profile.
    UserBitShared.QueryProfile fullProfile = planningProfile;
    profileStore.putFullProfile(queryId, fullProfile);
    assertEquals(fullProfile, profileStore.getFullProfile(queryId).get());

    // write executor profiles.
    final CoordinationProtos.NodeEndpoint e1 =
        CoordinationProtos.NodeEndpoint.newBuilder().setAddress("10.10.20.20").build();
    final CoordinationProtos.NodeEndpoint e2 =
        CoordinationProtos.NodeEndpoint.newBuilder().setAddress("10.10.20.21").build();

    // write two executor profiles.
    CoordExecRPC.ExecutorQueryProfile executorQueryProfile =
        CoordExecRPC.ExecutorQueryProfile.newBuilder().build();
    profileStore.putExecutorProfile(queryId, e1, executorQueryProfile, false);
    profileStore.putExecutorProfile(queryId, e2, executorQueryProfile, false);

    // delete and verify.
    profileStore.deleteSubProfiles(queryId);
    assertFalse(profileStore.getPlanningProfile(queryId).isPresent());
    assertFalse(profileStore.getTailProfile(queryId).isPresent());
    assertEquals(0, profileStore.getAllExecutorProfiles(queryId).count());
    assertTrue(profileStore.getFullProfile(queryId).isPresent());

    profileStore.deleteProfile(queryId);
    assertFalse(profileStore.getFullProfile(queryId).isPresent());
  }
}
