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
package com.dremio.service.usersessions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

import org.apache.calcite.avatica.util.Quoting;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.proto.UserSessionProtobuf.UserSessionRPC;
import com.dremio.sabot.rpc.user.UserRpcUtils;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.usersessions.store.UserSessionStoreProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Test class for UserSessionStore.
 */
public class TestUserSessionServiceImpl {

  private UserSession session;
  private UserSessionServiceImpl service;

  @Before
  public void setup() throws Exception {
    // set up the user session
    setupSession();

    // set up the service
    UserSessionStoreProvider provider = new UserSessionStoreProvider();

    service = new UserSessionServiceImpl(() -> provider, () -> 120 * 60);
    provider.start();
    service.start();
  }

  /**
   * Test to ensure inserting a session works correctly
   */
  @Test
  public void testPut() {
    final UserSessionService.SessionIdAndVersion sessionIdAndVersion = service.putSession(session);

    // check that the sessionId returns the correct result
    final UserSessionService.UserSessionAndVersion actualSession = service.getSession(sessionIdAndVersion.getId());
    compareUserSessions(session, actualSession.getSession());
  }

  /**
   * Test to ensure that updating a UserSession in the store respects the TTL and updates the provides the correct value on the next get.
   */
  @Test
  public void testUpdate() {
    final UserSessionService.SessionIdAndVersion sessionIdAndVersion = service.putSession(session);
    final String sessionId = sessionIdAndVersion.getId();

    // modify the session
    session.setEngine("Test");

    // update the session in the service
    service.updateSession(sessionId, sessionIdAndVersion.getVersion(), session);

    // ensure the session is updated
    final UserSessionService.UserSessionAndVersion actualSession = service.getSession(sessionId);
    compareUserSessions(session, actualSession.getSession());
  }

  @Test(expected = ConcurrentModificationException.class)
  public void testUpdateInvalidSessionId() {
    final UserSessionService.SessionIdAndVersion sessionIdAndVersion = service.putSession(session);
    final String sessionId = "not a sessionId";

    session.setEngine("Test");

    service.updateSession(sessionId, sessionIdAndVersion.getVersion(), session);
  }

  @Test
  public void testInvalidSessionId() {
    String invalidSessionId = "not a sessionId";
    assertNull(service.getSession(invalidSessionId));
  }

  @Test
  public void verifyToProtoBuf() throws Exception {
    final UserSessionRPC serialized = GrpcUserSessionConverter.toProtoBuf(session);

    final UserSession newSession = GrpcUserSessionConverter.fromProtoBuf(serialized);
    compareUserSessions(session, newSession);
  }

  @Test
  public void verifyToFromProtoBufEmptySession() throws Exception {
    final UserSession originalSession = UserSession.Builder.newBuilder().build();
    final UserSessionRPC serialized = GrpcUserSessionConverter.toProtoBuf(originalSession);

    final UserSession newSession = GrpcUserSessionConverter.fromProtoBuf(serialized);
    compareUserSessions(originalSession, newSession);
  }

  private void setupSession() {
    final List<String> defaultSchema = ImmutableList.of("schema", "table");
    final UserProtos.UserProperties properties = UserProtos.UserProperties.newBuilder()
      .addProperties(UserProtos.Property.newBuilder().setKey(UserSession.ROUTING_QUEUE).setValue("queue").build())
      .addProperties(UserProtos.Property.newBuilder().setKey(UserSession.ROUTING_TAG).setValue("tag").build())
      .addProperties(UserProtos.Property.newBuilder().setKey(UserSession.ROUTING_ENGINE).setValue("engine").build())
      .addProperties(UserProtos.Property.newBuilder().setKey(UserSession.IMPERSONATION_TARGET).setValue("target").build())
      .addProperties(UserProtos.Property.newBuilder().setKey(UserSession.TRACING_ENABLED).setValue("TRUE").build())
      .build();

    final Map<String, VersionContext> sourceVersionMapping = ImmutableMap.of(
        "test1", VersionContext.ofBranch("branch1"),
        "test2", VersionContext.ofBareCommit("0123456789ABCDEFabcdef"),
        "test3", VersionContext.ofRef("ref1"),
        "test4", VersionContext.ofTag("tag")
      );

    session = UserSession.Builder.newBuilder()
      .exposeInternalSources(true)
      .withClientInfos(UserRpcUtils.getRpcEndpointInfos("Dremio Java local client"))
      .withDefaultSchema(defaultSchema)
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName("foo").build())
      .withEngineName("First Engine")
      .withInitialQuoting(Quoting.BRACKET)
      .withFullyQualifiedProjectsSupport(true)
      .withLegacyCatalog()
      .withRecordBatchFormat(UserProtos.RecordBatchFormat.DREMIO_1_4)
      .setSupportComplexTypes(true)
      .withUserProperties(properties)
      .withSourceVersionMapping(sourceVersionMapping)
      .build();
    session.setLastQueryId(UserBitShared.QueryId.newBuilder().setPart1(1).setPart2(2).build());
  }

  private void compareUserSessions(UserSession session1, UserSession session2) {
    assertEquals(session1.exposeInternalSources(), session2.exposeInternalSources());
    assertEquals(session1.getCatalogName(), session2.getCatalogName());
    assertEquals(session1.getClientInfos(), session2.getClientInfos());
    assertEquals(session1.getCredentials(), session2.getCredentials());
    assertEquals(session1.getDefaultSchemaName(), session2.getDefaultSchemaName());
    assertEquals(session1.getDefaultSchemaPath(), session2.getDefaultSchemaPath());
    assertEquals(session1.getEngine(), session2.getEngine());
    assertEquals(session1.getInitialQuoting(), session2.getInitialQuoting());
    assertEquals(session1.getLastQueryId(), session2.getLastQueryId());
    assertEquals(session1.getMaxMetadataCount(), session2.getMaxMetadataCount());
    assertEquals(session1.getRecordBatchFormat(), session2.getRecordBatchFormat());
    assertEquals(UserProtos.RecordBatchFormat.DREMIO_1_4, session2.getRecordBatchFormat());
    assertEquals(session1.getRoutingEngine(), session2.getRoutingEngine());
    assertEquals(session1.getRoutingQueue(), session2.getRoutingQueue());
    assertEquals(session1.getRoutingTag(), session2.getRoutingTag());
    assertEquals(session1.getSubstitutionSettings().getExclusions(), session2.getSubstitutionSettings().getExclusions());
    assertEquals(session1.getSubstitutionSettings().getInclusions(), session2.getSubstitutionSettings().getInclusions());
    assertEquals(session1.getTargetUserName(), session2.getTargetUserName());
    assertEquals(session1.exposeInternalSources(), session2.exposeInternalSources());
    assertEquals(session1.isSupportComplexTypes(), session2.isSupportComplexTypes());
    assertEquals(session1.isTracingEnabled(), session2.isTracingEnabled());
    assertEquals(session1.supportFullyQualifiedProjections(), session2.supportFullyQualifiedProjections());
    assertEquals(session1.useLegacyCatalogName(), session2.useLegacyCatalogName());

    assertEquals(session1.getSourceVersionMapping().size(), session2.getSourceVersionMapping().size());

    final Map<String, VersionContext> session2SourceVersionMapping = session2.getSourceVersionMapping();
    session1.getSourceVersionMapping()
      .forEach((key, value1) -> {
        final VersionContext value2 = session2SourceVersionMapping.get(key);
        assertEquals(value1, value2);
      });
  }

}
