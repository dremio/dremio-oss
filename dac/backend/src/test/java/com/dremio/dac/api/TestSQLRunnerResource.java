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
package com.dremio.dac.api;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.dac.model.scripts.ScriptData;
import com.dremio.dac.model.sqlrunner.SQLRunnerSessionJson;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.sqlrunner.proto.SQLRunnerSessionProto;
import com.dremio.service.sqlrunner.store.SQLRunnerSessionStore;
import com.dremio.service.sqlrunner.store.SQLRunnerSessionStoreImpl;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserService;
import com.google.common.collect.Lists;
import java.util.UUID;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

/** SQLRunnerResource tests */
public class TestSQLRunnerResource extends BaseTestServer {
  private static final String SESSION_PATH = "/sql-runner/session";
  private static final String TABS_PATH = "/sql-runner/session/tabs";
  private static final String SCRIPTS_PATH = "/scripts";
  private static final SabotConfig DEFAULT_SABOT_CONFIG = SabotConfig.forClient();
  private static final ScanResult CLASSPATH_SCAN_RESULT =
      ClassPathScanner.fromPrescan(DEFAULT_SABOT_CONFIG);

  private static LocalKVStoreProvider kvStoreProvider;
  private static SQLRunnerSessionStore sqlRunnerSessionStore;

  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();

    kvStoreProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();

    sqlRunnerSessionStore = new SQLRunnerSessionStoreImpl(() -> kvStoreProvider);
    sqlRunnerSessionStore.start();
  }

  @Test
  public void testGetSQLRunnerSessionWhenNoSession() throws Exception {
    createUser("userA");
    login("userA", "dremio123");

    SQLRunnerSessionJson session =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), session.getUserId());
    assertTrue(session.getScriptIds().isEmpty());
    assertTrue(session.getCurrentScriptId().isEmpty());
  }

  @Test
  public void testGetSQLRunnerSessionWhenSessionExpired() throws Exception {
    createUser("userB");
    login("userB", "dremio123");

    String scriptId1 = createScript();
    String scriptId2 = createScript();

    SQLRunnerSessionProto.SQLRunnerSession expiredSession =
        SQLRunnerSessionProto.SQLRunnerSession.newBuilder()
            .setUserId(this.getUls().getUserId())
            .addAllScriptIds(Lists.newArrayList(scriptId1, scriptId2))
            .setCurrentScriptId(scriptId1)
            .setTtlExpireAt(System.currentTimeMillis() - 1)
            .build();
    sqlRunnerSessionStore.update(expiredSession);

    SQLRunnerSessionJson session =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), session.getUserId());
    assertTrue(session.getScriptIds().isEmpty());
    assertTrue(session.getCurrentScriptId().isEmpty());
  }

  @Test
  public void testDeletedScripts() throws Exception {
    createUser("userF");
    login("userF", "dremio123");

    SQLRunnerSessionJson session =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), session.getUserId());
    assertTrue(session.getScriptIds().isEmpty());
    assertTrue(session.getCurrentScriptId().isEmpty());

    String scriptId1 = createScript();
    String scriptId2 = createScript();

    SQLRunnerSessionJson changedSession =
        new SQLRunnerSessionJson(
            null, Lists.newArrayList(scriptId1, scriptId2, "script_deleted"), "script_deleted");
    expectSuccess(
        getBuilder(getAPIv2().path(SESSION_PATH)).buildPut(Entity.json(changedSession)),
        SQLRunnerSessionJson.class);

    SQLRunnerSessionJson newSession =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), newSession.getUserId());
    assertEquals(2, newSession.getScriptIds().size());
    assertEquals(scriptId1, newSession.getCurrentScriptId());
  }

  @Test
  public void testPutSQLRunnerSession() throws Exception {
    createUser("userC");
    login("userC", "dremio123");

    SQLRunnerSessionJson session =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), session.getUserId());
    assertTrue(session.getScriptIds().isEmpty());
    assertTrue(session.getCurrentScriptId().isEmpty());

    String scriptId1 = createScript();
    String scriptId2 = createScript();
    String scriptId3 = createScript();
    String scriptId4 = createScript();

    SQLRunnerSessionJson changedSession =
        new SQLRunnerSessionJson(
            null, Lists.newArrayList(scriptId1, scriptId2, scriptId3), scriptId3);
    expectSuccess(
        getBuilder(getAPIv2().path(SESSION_PATH)).buildPut(Entity.json(changedSession)),
        SQLRunnerSessionJson.class);

    SQLRunnerSessionJson newSession =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), newSession.getUserId());
    assertEquals(3, newSession.getScriptIds().size());
    assertEquals(scriptId3, newSession.getCurrentScriptId());

    SQLRunnerSessionJson changedSession1 =
        new SQLRunnerSessionJson(
            null, Lists.newArrayList(scriptId1, scriptId2, scriptId3, scriptId4), scriptId4);
    expectSuccess(
        getBuilder(getAPIv2().path(SESSION_PATH)).buildPut(Entity.json(changedSession1)),
        SQLRunnerSessionJson.class);

    SQLRunnerSessionJson newSession1 =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), newSession1.getUserId());
    assertEquals(4, newSession1.getScriptIds().size());
    assertEquals(scriptId4, newSession1.getCurrentScriptId());
  }

  @Test
  public void testAddSQLRunnerTab() throws Exception {
    createUser("userD");
    login("userD", "dremio123");

    SQLRunnerSessionJson session =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), session.getUserId());
    assertTrue(session.getScriptIds().isEmpty());
    assertTrue(session.getCurrentScriptId().isEmpty());

    String scriptId1 = createScript();
    expectSuccess(
        getBuilder(getAPIv2().path(TABS_PATH).path(scriptId1)).buildPut(Entity.json("{}")),
        SQLRunnerSessionJson.class);

    SQLRunnerSessionJson newSession =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), newSession.getUserId());
    assertEquals(1, newSession.getScriptIds().size());
    assertTrue(newSession.getScriptIds().contains(scriptId1));
    assertEquals(scriptId1, newSession.getCurrentScriptId());

    String scriptId2 = createScript();
    expectSuccess(
        getBuilder(getAPIv2().path(TABS_PATH).path(scriptId2)).buildPut(Entity.json("{}")),
        SQLRunnerSessionJson.class);

    newSession =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), newSession.getUserId());
    assertEquals(2, newSession.getScriptIds().size());
    assertTrue(newSession.getScriptIds().contains(scriptId1));
    assertEquals(scriptId2, newSession.getCurrentScriptId());

    // Add the same script one more time, should still have only 2 tabs
    expectSuccess(
        getBuilder(getAPIv2().path(TABS_PATH).path(scriptId2)).buildPut(Entity.json("{}")),
        SQLRunnerSessionJson.class);
    newSession =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), newSession.getUserId());
    assertEquals(2, newSession.getScriptIds().size());
    assertEquals(scriptId2, newSession.getCurrentScriptId());
  }

  @Test
  public void testDeleteSQLRunnerTab() throws Exception {
    createUser("userE");
    login("userE", "dremio123");

    SQLRunnerSessionJson session =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), session.getUserId());
    assertTrue(session.getScriptIds().isEmpty());
    assertTrue(session.getCurrentScriptId().isEmpty());

    String scriptId1 = createScript();
    expectSuccess(
        getBuilder(getAPIv2().path(TABS_PATH).path(scriptId1)).buildPut(Entity.json("{}")),
        SQLRunnerSessionJson.class);
    expectError(
        FamilyExpectation.CLIENT_ERROR,
        getBuilder(getAPIv2().path(TABS_PATH).path(scriptId1)).buildDelete(),
        UserExceptionMapper.ErrorMessageWithContext.class);

    String scriptId2 = createScript();
    expectSuccess(
        getBuilder(getAPIv2().path(TABS_PATH).path(scriptId2)).buildPut(Entity.json("{}")),
        SQLRunnerSessionJson.class);

    // Delete non-existing tab
    final String scriptId3 = UUID.randomUUID().toString();
    Response resp =
        expectSuccess(getBuilder(getAPIv2().path(TABS_PATH).path(scriptId3)).buildDelete());
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), resp.getStatus());

    resp = expectSuccess(getBuilder(getAPIv2().path(TABS_PATH).path(scriptId1)).buildDelete());
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), resp.getStatus());

    SQLRunnerSessionJson newSession =
        expectSuccess(
            getBuilder(getAPIv2().path(SESSION_PATH)).buildGet(), SQLRunnerSessionJson.class);
    assertEquals(this.getUls().getUserId(), newSession.getUserId());
    assertEquals(1, newSession.getScriptIds().size());
    assertFalse(newSession.getScriptIds().contains(scriptId1));
    assertNotEquals(scriptId1, newSession.getCurrentScriptId());
  }

  private void createUser(String userName) throws Exception {
    final UserService userService = getUserService();
    final User user =
        SimpleUser.newBuilder()
            .setUserName(userName)
            .setEmail(userName + "@dremio.test")
            .setFirstName(userName)
            .setLastName("dremio")
            .setCreatedAt(1234567889)
            .setModifiedAt(987654321)
            .build();

    RequestContext.current()
        .with(UserContext.CTX_KEY, UserContext.SYSTEM_USER_CONTEXT)
        .call(
            () -> {
              User userA = userService.createUser(user, "dremio123");
              return null;
            });
  }

  private String createScript() {
    ScriptData scriptData =
        new ScriptData(
            "",
            "",
            RandomStringUtils.randomAlphabetic(10),
            System.currentTimeMillis(),
            null,
            "",
            System.currentTimeMillis(),
            null,
            Lists.newArrayList(),
            Lists.newArrayList(),
            "select * from abc",
            Lists.newArrayList(),
            Lists.newArrayList());

    ScriptData createdScript =
        expectSuccess(
            getBuilder(getAPIv2().path(SCRIPTS_PATH)).buildPost(Entity.json(scriptData)),
            ScriptData.class);
    return createdScript.getId();
  }
}
