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
package com.dremio.service.sqlrunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.options.OptionManager;
import com.dremio.service.script.ScriptService;
import com.dremio.service.script.proto.ScriptProto;
import com.dremio.service.sqlrunner.store.SQLRunnerSessionStore;
import com.dremio.service.sqlrunner.store.SQLRunnerSessionStoreImpl;
import com.google.common.collect.Lists;

/**
 * Test class for SQLRunnerSessionServiceImpl.
 */
public class TestSQLRunnerSessionServiceImpl {

  private static final SabotConfig DEFAULT_SABOT_CONFIG = SabotConfig.forClient();
  private static final ScanResult
    CLASSPATH_SCAN_RESULT = ClassPathScanner.fromPrescan(DEFAULT_SABOT_CONFIG);
  private static String USER_ID = UUID.randomUUID().toString();

  private LocalKVStoreProvider kvStoreProvider;
  private SQLRunnerSessionStore sqlRunnerSessionStore;
  private SQLRunnerSessionService sqlRunnerSessionService;

  @Before
  public void setUp() throws Exception {
    kvStoreProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();

    sqlRunnerSessionStore = new SQLRunnerSessionStoreImpl(() -> kvStoreProvider);
    sqlRunnerSessionStore.start();

    OptionManager mockOptionManager = Mockito.mock(OptionManager.class);
    when(mockOptionManager.getOption(SQLRunnerOptions.SQLRUNNER_TABS)).thenReturn(true);

    ScriptService scriptService = mock(ScriptService.class);
    when(scriptService.getScripts(anyInt(), anyInt(), anyString(), anyString(), anyString(), any())).thenReturn(
      Lists.newArrayList(
        ScriptProto.Script.newBuilder().setScriptId("script_id1").build(),
        ScriptProto.Script.newBuilder().setScriptId("script_id2").build(),
        ScriptProto.Script.newBuilder().setScriptId("script_id3").build(),
        ScriptProto.Script.newBuilder().setScriptId("script_id4").build())
    );

    sqlRunnerSessionService = new SQLRunnerSessionServiceImpl(() -> sqlRunnerSessionStore, () -> mockOptionManager, () -> scriptService);
  }

  @Test
  public void testUpdate() {
    SQLRunnerSession session1 = new SQLRunnerSession(USER_ID, Lists.newArrayList("script_id1", "script_id2"),
      "script_id1");

    sqlRunnerSessionService.updateSession(session1);
    SQLRunnerSession session1a =  sqlRunnerSessionService.getSession(USER_ID);
    assertEquals(USER_ID, session1a.getUserId());
    assertEquals(2, session1a.getScriptIds().size());
    assertEquals("script_id1", session1a.getCurrentScriptId());

    SQLRunnerSession session2 = new SQLRunnerSession(USER_ID,
      Lists.newArrayList("script_id1", "script_id2", "script_id3", "script_id4"),
      "script_id2");

    sqlRunnerSessionService.updateSession(session2);
    SQLRunnerSession session2a =  sqlRunnerSessionService.getSession(USER_ID);
    assertEquals(USER_ID, session2a.getUserId());
    assertEquals(4, session2a.getScriptIds().size());
    assertEquals("script_id2", session2a.getCurrentScriptId());
  }

  @Test
  public void testDelete() {
    SQLRunnerSession oldSession = new SQLRunnerSession(
      USER_ID,
      Lists.newArrayList("script_id1", "script_id2"),
      "script_id1");

    sqlRunnerSessionService.updateSession(oldSession);
    sqlRunnerSessionService.deleteSession(USER_ID);

    SQLRunnerSession newSession = sqlRunnerSessionService.getSession(USER_ID);
    assertEquals(USER_ID, newSession.getUserId());
    assertEquals(0, newSession.getScriptIds().size());
    assertTrue(newSession.getCurrentScriptId().isEmpty());
  }
}
