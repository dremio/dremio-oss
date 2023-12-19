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

package com.dremio.service.script;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.dac.proto.model.dataset.DatasetProtobuf;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.options.ImmutableVersionOption;
import com.dremio.datastore.api.options.VersionOption;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.script.proto.ScriptProto;
import com.dremio.service.usersessions.UserSessionService;

/**
 * Test for script service
 */
@RunWith(MockitoJUnitRunner.class)
public class TestScriptServiceImpl {

  private static final SabotConfig DEFAULT_SABOT_CONFIG = SabotConfig.forClient();
  private static final ScanResult
    CLASSPATH_SCAN_RESULT = ClassPathScanner.fromPrescan(DEFAULT_SABOT_CONFIG);
  private static final String USER_ID_1 = "87605f99-88da-45b1-8668-09bd0cfa8528";
  private static final String USER_ID_2 = "f7a7c98c-8c44-4691-9b19-e4373a8c38d3";
  private ScriptStore scriptStore;
  private LocalKVStoreProvider kvStoreProvider;
  private ScriptService scriptService;
  private UserSessionService mockUserSessionService;

  @Before
  public void setUp() throws Exception {

    kvStoreProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();

    scriptStore = new ScriptStoreImpl(() -> kvStoreProvider);
    scriptStore.start();

    mockUserSessionService = Mockito.mock(UserSessionService.class);

    // get script service
    scriptService = new ScriptServiceImpl(() -> scriptStore, () -> mockUserSessionService);
    scriptService.start();
  }

  @After
  public void cleanUp() throws Exception {
    kvStoreProvider.close();
    scriptStore.close();
    scriptService.close();
  }

  @Test
  public void testCreateScript()
    throws Exception {
    //-----------------------create----------------------------
    // given script request
    List<DatasetProtobuf.SourceVersionReference> references = new ArrayList<>(Arrays.asList(
      DatasetProtobuf.SourceVersionReference.newBuilder()
        .setSourceName("Source 1")
        .setReference(DatasetProtobuf.VersionContext.newBuilder()
          .setType(DatasetProtobuf.VersionContextType.BRANCH)
          .setValue("dev")
          .build())
        .build(),
      DatasetProtobuf.SourceVersionReference.newBuilder()
        .setSourceName("Source 2")
        .setReference(DatasetProtobuf.VersionContext.newBuilder()
          .setType(DatasetProtobuf.VersionContextType.TAG)
          .setValue("tag_1")
          .build())
        .build(),
      DatasetProtobuf.SourceVersionReference.newBuilder()
        .setSourceName("Source 3")
        .setReference(DatasetProtobuf.VersionContext.newBuilder()
          .setType(DatasetProtobuf.VersionContextType.COMMIT)
          .setValue("932505776779430053766113965c21cfb7ab823a")
          .build())
        .build()
    ));
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("testScript")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a", "b", "c")))
      .addAllReferences(references)
      .setContent("select * from xyz")
      .build();

    // when create request is sent
    ScriptProto.Script script =
      runWithUserContext(USER_ID_1, () -> scriptService.createScript(scriptRequest));

    // assert
    Assert.assertEquals(scriptRequest.getName(), script.getName());
    Assert.assertEquals(USER_ID_1, script.getCreatedBy());
    Assert.assertEquals(scriptRequest.getDescription(), script.getDescription());
    Assert.assertEquals(USER_ID_1, script.getModifiedBy());
    Assert.assertEquals(scriptRequest.getContextList(), script.getContextList());
    Assert.assertEquals(scriptRequest.getReferencesList(), script.getReferencesList());
    Assert.assertEquals(scriptRequest.getContent(), script.getContent());

    //--------------------update-----------------------------

    ScriptProto.ScriptRequest updateRequest = scriptRequest.toBuilder()
      .setName("updatedTestScript")
      .setDescription("updated Description")
      .clearContext()
      .addAllContext(new ArrayList<>(Arrays.asList("updated", "context")))
      .setContent("updated content")
      .build();

    runWithUserContext(USER_ID_1,
                       () -> scriptService.updateScript(script.getScriptId(), updateRequest));

    //-------------------validate content after get----------

    ScriptProto.Script updatedScript =
      runWithUserContext(USER_ID_1, () -> scriptService.getScriptById(script.getScriptId()));

    Assert.assertEquals(updateRequest.getName(), updatedScript.getName());
    Assert.assertEquals(USER_ID_1, updatedScript.getModifiedBy());
    Assert.assertEquals(updateRequest.getContextList(), updatedScript.getContextList());
    Assert.assertEquals(updateRequest.getContent(), updatedScript.getContent());

    //--------------------update context---------------------

    CaseInsensitiveMap<VersionContext> referenceMap = CaseInsensitiveMap.newConcurrentMap();
    referenceMap.put("Source 1", VersionContext.ofTag("new_tag"));
    referenceMap.put("Source 2", VersionContext.ofCommit("9325057"));
    referenceMap.put("Source 3", VersionContext.ofBranch("new_brnch"));

    final List<String> newContext = new ArrayList<>(Arrays.asList("d", "e"));
    final UserSession newSession = UserSession.Builder.newBuilder()
      .withDefaultSchema(newContext)
      .withSourceVersionMapping(referenceMap)
      .build();
    final VersionOption version = new ImmutableVersionOption.Builder().setTag("version").build();
    when(mockUserSessionService.getSession(anyString())).thenReturn(new UserSessionService.UserSessionAndVersion(newSession, version));

    runWithUserContext(USER_ID_1,
      () -> scriptService.updateScriptContext(script.getScriptId(), UUID.randomUUID().toString()));

    //---------------validate context/references-------------

    ScriptProto.Script contextUpdatedScript =
      runWithUserContext(USER_ID_1, () -> scriptService.getScriptById(script.getScriptId()));
    Assert.assertEquals(newContext, contextUpdatedScript.getContextList());
    Assert.assertEquals(SourceVersionReferenceUtils.createSourceVersionReferenceListFromContextMap(referenceMap),
      contextUpdatedScript.getReferencesList());

    //--------------------delete-----------------------------

    runWithUserContext(USER_ID_1, () -> {
      scriptService.deleteScriptById(script.getScriptId());
      return null;
    });

    //----------------------get------------------------------
    // assert get of script raise scriptnot found exception

    assertThatThrownBy(() -> runWithUserContext(USER_ID_1,
                                                () -> scriptService.getScriptById(script.getScriptId())))
      .isInstanceOf(ScriptNotFoundException.class)
      .hasMessage(String.format("Script with id or name : %s not found.",
                                script.getScriptId()));
  }

  // test 2 scripts cannot have same script name
  @Test
  public void testDuplicateScriptName() throws Exception {
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("scriptName")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("test content")
      .build();

    ScriptProto.Script script =
      runWithUserContext(USER_ID_1, () -> scriptService.createScript(scriptRequest));
    assertThatThrownBy(() -> runWithUserContext(USER_ID_1,
                                                () -> scriptService.createScript(scriptRequest)))
      .isInstanceOf(DuplicateScriptNameException.class)
      .hasMessage(
        "Cannot reuse the same script name within a project. Please try another script name.");
  }

  // test 2 scripts created by different users can't have same name
  @Test
  public void testDuplicateNameDifferentUser() throws Exception {
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("scriptName")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("test content")
      .build();

//    ScriptProto.Script script1 = scriptService.createScript(scriptRequest);
    runWithUserContext(USER_ID_1, () -> scriptService.createScript(scriptRequest));

    assertThatThrownBy(() -> runWithUserContext(USER_ID_1,
                                                () -> scriptService.createScript(scriptRequest)))
      .isInstanceOf(DuplicateScriptNameException.class)
      .hasMessage(
        "Cannot reuse the same script name within a project. Please try another script name.");
  }

  // validate max length of name
  @Test
  public void testMaxLengthOfName() {
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName(StringUtils.repeat("*", 129))
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("test content")
      .build();

    assertThatThrownBy(() -> runWithUserContext(USER_ID_1,
                                                () -> scriptService.createScript(scriptRequest)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Maximum 128 characters allowed in script name. You have typed in 129 characters.");
  }

  // validate max length of content
  @Test
  public void testMaxLengthOfContent() {
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("scriptWithVeryLongContent")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent(StringUtils.repeat("*", 250001))
      .build();


    assertThatThrownBy(() -> runWithUserContext(USER_ID_1,
                                                () -> scriptService.createScript(scriptRequest)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Maximum 250000 characters allowed in script content. You have typed in 250001 characters.");
  }

  // validate max length of description
  @Test
  public void testMaxLengthOfDescription() {
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("scriptWithVeryLongDescription")
      .setDescription(StringUtils.repeat("*", 1025))
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("select * from xyz")
      .build();

    assertThatThrownBy(() -> runWithUserContext(USER_ID_1,
                                                () -> scriptService.createScript(scriptRequest)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Maximum 1024 characters allowed in script description. You have typed in 1025 characters.");
  }

  // test a script can be read/update/delete by another user
  @Test
  public void testReadAccess() throws Exception {
    //given script created by user1
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("testReadAccess")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("select * from xyz")
      .build();

    ScriptProto.Script script =
      runWithUserContext(USER_ID_1, () -> scriptService.createScript(scriptRequest));

    // when script is read by user2
    // assert user2 is able to access the script
    ScriptProto.Script fetchedScript = runWithUserContext(USER_ID_2,
                                                          () -> scriptService.getScriptById(script.getScriptId()));

    Assert.assertEquals(USER_ID_1, fetchedScript.getCreatedBy());
  }

  @Test
  public void testUpdateAccess() throws Exception {
    //given script created by user1
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("testUpdateAccess")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("select * from xyz")
      .build();

    ScriptProto.Script script =
      runWithUserContext(USER_ID_1, () -> scriptService.createScript(scriptRequest));

    // when script is updated by user2
    // assert user2 is able to update script
    ScriptProto.Script updatedScript = runWithUserContext(USER_ID_2,
                                        () -> scriptService.updateScript(script.getScriptId(),
                                                                         scriptRequest));

    Assert.assertEquals(USER_ID_2, updatedScript.getModifiedBy());
  }

  @Test
  public void testDeleteAccess() throws Exception {
    //given script created by user1
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("testDeleteAccess")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("select * from xyz")
      .build();

    ScriptProto.Script script =
      runWithUserContext(USER_ID_1, () -> scriptService.createScript(scriptRequest));

    // when script is deleted by user2
    // assert user2 is able to delete
    runWithUserContext(USER_ID_2, () -> {
      scriptService.deleteScriptById(script.getScriptId());
      return null;
    });

    // assert when script is fetched again throws not found exception
    assertThatThrownBy(() ->  runWithUserContext(USER_ID_2, () -> scriptService.getScriptById(script.getScriptId())))
      .isInstanceOf(ScriptNotFoundException.class)
      .hasMessage(String.format("Script with id or name : %s not found.",
                                script.getScriptId()));
  }

  @Test
  public void testSearchOnGetAll() throws Exception {
    //given scripts with name FirstScript, SecondScript and RandomName
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("FirstScript")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("select * from xyz")
      .build();

    ScriptProto.Script script1 =
      runWithUserContext(USER_ID_1, () -> scriptService.createScript(scriptRequest));
    ScriptProto.Script script2 =
      runWithUserContext(USER_ID_1,
                         () -> scriptService.createScript(scriptRequest.toBuilder()
                                                            .setName("SecondScript")
                                                            .build()));
    ScriptProto.Script script3 =
      runWithUserContext(USER_ID_1,
                         () -> scriptService.createScript(scriptRequest.toBuilder()
                                                            .setName("RandomName")
                                                            .build()));

    // when script is searched for name "Script"
    List<ScriptProto.Script> scripts =
      runWithUserContext(USER_ID_1,
                         () -> scriptService.getScripts(0, 1000, "Script", "", "", null));

    // assert 2 scripts are found
    Assert.assertEquals(2, scripts.size());
    Assert.assertTrue(scripts.stream()
                        .map(ScriptProto.Script::getScriptId)
                        .collect(Collectors.toList())
                        .containsAll(Arrays.asList(script1.getScriptId(), script2.getScriptId())));
  }

  // test sort feature
  @Test
  public void testSortOnGetAll()
    throws Exception {
    // given
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("sortScript1")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("select * from xyz")
      .build();

    // create script 1
    ScriptProto.Script script1 =
      runWithUserContext(USER_ID_1, () -> scriptService.createScript(scriptRequest));

    // create script 2
    ScriptProto.Script script2 =
      runWithUserContext(USER_ID_1,
                         () -> scriptService.createScript(scriptRequest.toBuilder()
                                                            .setName("sortScript2")
                                                            .build()));

    // update script 1
    runWithUserContext(USER_ID_1, () -> scriptService.updateScript(script1.getScriptId(),
                                                                   scriptRequest.toBuilder()
                                                                     .setDescription(
                                                                       "updated description")
                                                                     .build()));

    // when  sorted on name
    List<ScriptProto.Script> scripts =
      runWithUserContext(USER_ID_1,
                         () -> scriptService.getScripts(0, 1000, "sortScript", "-name", "", null));
    Assert.assertEquals("sortScript2", scripts.get(0).getName());
    Assert.assertEquals("sortScript1", scripts.get(1).getName());

    // when sorted on createdAt
    scripts = runWithUserContext(USER_ID_1,
                                 () -> scriptService.getScripts(0,
                                                                1000,
                                                                "sortScript",
                                                                "createdAt",
                                                                "",
                                                                null));
    Assert.assertEquals("sortScript1", scripts.get(0).getName());
    Assert.assertEquals("sortScript2", scripts.get(1).getName());
    Assert.assertTrue(scripts.get(0).getCreatedAt() <= scripts.get(1).getCreatedAt());

    // when sorted on -modifiedAt
    scripts = runWithUserContext(USER_ID_1,
                                 () -> scriptService.getScripts(0,
                                                                1000,
                                                                "sortScript",
                                                                "-modifiedAt",
                                                                "",
                                                                null));
    Assert.assertEquals("sortScript1", scripts.get(0).getName());
    Assert.assertEquals("sortScript2", scripts.get(1).getName());
    Assert.assertTrue(scripts.get(0).getCreatedAt() <= scripts.get(1).getCreatedAt());

    // when sorted on unsupported key

    assertThatThrownBy(() -> runWithUserContext(USER_ID_1,
                                                () -> scriptService.getScripts(0,
                                                                               1000,
                                                                               "",
                                                                               "xyz",
                                                                               "",
                                                                               null)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("sort on parameter : xyz not supported.");
  }

  private <V> V runWithUserContext(String userId, Callable<V> callable) throws Exception {
    return RequestContext.current()
      .with(UserContext.CTX_KEY, new UserContext(userId))
      .call(callable);
  }

  @Test
  public void testMaxScriptsLimitReachedException() throws Exception {
    // given script request
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("testScript")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a", "b", "c")))
      .setContent("select * from xyz")
      .build();

    long maxNumberOfScriptsPerUser = 1000L;
    ScriptStore mockedScriptStore = mock(ScriptStore.class);
    when(mockedScriptStore.getCountByCondition(any(SearchTypes.SearchQuery.class))).thenReturn(maxNumberOfScriptsPerUser);
    UserSessionService mockUserSessionService = Mockito.mock(UserSessionService.class);
    ScriptService testScriptService = new ScriptServiceImpl(() -> mockedScriptStore, () -> mockUserSessionService);
    testScriptService.start();

    // assert createScript raise MaxScriptsLimitReachedException
    assertThatThrownBy(() -> runWithUserContext(USER_ID_1,
      () -> testScriptService.createScript(scriptRequest)))
      .isInstanceOf(MaxScriptsLimitReachedException.class)
      .hasMessage(String.format("Maximum scripts limit per user is reached. Limit %s; Current %s.",
        maxNumberOfScriptsPerUser, maxNumberOfScriptsPerUser));

    verify(mockedScriptStore, times(1)).getCountByCondition(any(SearchTypes.SearchQuery.class));
  }
}
