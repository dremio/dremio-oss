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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.script.proto.ScriptProto;

/**
 * Test for script service
 */
public class TestScriptServiceImpl {

  private static final SabotConfig DEFAULT_SABOT_CONFIG = SabotConfig.forClient();
  private static final ScanResult
    CLASSPATH_SCAN_RESULT = ClassPathScanner.fromPrescan(DEFAULT_SABOT_CONFIG);
  private static final String USER_ID_1 = "87605f99-88da-45b1-8668-09bd0cfa8528";
  private static final String USER_ID_2 = "f7a7c98c-8c44-4691-9b19-e4373a8c38d3";
  private ScriptStore scriptStore;
  private LocalKVStoreProvider kvStoreProvider;
  private ScriptService scriptService;

  @Before
  public void setUp() throws Exception {

    kvStoreProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();

    scriptStore = new ScriptStoreImpl(() -> kvStoreProvider);
    scriptStore.start();

    // get script service
    scriptService = new ScriptServiceImpl(() -> scriptStore);
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
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("testScript")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a", "b", "c")))
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
      .hasMessage("Maximum 128 characters allowed in script name.");
  }

  // validate max length of content
  @Test
  public void testMaxLengthOfContent() {
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("scriptWithVeryLongContent")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent(StringUtils.repeat("*", 10001))
      .build();


    assertThatThrownBy(() -> runWithUserContext(USER_ID_1,
                                                () -> scriptService.createScript(scriptRequest)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Maximum 10000 characters allowed in script content.");
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
      .hasMessage("Maximum 1024 characters allowed in script description.");
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

  // TODO:
  // validate max number of script per user
  // test get count of script
}
