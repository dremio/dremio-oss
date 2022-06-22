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

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.core.SecurityContext;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.script.proto.ScriptProto;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;

/**
 * Test for script service
 */
public class TestScriptServiceImpl {

  private static final SabotConfig DEFAULT_SABOT_CONFIG = SabotConfig.forClient();
  private static final ScanResult
    CLASSPATH_SCAN_RESULT = ClassPathScanner.fromPrescan(DEFAULT_SABOT_CONFIG);
  private static final String USER_NAME_1 = "user1";
  private static final String USER_ID_1 = "87605f99-88da-45b1-8668-09bd0cfa8528";
  private static final String USER_NAME_2 = "user2";
  private static final String USER_ID_2 = "f7a7c98c-8c44-4691-9b19-e4373a8c38d3";
  private ScriptStore scriptStore;
  private LocalKVStoreProvider kvStoreProvider;
  private ScriptService scriptService1;
  private ScriptService scriptService2;

  @Before
  public void setUp() throws Exception {

    kvStoreProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();

    scriptStore = new ScriptStoreImpl(() -> kvStoreProvider);
    scriptStore.start();

    UserService userService = Mockito.mock(UserService.class);

    Mockito.doReturn(SimpleUser.newBuilder()
                       .setUserName(USER_NAME_1)
                       .setUID(new UID(USER_ID_1))
                       .build()).when(userService).getUser(USER_NAME_1);
    Mockito.doReturn(SimpleUser.newBuilder()
                       .setUserName(USER_NAME_2)
                       .setUID(new UID(USER_ID_2))
                       .build()).when(userService).getUser(USER_NAME_2);

    // get script service for user1
    SecurityContext securityContext1 = Mockito.mock(SecurityContext.class);
    Principal principal1 = Mockito.mock(Principal.class);
    Mockito.doReturn(USER_NAME_1).when(principal1).getName();
    Mockito.doReturn(principal1).when(securityContext1).getUserPrincipal();
    scriptService1 = new ScriptServiceImpl(() -> scriptStore, securityContext1, userService);

    // get script service for user2
    SecurityContext securityContext2 = Mockito.mock(SecurityContext.class);
    Principal principal2 = Mockito.mock(Principal.class);
    Mockito.doReturn(USER_NAME_2).when(principal2).getName();
    Mockito.doReturn(principal2).when(securityContext2).getUserPrincipal();
    scriptService2 = new ScriptServiceImpl(() -> scriptStore, securityContext2, userService);
  }

  @After
  public void cleanUp() throws Exception {
    kvStoreProvider.close();
  }

  @Test
  public void testCreateScript()
    throws DuplicateScriptNameException, ScriptNotAccessible, ScriptNotFoundException {
    //-----------------------create----------------------------
    // given script request
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("testScript")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a", "b", "c")))
      .setContent("select * from xyz")
      .build();

    RequestContext requestContext =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext);

    // when create request is sent
    ScriptProto.Script script = scriptService1.createScript(scriptRequest);

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

    scriptService1.updateScript(script.getScriptId(), updateRequest);

    //-------------------validate content after get----------

    ScriptProto.Script updatedScript = scriptService1.getScriptById(script.getScriptId());

    Assert.assertEquals(updateRequest.getName(), updatedScript.getName());
    Assert.assertEquals(USER_ID_1, updatedScript.getModifiedBy());
    Assert.assertEquals(updateRequest.getContextList(), updatedScript.getContextList());
    Assert.assertEquals(updateRequest.getContent(), updatedScript.getContent());


    //--------------------delete-----------------------------

    scriptService1.deleteScriptById(script.getScriptId());


    //----------------------get------------------------------
    // assert get of script raise scriptnot found exception

    assertThatThrownBy(() -> scriptService1.getScriptById(script.getScriptId()))
      .isInstanceOf(ScriptNotFoundException.class)
      .hasMessage(String.format("Script with id or name : %s not found.",
                                script.getScriptId()));
    mocked.close();
  }

  // test 2 scripts cannot have same script name
  @Test
  public void testDuplicateScriptName() throws DuplicateScriptNameException {
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("scriptName")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("test content")
      .build();

    RequestContext requestContext =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext);

    ScriptProto.Script script = scriptService1.createScript(scriptRequest);
    assertThatThrownBy(() -> scriptService1.createScript(scriptRequest))
      .isInstanceOf(DuplicateScriptNameException.class)
      .hasMessage(
        "Cannot reuse the same script name within a project. Please try another script name.");
    mocked.close();
  }

  // test 2 scripts created by different users can have same name
  @Test
  public void testDuplicateNameDifferentUser() throws DuplicateScriptNameException {
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("scriptName")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("test content")
      .build();

    RequestContext requestContext1 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext1);

    ScriptProto.Script script1 = scriptService1.createScript(scriptRequest);

    RequestContext requestContext2 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_2));
    mocked.when(RequestContext::current).thenReturn(requestContext2);

    ScriptProto.Script script2 = scriptService2.createScript(scriptRequest);

    Assert.assertEquals(USER_ID_1, script1.getCreatedBy());
    Assert.assertEquals(USER_ID_2, script2.getCreatedBy());
    Assert.assertEquals(script1.getName(), script2.getName());
    mocked.close();
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

    RequestContext requestContext1 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext1);

    assertThatThrownBy(() -> scriptService1.createScript(scriptRequest))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Maximum 128 characters allowed in script name.");
    mocked.close();
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

    RequestContext requestContext1 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext1);

    assertThatThrownBy(() -> scriptService1.createScript(scriptRequest))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Maximum 10000 characters allowed in script content.");
    mocked.close();
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

    RequestContext requestContext1 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext1);

    assertThatThrownBy(() -> scriptService1.createScript(scriptRequest))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Maximum 1024 characters allowed in script description.");
    mocked.close();
  }

  // test a script cannot by read/update/delete by another user
  @Test
  public void testReadAccess() throws DuplicateScriptNameException {
    //given script created by user1
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("testReadAccess")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("select * from xyz")
      .build();

    RequestContext requestContext1 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext1);

    ScriptProto.Script script = scriptService1.createScript(scriptRequest);

    // when script is read by user2
    RequestContext requestContext2 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_2));
    mocked.when(RequestContext::current).thenReturn(requestContext2);

    // assert throws scriptNotAccessible
    assertThatThrownBy(() -> scriptService2.getScriptById(script.getScriptId()))
      .isInstanceOf(ScriptNotAccessible.class)
      .hasMessage(String.format("User : %s not authorized to perform this action on Script : %s.",
                                USER_ID_2,
                                script.getScriptId()));
    mocked.close();
  }

  @Test
  public void testUpdateAccess() throws DuplicateScriptNameException {
    //given script created by user1
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("testUpdateAccess")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("select * from xyz")
      .build();

    RequestContext requestContext1 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext1);

    ScriptProto.Script script = scriptService1.createScript(scriptRequest);

    // when script is updated by user2
    RequestContext requestContext2 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_2));
    mocked.when(RequestContext::current).thenReturn(requestContext2);

    // assert throws scriptNotAccessible
    assertThatThrownBy(() -> scriptService2.updateScript(script.getScriptId(), scriptRequest))
      .isInstanceOf(ScriptNotAccessible.class)
      .hasMessage(String.format("User : %s not authorized to perform this action on Script : %s.",
                                USER_ID_2,
                                script.getScriptId()));
    mocked.close();
  }

  @Test
  public void testDeleteAccess() throws DuplicateScriptNameException {
    //given script created by user1
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("testDeleteAccess")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("select * from xyz")
      .build();

    RequestContext requestContext1 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext1);

    ScriptProto.Script script = scriptService1.createScript(scriptRequest);

    // when script is deleted by user2
    RequestContext requestContext2 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_2));
    mocked.when(RequestContext::current).thenReturn(requestContext2);

    // assert throws scriptNotAccessible
    assertThatThrownBy(() -> scriptService2.deleteScriptById(script.getScriptId()))
      .isInstanceOf(ScriptNotAccessible.class)
      .hasMessage(String.format("User : %s not authorized to perform this action on Script : %s.",
                                USER_ID_2,
                                script.getScriptId()));
    mocked.close();
  }

  @Test
  public void testSearchOnGetAll() throws DuplicateScriptNameException {
    //given scripts with name FirstScript, SecondScript and RandomName
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("FirstScript")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("select * from xyz")
      .build();

    RequestContext requestContext1 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext1);

    ScriptProto.Script script1 = scriptService1.createScript(scriptRequest);
    ScriptProto.Script script2 =
      scriptService1.createScript(scriptRequest.toBuilder().setName("SecondScript").build());
    ScriptProto.Script script3 =
      scriptService1.createScript(scriptRequest.toBuilder().setName("RandomName").build());

    // when script is searched for name "Script"
    List<ScriptProto.Script> scripts =
      scriptService1.getScripts(0, 1000, "Script", "", "", null);

    // assert 2 scripts are found
    Assert.assertEquals(2, scripts.size());
    Assert.assertTrue(scripts.stream()
                        .map(ScriptProto.Script::getScriptId)
                        .collect(Collectors.toList())
                        .containsAll(Arrays.asList(script1.getScriptId(), script2.getScriptId())));
    mocked.close();
  }

  // test sort feature
  @Test
  public void testSortOnGetAll()
    throws DuplicateScriptNameException, ScriptNotAccessible, ScriptNotFoundException {
    // given
    ScriptProto.ScriptRequest scriptRequest = ScriptProto.ScriptRequest.newBuilder()
      .setName("sortScript1")
      .setDescription("test description")
      .addAllContext(new ArrayList<>(Arrays.asList("a")))
      .setContent("select * from xyz")
      .build();

    RequestContext requestContext1 =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext1);

    // create script 1
    ScriptProto.Script script1 = scriptService1.createScript(scriptRequest);

    // create script 2
    ScriptProto.Script script2 =
      scriptService1.createScript(scriptRequest.toBuilder().setName("sortScript2").build());

    // update script 1
    scriptService1.updateScript(script1.getScriptId(),
                                scriptRequest.toBuilder()
                                  .setDescription("updated description")
                                  .build());

    // when  sorted on name
    List<ScriptProto.Script> scripts =
      scriptService1.getScripts(0, 1000, "sortScript", "-name", "", null);
    Assert.assertEquals("sortScript2", scripts.get(0).getName());
    Assert.assertEquals("sortScript1", scripts.get(1).getName());

    // when sorted on createdAt
    scripts = scriptService1.getScripts(0, 1000, "sortScript", "createdAt", "", null);
    Assert.assertEquals("sortScript1", scripts.get(0).getName());
    Assert.assertEquals("sortScript2", scripts.get(1).getName());
    Assert.assertTrue(scripts.get(0).getCreatedAt() <= scripts.get(1).getCreatedAt());

    // when sorted on -modifiedAt
    scripts = scriptService1.getScripts(0, 1000, "sortScript", "-modifiedAt", "", null);
    Assert.assertEquals("sortScript1", scripts.get(0).getName());
    Assert.assertEquals("sortScript2", scripts.get(1).getName());
    Assert.assertTrue(scripts.get(0).getCreatedAt() <= scripts.get(1).getCreatedAt());

    // when sorted on unsupported key

    assertThatThrownBy(() -> scriptService1.getScripts(0, 1000, "", "xyz", "", null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("sort on parameter : xyz not supported.");
    mocked.close();
  }

  // TODO:
  // validate max number of script per user
  // test get count of script
}
