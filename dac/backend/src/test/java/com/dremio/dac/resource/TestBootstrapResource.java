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
package com.dremio.dac.resource;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.common.perf.Timer;
import com.dremio.config.DremioConfig;
import com.dremio.dac.daemon.DACDaemon;
import com.dremio.dac.model.usergroup.UserForm;
import com.dremio.dac.model.usergroup.UserLogin;
import com.dremio.dac.model.usergroup.UserUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.User;
import com.dremio.test.DremioTest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests for class {@link BootstrapResource} */
public class TestBootstrapResource extends BaseTestServer {

  // make sure we disable the test APIs otherwise NoUserFilter will never be tested.
  private static DACConfig dacConfig =
      DACConfig.newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
          .autoPort(true)
          .serveUI(false)
          .inMemoryStorage(false)
          .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
          .clusterMode(DACDaemon.ClusterMode.LOCAL);

  @BeforeClass
  public static void init() throws Exception {
    Assume.assumeFalse(BaseTestServer.isMultinode());
    enableDefaultUser(false);
    try (Timer.TimedBlock b = Timer.time("TestBootstrapResource.@BeforeClass")) {
      dacConfig = dacConfig.writePath(folder1.newFolder().getAbsolutePath());
      startDaemon();
    }
  }

  private static void startDaemon() throws Exception {
    setCurrentDremioDaemon(DACDaemon.newDremioDaemon(dacConfig, DremioTest.CLASSPATH_SCAN_RESULT));
    getCurrentDremioDaemon().init();
    initClient();
  }

  @Test
  public void testFirstUser() throws Exception {
    // first make sure test APIs are disabled, we want to exercise NoUserFilter
    doc("ensure test APIs disabled");
    try {
      expectStatus(
          Response.Status.NOT_FOUND,
          getHttpClient().getAPIv2().path("/test/clear").request().buildPost(null));
    } catch (AssertionError e) {
      throw new AssertionError("This test expects test APIs to be disabled", e);
    }

    doc("login when no user available");
    // we've disabled adding the default user
    // so trying to login should trigger the NoUserFilter and return a FORBIDDEN response
    {
      final UserLogin userLogin = new UserLogin(DEFAULT_USERNAME, DEFAULT_PASSWORD);
      GenericErrorMessage errorMessage =
          expectStatus(
              Response.Status.FORBIDDEN,
              getHttpClient()
                  .getAPIv2()
                  .path("/login")
                  .request(JSON)
                  .buildPost(Entity.json(userLogin)),
              GenericErrorMessage.class);
      assertThat(errorMessage.getErrorMessage()).isEqualTo(GenericErrorMessage.NO_USER_MSG);
    }

    {
      // trying to access a secured API should also return the same expected error
      GenericErrorMessage errorMessage =
          expectStatus(
              Response.Status.FORBIDDEN,
              getHttpClient().getAPIv2().path("users/all").request().buildGet(),
              GenericErrorMessage.class);
      assertThat(errorMessage.getErrorMessage()).isEqualTo(GenericErrorMessage.NO_USER_MSG);
    }

    doc("create first user");
    {
      final User uc =
          SimpleUser.newBuilder()
              .setUserName("bootstrap_user")
              .setEmail("bootstrap_user@dremio.test")
              .setFirstName("test")
              .setLastName("dremio")
              .build();
      expectSuccess(
          getHttpClient()
              .getAPIv2()
              .path("bootstrap/firstuser")
              .request(JSON)
              .buildPut(Entity.json(new UserForm(uc, "dremio123"))),
          UserUI.class);
    }

    doc("access first user api 2nd time");
    {
      final User uc =
          SimpleUser.newBuilder()
              .setUserName("bootstrap_user_2")
              .setEmail("bootstrap_user_2@dremio.test")
              .setFirstName("bootstrap")
              .setLastName("dremio")
              .build();
      UserExceptionMapper.ErrorMessageWithContext errorMessage =
          expectError(
              FamilyExpectation.CLIENT_ERROR,
              getHttpClient()
                  .getAPIv2()
                  .path("bootstrap/firstuser")
                  .request(JSON)
                  .buildPut(Entity.json(new UserForm(uc, "dremio123"))),
              UserExceptionMapper.ErrorMessageWithContext.class);
      assertThat(errorMessage.getErrorMessage()).isEqualTo(BootstrapResource.ERROR_MSG);
    }

    // we should be able to login using the newly created user
    doc("login as first user");
    login("bootstrap_user", "dremio123");
  }
}
