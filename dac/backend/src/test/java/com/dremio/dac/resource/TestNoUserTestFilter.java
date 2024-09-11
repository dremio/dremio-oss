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

import com.dremio.dac.model.usergroup.UserLogin;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.GenericErrorMessage;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.junit.Assume;
import org.junit.Test;

/** Test for class {@link com.dremio.dac.server.test.NoUserTestFilter} */
public class TestNoUserTestFilter extends BaseTestServer {

  @Test
  public void testNoUserTestFilter() throws Exception {
    Assume.assumeFalse(BaseTestServer.isMultinode());

    // we should be able to login using the default user
    doc("make sure we can login as the default user");
    login();

    doc("use the clear test API");
    expectSuccess(getBuilder(getAPIv2().path("/test/clear")).buildPost(Entity.json("")));

    doc("make sure server returns proper response");
    // clearing everything should not affect the server response if NoUserTestFilter is being used
    {
      final UserLogin userLogin = new UserLogin(DEFAULT_USERNAME, DEFAULT_PASSWORD);
      GenericErrorMessage errorMessage =
          expectStatus(
              Response.Status.FORBIDDEN,
              getAPIv2().path("/login").request(JSON).buildPost(Entity.json(userLogin)),
              GenericErrorMessage.class);
      assertThat(errorMessage.getErrorMessage()).isEqualTo(GenericErrorMessage.NO_USER_MSG);
    }
  }
}
