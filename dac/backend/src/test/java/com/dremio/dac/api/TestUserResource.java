/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static org.junit.Assert.assertEquals;

import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.UserService;

/**
 * Tests {@link UserResource} API
 */
public class TestUserResource extends BaseTestServer {
  private static final String USER_PATH = "/user/";

  private static com.dremio.service.users.User createdUser = null;

  @Before
  public void setup() throws Exception {
    final UserService userService = l(UserService.class);

    SimpleUser user1 = SimpleUser.newBuilder().setUserName("user1").setFirstName("").setLastName("").setEmail("user1@foo.com").build();
    createdUser = userService.createUser(user1, "foo12bar");
  }

  @After
  public void teardown() throws Exception {
    final UserService userService = l(UserService.class);

    userService.deleteUser(createdUser.getUserName(), createdUser.getVersion());
  }

  @Test
  public void testGetUserById() throws Exception {
    final UserService userService = l(UserService.class);
    com.dremio.service.users.User user1 = userService.getUser("user1");

    User user = expectSuccess(getBuilder(getPublicAPI(3).path(USER_PATH).path(user1.getUID().getId())).buildGet(), User.class);
    assertEquals(user.getId(), user1.getUID().getId());
    assertEquals(user.getName(), user1.getUserName());
  }

  @Test
  public void testGetUserByInvalidId() throws Exception {
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(USER_PATH).path("invalid-id")).buildGet());
  }

  @Test
  public void testGetUserByName() throws Exception {
    final UserService userService = l(UserService.class);

    User user = expectSuccess(getBuilder(getPublicAPI(3).path(USER_PATH).path("by-name").path(createdUser.getUserName())).buildGet(), User.class);
    assertEquals(user.getId(), createdUser.getUID().getId());
    assertEquals(user.getName(), createdUser.getUserName());
  }

  @Test
  public void testGetUserByInvalidName() throws Exception {
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(USER_PATH).path("by-name").path("invalid-name")).buildGet());
  }
}
