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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;
import com.fasterxml.jackson.annotation.JsonIgnore;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests {@link UserResource} API */
public class TestUserResource extends BaseTestServer {
  private static final String USER_PATH = "/user/";
  private static final String password = "foo12bar";

  private static com.dremio.service.users.User createdUser = null;

  @Before
  public void setup() throws Exception {
    final UserService userService = getUserService();

    SimpleUser user1 =
        SimpleUser.newBuilder()
            .setUserName("user1")
            .setFirstName("")
            .setLastName("")
            .setEmail("user1@foo.com")
            .build();
    createdUser = userService.createUser(user1, password);
  }

  @After
  public void teardown() throws Exception {
    final UserService userService = getUserService();

    userService.deleteUser(createdUser.getUserName(), createdUser.getVersion());
  }

  @Test
  public void testGetUserById() throws Exception {
    final UserService userService = getUserService();
    com.dremio.service.users.User user1 = userService.getUser("user1");

    User user =
        expectSuccess(
            getBuilder(getHttpClient().getAPIv3().path(USER_PATH).path(user1.getUID().getId()))
                .buildGet(),
            User.class);
    assertEquals(user.getId(), user1.getUID().getId());
    assertEquals(user.getName(), user1.getUserName());
  }

  @Test
  public void testGetUserDetails() throws Exception {
    final UserService userService = getUserService();
    com.dremio.service.users.User user1 = userService.getUser("user1");

    User user =
        expectSuccess(
            getBuilder(getHttpClient().getAPIv3().path(USER_PATH).path(user1.getUID().getId()))
                .buildGet(),
            User.class);
    assertEquals(user.getId(), user1.getUID().getId());
    assertEquals(user.getName(), user1.getUserName());
    assertEquals(user.getFirstName(), user1.getFirstName());
    assertEquals(user.getLastName(), user1.getLastName());
    assertEquals(user.getEmail(), user1.getEmail());
    assertEquals(user.getTag(), user1.getVersion());
    assertNull("Password should not be sent to a data consumer", user.getPassword());
  }

  /** User update request that includes password information */
  private static class UserInfoRequest extends User {
    public UserInfoRequest(
        String id,
        String name,
        String firstName,
        String lastName,
        String email,
        String version,
        String password,
        String extra) {
      super(id, name, firstName, lastName, email, version, password, extra);
    }

    @Override
    @JsonIgnore(false) // tests should be able to send password to api
    public String getPassword() {
      return super.getPassword();
    }
  }

  @Test
  public void testCreateUser() throws Exception {
    UserInfoRequest userInfo =
        new UserInfoRequest(
            null,
            "test_new_user",
            "test",
            "new user",
            "bla@bla.bla",
            "0",
            "123some_password",
            null);
    User savedUser =
        expectSuccess(
            getBuilder(getHttpClient().getAPIv3().path(USER_PATH)).buildPost(Entity.json(userInfo)),
            User.class);

    assertNotNull(savedUser.getId());
    assertEquals(savedUser.getName(), userInfo.getName());
    assertEquals(savedUser.getFirstName(), userInfo.getFirstName());
    assertEquals(savedUser.getLastName(), userInfo.getLastName());
    assertEquals(savedUser.getEmail(), userInfo.getEmail());
    assertNull("Password should not be sent to a data consumer", savedUser.getPassword());
    assertNotNull(savedUser.getTag());

    final UserService userService = getUserService();
    userService.deleteUser(savedUser.getName(), savedUser.getTag());
  }

  @Test
  public void testCreateUserWithExistingName() throws Exception {
    UserInfoRequest userInfo =
        new UserInfoRequest(
            null,
            createdUser.getUserName(),
            "test",
            "new user",
            "bla@bla.bla",
            "0",
            "123some_password",
            null);

    // should not allow to create a user with a name of existing user
    expect(
        FamilyExpectation.CLIENT_ERROR,
        getBuilder(getHttpClient().getAPIv3().path(USER_PATH).path("createUser"))
            .buildPut(Entity.json(userInfo)));
  }

  @Test
  public void testUpdateUser() {
    UserInfoRequest userInfo =
        new UserInfoRequest(
            createdUser.getUID().getId(),
            createdUser.getUserName(),
            "a new firstName",
            " a new last name",
            "new_email@mail.com",
            createdUser.getVersion(),
            null,
            null);

    User savedUser =
        expectSuccess(
            getBuilder(
                    getHttpClient().getAPIv3().path(USER_PATH).path(createdUser.getUID().getId()))
                .buildPut(Entity.json(userInfo)),
            User.class);

    assertEquals(createdUser.getUID().getId(), savedUser.getId());
    assertEquals(savedUser.getName(), userInfo.getName());
    assertEquals(savedUser.getFirstName(), userInfo.getFirstName());
    assertEquals(savedUser.getLastName(), userInfo.getLastName());
    assertEquals(savedUser.getEmail(), userInfo.getEmail());
    assertNotEquals("version should be changed", savedUser.getTag(), userInfo.getTag());
    assertNull("Password should not be sent to a data consumer", savedUser.getPassword());

    // for correct cleanup
    createdUser =
        SimpleUser.newBuilder()
            .setUID(new UID(savedUser.getId()))
            .setUserName(savedUser.getName())
            .setVersion(savedUser.getTag())
            .build();
  }

  @Test
  public void testUserNameChange() {
    UserInfoRequest userInfo =
        new UserInfoRequest(
            createdUser.getUID().getId(),
            createdUser.getUserName() + "2",
            "a new firstName",
            " a new last name",
            "new_email@mail.com",
            createdUser.getVersion(),
            null,
            null);

    // should not allow to change a user name
    expect(
        FamilyExpectation.CLIENT_ERROR,
        getBuilder(getHttpClient().getAPIv3().path(USER_PATH).path(createdUser.getUID().getId()))
            .buildPut(Entity.json(userInfo)));
  }

  @Test
  public void testGetUserByInvalidId() throws Exception {
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(getHttpClient().getAPIv3().path(USER_PATH).path("invalid-id")).buildGet());
  }

  @Test
  public void testGetUserByName() throws Exception {
    User user =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(USER_PATH)
                        .path("by-name")
                        .path(createdUser.getUserName()))
                .buildGet(),
            User.class);
    assertEquals(user.getId(), createdUser.getUID().getId());
    assertEquals(user.getName(), createdUser.getUserName());
  }

  @Test
  public void testGetUserByInvalidName() throws Exception {
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(getHttpClient().getAPIv3().path(USER_PATH).path("by-name").path("invalid-name"))
            .buildGet());
  }
}
