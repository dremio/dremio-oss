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
package com.dremio.dac.server;

import static com.dremio.dac.server.FamilyExpectation.CLIENT_ERROR;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

import org.eclipse.jetty.http.HttpHeader;
import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.job.JobFilterItems;
import com.dremio.dac.model.usergroup.UserForm;
import com.dremio.dac.model.usergroup.UserLogin;
import com.dremio.dac.model.usergroup.UserLoginSession;
import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.model.usergroup.UserUI;
import com.dremio.dac.model.usergroup.UsersUI;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;

/**
 * Tests for user services.
 */
public class TestUserServices extends BaseTestServer {
  /**
   * Helper REST resource
   */
  @RestResource
  @Path("testuserservices-helper")
  public static final class HelperResource {
    @GET
    @Secured
    @RolesAllowed("user")
    @Path("test1")
    @Produces(MediaType.APPLICATION_JSON)
    public String test1() {
      return "test1";
    }

    @GET
    @Secured
    @RolesAllowed("admin")
    @Path("test2")
    @Produces(MediaType.APPLICATION_JSON)
    public String test2() {
      return "test2";
    }

    @GET
    @Secured
    @RolesAllowed({"admin", "user"})
    @Path("test3")
    @Produces(MediaType.APPLICATION_JSON)
    public String test3() {
      return "test3";
    }
  }
  private static final String USERNAME_PREFIX = "testUser_";
  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    final UserService userService = l(UserService.class);
    // clear test users
    for (User userConfig: userService.getAllUsers(10000)) {
      if (userConfig.getUserName().startsWith(USERNAME_PREFIX)) {
        try {
          userService.deleteUser(userConfig.getUserName(), userConfig.getVersion());
        } catch (UserNotFoundException e) {
          // ignore search index may have user thats not in kvstore.
        }
      }
    }
  }

  private String testUserName(String name) {
    return USERNAME_PREFIX + name;
  }

  private String testPassword(String name) {
    return USERNAME_PREFIX + name + "123";
  }

  @Test
  public void testUserOCC() throws Exception {
    getPopulator().populateTestUsers();

    doc("create user");
    final User uc = SimpleUser.newBuilder().setUserName(testUserName("test11")).setEmail("test11@dremio.test").setFirstName("test11")
      .setLastName("dremio").build();
    expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test11"))).buildPut(Entity.json(new UserForm(uc, testPassword("test11")))), UserUI.class);
    UserUI u1 = expectSuccess(getBuilder(getAPIv2().path("user/" +  testUserName("test11"))).buildGet(), UserUI.class);

    doc("update user");
    final User uc2 = SimpleUser.newBuilder(u1.getUser()).setEmail("test22@dremio.test").build();
    expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test11"))).buildPost(Entity.json(new UserForm(uc2))), UserUI.class);
    UserUI u2 = expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test11"))).buildGet(), UserUI.class);
    assertEquals(uc2.getEmail(), u2.getUser().getEmail());

    doc("delete with missing version");
    final GenericErrorMessage errorDelete = expectStatus(BAD_REQUEST, getBuilder(getAPIv2().path("user/" + testUserName("test11"))).buildDelete(), GenericErrorMessage.class);
    assertErrorMessage(errorDelete, "missing version param");

    doc("delete with bad version");
    long badVersion = 1234L;
    String expectedErrorMessage = String.format("Cannot delete user \"%s\", version provided \"%s\" is different from version found \"%s\"",
      u2.getName(), badVersion, u2.getUser().getVersion());
    final GenericErrorMessage errorDelete2 = expectStatus(CONFLICT, getBuilder(getAPIv2().path("user/" + testUserName("test11")).queryParam("version", badVersion)).buildDelete(), GenericErrorMessage.class);
    assertErrorMessage(errorDelete2, expectedErrorMessage);

    doc("delete");
    expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test11")).queryParam("version", u2.getUser().getVersion())).buildDelete());
  }

  @Test
  public void testUser() throws Exception {
    getPopulator().populateTestUsers();
    doc("getting user info");
    UserUI u1 = expectSuccess(getBuilder(getAPIv2().path("user/" + SampleDataPopulator.DEFAULT_USER_NAME)).buildGet(), UserUI.class);
    assertEquals(SampleDataPopulator.DEFAULT_USER_NAME, u1.getUserName().getName());
    assertEquals(SampleDataPopulator.DEFAULT_USER_NAME + "@dremio.test", u1.getUser().getEmail());
    // TODO: Expected error is going to change after refactoring of DAC/dependent services into a common module.
    expectError(CLIENT_ERROR, getBuilder(getAPIv2().path("user/dac")).buildGet(), GenericErrorMessage.class);
    doc("Creating user");
    final User userConfig2 = SimpleUser.newBuilder().setUserName(testUserName("test11")).setEmail("test11@dremio.test").setFirstName("test11")
      .setLastName("dremio").build();
    expectSuccess(getBuilder(getAPIv2().path("user/" +  testUserName("test11"))).buildPut(Entity.json(new UserForm(userConfig2, testPassword("test11")))), UserUI.class);

    UserUI u2 = expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test11"))).buildGet(), UserUI.class);
    assertEquals(userConfig2.getEmail(), u2.getUser().getEmail());
    assertEquals(userConfig2.getFirstName(), u2.getUser().getFirstName());
    assertEquals(userConfig2.getLastName(), u2.getUser().getLastName());

    doc("updating user");
    User uc3 = SimpleUser.newBuilder(u2.getUser()).setEmail("test2@dremio.test").build();
    expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test11"))).buildPost(Entity.json(new UserForm(uc3))), UserUI.class);
    UserUI u3 = expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test11"))).buildGet(), UserUI.class);
    assertEquals(u3.getUser().getEmail(), uc3.getEmail());
    assertEquals(u3.getUser().getFirstName(), uc3.getFirstName());
    assertEquals(u3.getUser().getLastName(), uc3.getLastName());

    doc("deleting user");
    expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test11")).queryParam("version", u3.getUser().getVersion())).buildDelete());

    // TODO: Expected error is going to change after refactoring of DAC/dependent services into a common module.
    expectError(CLIENT_ERROR, getBuilder(getAPIv2().path("user/" + testUserName("test11"))).buildGet(), GenericErrorMessage.class);
  }

  @Test
  public void testUserSearch() throws Exception {
    getPopulator().populateTestUsers();
    final UserService userService = l(UserService.class);
    User db = SimpleUser.newBuilder().setUserName(testUserName("DavidBrown")).setCreatedAt(System.currentTimeMillis()).
      setEmail("david.brown@dremio.test").setFirstName("David").setLastName("Brown").build();

    User md = SimpleUser.newBuilder().setUserName(testUserName("MarkDavid")).setCreatedAt(System.currentTimeMillis()).
      setEmail("markdavid@gmail.com").setFirstName("Mark").setLastName("David").build();

    User dw = SimpleUser.newBuilder().setUserName(testUserName("DavidWilson")).setCreatedAt(System.currentTimeMillis()).
      setEmail("david@dremio.test").setFirstName("David").setLastName("Wilson").build();

    User mj = SimpleUser.newBuilder().setUserName("MarkJohnson").setCreatedAt(System.currentTimeMillis()).
      setEmail("mark.johnson@dremio.test").setFirstName("Mark").setLastName("Johnson").build();

    db = userService.createUser(db, db.getFirstName() + "1234");
    md = userService.createUser(md, md.getFirstName() + "1234");
    dw = userService.createUser(dw, dw.getFirstName() + "1234");
    mj = userService.createUser(mj, mj.getFirstName() + "1234");

    UserUI u = expectSuccess(getBuilder(getAPIv2().path("user/" + md.getUserName())).buildGet(), UserUI.class);
    assertEquals(md.getEmail(), u.getUser().getEmail());
    assertEquals(md.getFirstName(), u.getUser().getFirstName());
    assertEquals(md.getLastName(), u.getUser().getLastName());

    u = expectSuccess(getBuilder(getAPIv2().path("user/" + mj.getUserName())).buildGet(), UserUI.class);
    assertEquals(mj.getEmail(), u.getUser().getEmail());
    assertEquals(mj.getFirstName(), u.getUser().getFirstName());
    assertEquals(mj.getLastName(), u.getUser().getLastName());

    doc("Get all users");
    UsersUI users1 = expectSuccess(getBuilder(getAPIv2().path("users/all")).buildGet(), UsersUI.class);
    assertEquals(7, users1.getUsers().size());

    doc("Search all users");
    UsersUI users2 = expectSuccess(getBuilder(getAPIv2().path("users/search")).buildGet(), UsersUI.class);
    assertEquals(7, users2.getUsers().size());

    doc("Search for a user");
    UsersUI users3 = expectSuccess(getBuilder(getAPIv2().path("users/search").queryParam("filter", "David")).buildGet(), UsersUI.class);
    assertEquals(3, users3.getUsers().size());

    UsersUI users4 = expectSuccess(getBuilder(getAPIv2().path("users/search").queryParam("filter", "Mark")).buildGet(), UsersUI.class);
    assertEquals(2, users4.getUsers().size());

    UsersUI users5 = expectSuccess(getBuilder(getAPIv2().path("users/search").queryParam("filter", "Johnson")).buildGet(), UsersUI.class);
    assertEquals(1, users5.getUsers().size());

    doc("Search users using job filters");
    JobFilterItems users6 = expectSuccess(getBuilder(getAPIv2().path("jobs/filters/users").queryParam("filter", "David")).buildGet(), JobFilterItems.class);
    assertEquals(3, users6.getItems().size());

    final JobFilterItems users7 = expectSuccess(getBuilder(getAPIv2().path("jobs/filters/users").queryParam("limit", 2)).buildGet(), JobFilterItems.class);
    assertEquals(toString(), 2, users7.getItems().size());

    userService.deleteUser(db.getUserName(), db.getVersion());
    userService.deleteUser(md.getUserName(), md.getVersion());
    userService.deleteUser(dw.getUserName(), dw.getVersion());
    userService.deleteUser(mj.getUserName(), mj.getVersion());
  }

  @Test
  public void testUserLogin() throws Exception {
    doc("Creating user");
    final UserService userService = l(UserService.class);
    User userConfig2 = SimpleUser.newBuilder().setUserName(testUserName("test12")).setEmail("test1@dremio.test").setFirstName("test12")
      .setLastName("dremio").build();
    expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test12"))).buildPut(Entity.json(new UserForm(userConfig2, testPassword("test12")))), UserUI.class);

    doc("login");
    long now = System.currentTimeMillis();
    UserLogin userLogin = new UserLogin(testUserName("test12"), testPassword("test12"));
    UserLoginSession userLoginToken = expectSuccess(getBuilder(getAPIv2().path("/login")).buildPost(Entity.json(userLogin)), UserLoginSession.class);
    assertEquals(testUserName("test12"), userLoginToken.getUserName());
    assertEquals("test1@dremio.test", userLoginToken.getEmail());
    assertEquals(false, userLoginToken.getPermissions().isCanChatForSupport());
    assertEquals(true, userLoginToken.getPermissions().isCanDownloadProfiles());
    assertEquals(true, userLoginToken.getPermissions().isCanEmailForSupport());
    assertEquals(true, userLoginToken.getPermissions().isCanUploadProfiles());

    // token should expire between 30 days and 30 days 5 mins
    assertTrue(userLoginToken.getExpires() > (now + 30 * 3600 * 1000) && userLoginToken.getExpires() < (now + (30 * 3600 + 5 * 60) * 1000));
    assertTrue(userLoginToken.isShowUserAndUserProperties());

    doc("login with wrong password");
    UserLogin userLogin1 = new UserLogin(testUserName("test12"), "test1-p2");
    expectStatus(UNAUTHORIZED, getBuilder(getAPIv2().path("/login")).buildPost(Entity.json(userLogin1)));

    // start of next test
    UserUI u = expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test12"))).buildGet(), UserUI.class);
    userService.deleteUser(new UserName(testUserName("test12")).getName(), u.getUser().getVersion());

    doc("Creating user");
    User userConfig1 = SimpleUser.newBuilder().setUserName(testUserName("test1")).setEmail("test1@dremio.test").setFirstName("test1")
      .setLastName("dremio").build();
    userConfig1 = userService.createUser(userConfig1, testPassword("test1"));

    userConfig2 = SimpleUser.newBuilder().setUserName(testUserName("test2")).setEmail("test2@dremio.test").setFirstName("test2")
      .setLastName("dremio").build();
    userConfig2 = userService.createUser(userConfig2, testPassword("test2"));

    User userConfig3 = SimpleUser.newBuilder().setUserName(testUserName("test3")).setEmail("test3@dremio.test").setFirstName("test3")
      .setLastName("dremio").build();
    userConfig3 = userService.createUser(userConfig3, testPassword("test3"));

    doc("login as user test1(user)");
    userLogin = new UserLogin(testUserName("test1"), testPassword("test1"));
    String token = expectSuccess(getAPIv2().path("/login").request(JSON).buildPost(Entity.json(userLogin)), UserLoginSession.class).getToken();
    doc("set auth header and make request");
    expectSuccess(getAPIv2().path("/testuserservices-helper/test1").request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + token).buildGet(), String.class);

    expectSuccess(getAPIv2().path("/testuserservices-helper/test3").request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + token).buildGet(), String.class);

    expectSuccess(getAPIv2().path("/testuserservices-helper/test2").request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + token).buildGet());

    doc("login as user test2(admin)");
    userLogin = new UserLogin(testUserName("test2"), testPassword("test2"));
    token = expectSuccess(getBuilder(getAPIv2().path("/login")).buildPost(Entity.json(userLogin)), UserLoginSession.class).getToken();
    doc("set auth header and make request");
    // Every user is part of "user" after the introduction of the roles service.
    expectSuccess(getAPIv2().path("/testuserservices-helper/test1").request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + token).buildGet());

    expectSuccess(getAPIv2().path("/testuserservices-helper/test2").request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + token).buildGet());

    expectSuccess(getAPIv2().path("/testuserservices-helper/test3").request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + token).buildGet());


    doc("login as user test3(user, admin)");
    userLogin = new UserLogin(testUserName("test3"), testPassword("test3"));
    token = expectSuccess(getAPIv2().path("/login").request(JSON).buildPost(Entity.json(userLogin)), UserLoginSession.class).getToken();
    doc("set auth header and make request");
    expectSuccess(getAPIv2().path("/testuserservices-helper/test1").request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + token).buildGet(), String.class);

    expectSuccess(getAPIv2().path("/testuserservices-helper/test2").request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + token).buildGet(), String.class);

    expectSuccess(getAPIv2().path("/testuserservices-helper/test3").request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + token).buildGet(), String.class);

    u = expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test1"))).buildGet(), UserUI.class);
    userService.deleteUser(new UserName(testUserName("test1")).getName(), u.getUser().getVersion());
    u = expectSuccess(getBuilder(getAPIv2().path("user/" +  testUserName("test2"))).buildGet(), UserUI.class);
    userService.deleteUser(new UserName(testUserName("test2")).getName(), u.getUser().getVersion());
    u = expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test3"))).buildGet(), UserUI.class);
    userService.deleteUser(new UserName(testUserName("test3")).getName(), u.getUser().getVersion());
  }

  @Test
  public void testUserChangePassword() throws Exception {
    doc("Creating user");
    User userConfig = SimpleUser.newBuilder().setUserName(testUserName("test12")).setEmail("test1@dremio.test").setFirstName("test12")
      .setLastName("dremio").build();
    expectSuccess(getBuilder(getAPIv2().path("user/" +  testUserName("test12"))).buildPut(Entity.json(new UserForm(userConfig, testPassword("test12")))), UserUI.class);
    UserUI u = expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test12"))).buildGet(), UserUI.class);

    doc("login");
    UserLogin userLogin = new UserLogin(testUserName("test12"), testPassword("test12"));
    expectSuccess(getBuilder(getAPIv2().path("/login")).buildPost(Entity.json(userLogin)), UserLoginSession.class);

    doc("change name");
    User userConfig2 = SimpleUser.newBuilder(u.getUser()).setUserName(testUserName("newTestName")).build();
    expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("test12"))).buildPost(Entity.json(new UserForm(userConfig2, testPassword("test12")))), UserUI.class);

    UserLogin userLogin2 = new UserLogin(testUserName("newTestName"), testPassword("test12"));
    expectSuccess(getBuilder(getAPIv2().path("/login")).buildPost(Entity.json(userLogin2)), UserLoginSession.class);

    u = expectSuccess(getBuilder(getAPIv2().path("user/" + testUserName("newTestName"))).buildGet(), UserUI.class);
    l(UserService.class).deleteUser(new UserName(testUserName("newTestName")).getName(), u.getUser().getVersion());
  }

  @Test
  public void testUserAuthorization() throws Exception {
    doc("Creating user");
    final UserService userService = l(UserService.class);

    User admin = SimpleUser.newBuilder().setUserName(testUserName("admin")).setEmail("admin@dremio.test").setFirstName("admin")
      .setLastName("dremio").build();
    admin = userService.createUser(admin, testPassword("admin"));

    User user1 = SimpleUser.newBuilder().setUserName(testUserName("user1")).setEmail("user1@dremio.test").setFirstName("user1")
      .setLastName("user1").build();
    user1 = userService.createUser(user1, testPassword("user1"));

    User user2 = SimpleUser.newBuilder().setUserName(testUserName("user2")).setEmail("user2@dremio.test").setFirstName("user2")
      .setLastName("user2").build();

    // login as admin
    UserLogin adminLogin = new UserLogin(testUserName("admin"), testPassword("admin"));
    UserLoginSession adminLoginToken = expectSuccess(getBuilder(getAPIv2().path("/login")).buildPost(Entity.json(adminLogin)), UserLoginSession.class);

    // create user2 as admin
    UserUI user = expectSuccess(getAPIv2().path("user/" + testUserName("user2")).request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + adminLoginToken.getToken()).buildPut(Entity.json(new UserForm(user2, testPassword("user2")))), UserUI.class);
    assertEquals(testUserName("user2"), user.getUserName().getName());

    // login in as user2
    UserLogin user2Login = new UserLogin(testUserName("user2"), testPassword("user2"));
    @SuppressWarnings("unused")
    UserLoginSession user2LoginToken = expectSuccess(getBuilder(getAPIv2().path("/login")).buildPost(Entity.json(user2Login)), UserLoginSession.class);

    // test admin overrides
    // email
    user2 = SimpleUser.newBuilder(user2)
        .setEmail("user2_new@dremio.test")
        .setVersion(user.getUser().getVersion())
        .build();
    user = expectSuccess(getAPIv2().path("user/" + testUserName("user2")).request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + adminLoginToken.getToken()).buildPost(Entity.json(new UserForm(user2, testPassword("user2")))), UserUI.class);
    assertEquals(testUserName("user2"), user.getUserName().getName());
    assertEquals(user2.getEmail(), user.getUser().getEmail());

    // Password set only password in user form
    user = expectSuccess(getAPIv2().path("user/" + testUserName("user2")).request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + adminLoginToken.getToken()).buildPost(Entity.json(new UserForm(testPassword("user2_new")))), UserUI.class);
    assertEquals(testUserName("user2"), user.getUserName().getName());
    assertEquals(user2.getEmail(), user.getUser().getEmail());

    // login with old password should fail
    expectStatus(UNAUTHORIZED, getBuilder(getAPIv2().path("/login")).buildPost(Entity.json(user2Login)));
    // login with new password
    user2Login = new UserLogin(testUserName("user2"), testPassword("user2_new"));
    user2LoginToken = expectSuccess(getBuilder(getAPIv2().path("/login")).buildPost(Entity.json(user2Login)), UserLoginSession.class);

    // admin changes username
    user = expectSuccess(getAPIv2().path("user/" + testUserName("user2")).request(JSON).header(HttpHeader.AUTHORIZATION.toString(),
      "_dremio" + adminLoginToken.getToken()).buildPost(Entity.json(new UserForm(SimpleUser.newBuilder().setUserName(testUserName("user2_new")).build()))), UserUI.class);
    assertEquals(user2.getEmail(), user.getUser().getEmail());
    assertEquals(testUserName("user2_new"), user.getUserName().getName());

    // old login should fail
    expectError(CLIENT_ERROR, getBuilder(getAPIv2().path("/login")).buildPost(Entity.json(user2Login)), NotFoundErrorMessage.class);
    // new login should work
    user2Login = new UserLogin(testUserName("user2_new"), testPassword("user2_new"));
    user2LoginToken = expectSuccess(getBuilder(getAPIv2().path("/login")).buildPost(Entity.json(user2Login)), UserLoginSession.class);

    user2 = SimpleUser.newBuilder(user2)
        .setVersion(user.getUser().getVersion())
        .build();
  }

  @Test
  public void testNoSelfDelete() throws Exception {
    // Get the current user info
    UserUI u1 = expectSuccess(getBuilder(getAPIv2().path("user/" + SampleDataPopulator.DEFAULT_USER_NAME)).buildGet(), UserUI.class);
    assertEquals(SampleDataPopulator.DEFAULT_USER_NAME, u1.getUserName().getName());

    final GenericErrorMessage errorDelete = expectStatus(FORBIDDEN,
        getBuilder(
            getAPIv2().path("user/" + SampleDataPopulator.DEFAULT_USER_NAME)
                .queryParam("version", u1.getUser().getVersion())
        ).buildDelete(),
        GenericErrorMessage.class);

    assertEquals("Deletion of the user account of currently logged in user is not allowed.",
        errorDelete.getErrorMessage());
  }

}
