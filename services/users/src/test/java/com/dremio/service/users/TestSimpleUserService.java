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
package com.dremio.service.users;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.service.users.SimpleUserService.UserGroupStoreBuilder;
import com.dremio.service.users.StatusUserLoginException.Status;
import com.dremio.service.users.proto.UID;
import com.dremio.service.users.proto.UserConfig;
import com.dremio.service.users.proto.UserInfo;
import com.dremio.service.users.proto.UserType;
import com.dremio.test.DremioTest;
import com.google.common.collect.Iterables;

/**
 * user/admin/group management tests.
 */
public class TestSimpleUserService {

  public void good(String passwd) throws Exception {
    SimpleUserService.validatePassword(passwd);
  }

  public void bad(String passwd, String err) throws Exception {
    try {
      SimpleUserService.validatePassword(passwd);
      fail("Password '" + passwd + "' should not be accepted.");
    } catch (UserException e) {
      assertEquals("Expected a VALIDATION error but got " + e.getErrorType(), ErrorType.VALIDATION, e.getErrorType());
      assertTrue("Unexpected error message", e.getOriginalMessage().contains(err));
    }
  }

  @Test
  public void testPassword() throws Exception {
    good("dremio123");
    good("blah1@345blah");
    bad("t", "must be at least 8 letters long");
    bad("dremio", "must be at least 8 letters long");
    bad("dremioinc", "must contain at least one number");
    good("test1234");
    bad("test123", "must be at least 8 letters long");
    good("dremio 123");
    good(" dremio123");
    good("dremio123 ");
    bad("12345678", "must contain at least one number and one letter");
    good("12345678Z");
  }

  @Test
  public void testUsers() throws Exception {
    try(final LegacyKVStoreProvider kvstore =
      LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT)) {
      kvstore.start();
      final SimpleUserService userGroupService = new SimpleUserService(() -> kvstore);
      initUserStore(kvstore, userGroupService);
      User db = SimpleUser.newBuilder().setUserName("DavidBrown").setCreatedAt(System.currentTimeMillis()).
        setEmail("david.brown@dremio.test").setFirstName("David").setLastName("Brown").build();
      User createdUser = userGroupService.createUser(db, db.getUserName() + "1");
      assertEquals(4, Iterables.size(userGroupService.getAllUsers(null)));
      assertEquals(4, Iterables.size(userGroupService.searchUsers(null, null, null, null)));
      assertEquals(3, Iterables.size(userGroupService.searchUsers(null, null, null, 3)));

      assertEquals(3, Iterables.size(userGroupService.searchUsers("David", null, null, null)));
      assertEquals(1, Iterables.size(userGroupService.searchUsers("Johnson", null, null, null)));
      assertEquals(2, Iterables.size(userGroupService.searchUsers("Mark", null, null, null)));

      // update user by userId
      User updatedUserWithId = SimpleUser.newBuilder(db).setUID(createdUser.getUID()).setEmail("db_id").build();
      updatedUserWithId = userGroupService.updateUserById(updatedUserWithId, null);
      assertEquals("db_id", userGroupService.getUser(updatedUserWithId.getUserName()).getEmail());

      // update user by userName
      db = SimpleUser.newBuilder(db).setEmail("db").build();

      // Try to update the user with invalid credentials -- expect a failure
      try {
        userGroupService.updateUser(db, "");
        fail("expected the above call to fail");
      } catch (UserException ex) {
        assertTrue(ex.getMessage().contains("Invalid password"));
      }

      db = userGroupService.updateUser(db, null);
      assertEquals("db", userGroupService.getUser(db.getUserName()).getEmail());

      assertEquals(4, Iterables.size(userGroupService.getAllUsers(null)));
      assertEquals(4, Iterables.size(userGroupService.searchUsers(null, null, null, null)));

      // login
      userGroupService.authenticate(db.getUserName(), db.getUserName() + "1");

      // update password
      db = userGroupService.updateUser(db, "abcABC123");
      // try logging in with new password.
      userGroupService.authenticate(db.getUserName(), "abcABC123");

      // update username
      final String oldUserName = db.getUserName();
      final String newUserName = "JohnBrown";
      db = SimpleUser.newBuilder(db)
          .setUserName(newUserName)
          .setFirstName("John")
          .build();
      db = userGroupService.updateUserName(oldUserName, newUserName, db, null);
      assertEquals("db", userGroupService.getUser(newUserName).getEmail());
      assertEquals("John", userGroupService.getUser(newUserName).getFirstName());

      assertEquals(4, Iterables.size(userGroupService.getAllUsers(null)));
      assertEquals(4, Iterables.size(userGroupService.searchUsers(null, null, null, null)));
      assertEquals(2, Iterables.size(userGroupService.searchUsers("David", null, null, null)));
      assertEquals(1, Iterables.size(userGroupService.searchUsers("Johnson", null, null, null)));
      assertEquals(2, Iterables.size(userGroupService.searchUsers("Mark", null, null, null)));

      try {
        userGroupService.getUser(oldUserName);
        fail(oldUserName + " should not exist");
      } catch (UserNotFoundException unfe) {
        assertTrue(unfe.getMessage() + " should contain " + oldUserName, unfe.getMessage().contains(oldUserName));
      }

      // restore user name
      db = SimpleUser.newBuilder(db).setUserName(oldUserName).build();
      db = userGroupService.updateUserName(newUserName, oldUserName, db, null);

      try {
        userGroupService.getUser(newUserName);
        fail(newUserName + " should not exist");
      } catch (UserNotFoundException unfe) {
        assertTrue(unfe.getMessage() + " should contain " + newUserName, unfe.getMessage().contains(newUserName));
      }

      // delete user
      userGroupService.deleteUser(oldUserName, db.getVersion());

      assertEquals(3, Iterables.size(userGroupService.getAllUsers(null)));
      assertEquals(3, Iterables.size(userGroupService.searchUsers(null, null, null, null)));
      assertEquals(2, Iterables.size(userGroupService.searchUsers("David", null, null, null)));
      assertEquals(1, Iterables.size(userGroupService.searchUsers("Johnson", null, null, null)));
      assertEquals(2, Iterables.size(userGroupService.searchUsers("Mark", null, null, null)));

    }
  }

  @Test
  public void testUpdateUser() throws Exception {
    try(final LegacyKVStoreProvider kvstore =
          LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT)) {
      kvstore.start();
      final SimpleUserService userGroupService = new SimpleUserService(() -> kvstore);
      LegacyIndexedStore<UID, UserInfo> userGroupStore = kvstore.getStore(UserGroupStoreBuilder.class);
      // save an remote user
      final UID uid = new UID(UUID.randomUUID().toString());
      final UserConfig userConfig = new UserConfig()
        .setUid(uid)
        .setUserName("DavidBrown")
        .setCreatedAt(System.currentTimeMillis())
        .setType(UserType.REMOTE)
        .setEmail("david.brown@dremio.test")
        .setFirstName("David")
        .setLastName("Brown");
      userGroupStore.put(uid, new UserInfo().setConfig(userConfig));

      // verify update doesn't set the user type to local
      User updatedUserWithId = SimpleUser.newBuilder().setUID(uid).setEmail("newemail@test.com").build();
      userGroupService.updateUserById(updatedUserWithId, null);
      UserConfig updatedUserConfig = userGroupStore.get(uid).getConfig();
      // fields that cannot be updated
      assertEquals(userConfig.getUid(), updatedUserConfig.getUid());
      assertEquals(userConfig.getUserName(), updatedUserConfig.getUserName());
      assertEquals(userConfig.getType(), updatedUserConfig.getType());
      assertEquals(userConfig.getCreatedAt(), updatedUserConfig.getCreatedAt());
      // fields that must be updated
      assertEquals("newemail@test.com", updatedUserConfig.getEmail());
      // fields that should be intact
      assertEquals(userConfig.getFirstName(), updatedUserConfig.getFirstName());
      assertEquals(userConfig.getLastName(), updatedUserConfig.getLastName());
      assertTrue(updatedUserConfig.getModifiedAt() > updatedUserConfig.getCreatedAt());

      // cleanup
      userGroupService.deleteUser(uid);
    }
  }

  @Test
  public void testUsersDeactivation() throws Exception {
    try(final LegacyKVStoreProvider kvstore =
          LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT)) {
      kvstore.start();
      final SimpleUserService userGroupService = new SimpleUserService(() -> kvstore);
      initUserStore(kvstore, userGroupService);

      // setup: create an active user
      User user = SimpleUser.newBuilder().setUserName("DavidBrown").setCreatedAt(System.currentTimeMillis()).
        setEmail("david.brown@dremio.test").setFirstName("David").setLastName("Brown").build(); // `active` default to true
      String password = "password123!";
      assertEquals(3, StreamSupport.stream(userGroupService.getAllUsers(null).spliterator(), false)
        .filter(User::isActive).count());
      User activeUser = userGroupService.createUser(user, password);
      assertEquals(4, StreamSupport.stream(userGroupService.getAllUsers(null).spliterator(), false)
        .filter(User::isActive).count());

      // deactivate the user
      User inactiveUser = SimpleUser.newBuilder(activeUser).setActive(false).build();
      User updatedUser = userGroupService.updateUser(inactiveUser, null);
      assertFalse(userGroupService.getUser(updatedUser.getUID()).isActive());

      // inactive users should appear in listing
      Iterable<User> listAllUsers = (Iterable<User>) userGroupService.getAllUsers(null);
      assertEquals(4, StreamSupport.stream(listAllUsers.spliterator(), false).count());
      assertEquals(1, StreamSupport.stream(listAllUsers.spliterator(), false)
        .filter(userGroup -> !userGroup.isActive()).count());

      // login as an inactive user should fail
      try {
        userGroupService.authenticate(updatedUser.getUserName(), password);
        fail("Login as an inactive user should fail with StatusUserLoginException");
      } catch (StatusUserLoginException e) {
        assertEquals(Status.INACTIVE, e.getErrorStatus());
        assertTrue(e.getMessage().contains("Inactive user"));
      } catch (Exception e) {
        fail("Login as an inactive user should fail with StatusUserLoginException");
      }

      // activate the inactive user
      User activateInactiveUser = SimpleUser.newBuilder(updatedUser).setActive(true).build();
      User updatedUser2 = userGroupService.updateUser(activateInactiveUser, null);
      assertTrue(userGroupService.getUser(updatedUser2.getUID()).isActive());
      assertEquals(0, StreamSupport.stream(userGroupService.getAllUsers(null).spliterator(), false)
        .filter(userGroup -> !userGroup.isActive()).count());

      // login as an active user should pass
      userGroupService.authenticate(updatedUser2.getUserName(), password);
    }
  }

  /**
   * write now we support a case sensitive search but direct match is not required. So we could search using a substring
   * See {@code initUserStore} for initial data
   */
  @Test
  public void testSearch() throws Exception {
    try(final LegacyKVStoreProvider kvstore =
      LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT)) {
      kvstore.start();
      final SimpleUserService userGroupService = new SimpleUserService(() -> kvstore);
      initUserStore(kvstore, userGroupService);

      // direct match works
      assertEquals(2, Iterables.size(userGroupService.searchUsers("David", null, null, null))); // first and last name match
      assertEquals(1, Iterables.size(userGroupService.searchUsers("Johnson", null, null, null))); // last name match
      assertEquals(2, Iterables.size(userGroupService.searchUsers("Mark", null, null, null))); // first name match
      // substring search should work
      assertEquals(2, Iterables.size(userGroupService.searchUsers("avi", null, null, null))); // first and last name match
      assertEquals(1, Iterables.size(userGroupService.searchUsers("k.j", null, null, null))); // mark.johnson@dremio.test match
      assertEquals(2, Iterables.size(userGroupService.searchUsers("@dremio", null, null, null))); // email match
      assertEquals(1, Iterables.size(userGroupService.searchUsers("rkda", null, null, null))); // markdavid@gmail.com match
      assertEquals(3, Iterables.size(userGroupService.searchUsers("a", null, null, null)));
    }
  }

  /**
   * Init a store with 3 users:
   * First_Name   Last_Name   Email
   * Mark         David       markdavid@gmail.com
   * David        Wilson      david@dremio.test
   * Mark         Johnson     mark.johnson@dremio.test
   * @param kvstore
   * @param userGroupService
   * @throws Exception
   */
  private final void initUserStore(final LegacyKVStoreProvider kvstore, final SimpleUserService userGroupService) throws Exception {
    User md = SimpleUser.newBuilder().setUserName("MarkDavid").setCreatedAt(System.currentTimeMillis()).
      setEmail("markdavid@gmail.com").setFirstName("Mark").setLastName("David").build();

    User dw = SimpleUser.newBuilder().setUserName("DavidWilson").setCreatedAt(System.currentTimeMillis()).
      setEmail("david@dremio.test").setFirstName("David").setLastName("Wilson").build();

    User mj = SimpleUser.newBuilder().setUserName("MarkJohnson").setCreatedAt(System.currentTimeMillis()).
      setEmail("mark.johnson@dremio.test").setFirstName("Mark").setLastName("Johnson").build();

    userGroupService.createUser(md, md.getUserName() + "1");
    userGroupService.createUser(dw, dw.getUserName() + "1");
    userGroupService.createUser(mj, mj.getUserName() + "1");

    assertEquals(3, Iterables.size(userGroupService.getAllUsers(null)));
  }

  @Test
  public void testSearchQueryFilter() throws Exception {
    try(final LegacyKVStoreProvider kvstore =
          LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT)) {
      kvstore.start();
      final SimpleUserService userGroupService = new SimpleUserService(() -> kvstore);
      initUserStore(kvstore, userGroupService);

      Iterable<? extends User> users = userGroupService.searchUsers(
        SearchQueryUtils.newTermQuery("USERNAME", "MarkDavid"),
        null,
        null,
        null,
        null);
      assertEquals(1, Iterables.size(users));
    }
  }

  @Test
  public void testSearchQuerySort() throws Exception {
    try(final LegacyKVStoreProvider kvstore =
          LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT)) {
      kvstore.start();
      final SimpleUserService userGroupService = new SimpleUserService(() -> kvstore);
      initUserStore(kvstore, userGroupService);

      Iterable<? extends User> users = userGroupService.searchUsers(
        SearchQueryUtils.newMatchAllQuery(),
        "name",
        SearchTypes.SortOrder.DESCENDING,
        null,
        null);
      List<String> names = StreamSupport.stream(users.spliterator(), false)
        .map(User::getUserName)
        .collect(Collectors.toList());
      assertEquals(Arrays.asList("MarkJohnson", "MarkDavid", "DavidWilson"), names);
    }
  }

  @Test
  public void testUserNameCaseSensitivity() throws Exception {
    try(final LegacyKVStoreProvider kvstore =
          LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT)) {
      kvstore.start();
      final SimpleUserService userGroupService = new SimpleUserService(() -> kvstore);
      initUserStore(kvstore, userGroupService);
      User testUser = SimpleUser.newBuilder().setUserName("testUser").setCreatedAt(System.currentTimeMillis()).
        setEmail("testUser@dremio.test").setFirstName("test").setLastName("user").build();
      userGroupService.createUser(testUser, testUser.getUserName() + "1");

      User testUser2 = SimpleUser.newBuilder().setUserName("testuseR").setCreatedAt(System.currentTimeMillis()).
        setEmail("testUser@dremio.test").setFirstName("test").setLastName("user").build();

      // Try to update the user with invalid credentials -- expect a failure
      try {
        userGroupService.createUser(testUser2, testUser2.getUserName() + "1");
        fail("expected the above call to fail");
      } catch (Exception ex) {
        assertTrue(ex instanceof UserAlreadyExistException);
      }
    }
  }

  @Test
  public void testDeleteUserWithVersion() throws Exception {
    try (final LegacyKVStoreProvider kvstore =
           LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT)) {
      kvstore.start();
      final SimpleUserService userGroupService = new SimpleUserService(() -> kvstore);
      LegacyIndexedStore<UID, UserInfo> userGroupStore = kvstore.getStore(UserGroupStoreBuilder.class);
      // save an remote user
      final UID uid = new UID(UUID.randomUUID().toString());
      final UserConfig userConfig = new UserConfig()
        .setUid(uid)
        .setUserName("DavidBrown")
        .setCreatedAt(System.currentTimeMillis())
        .setType(UserType.REMOTE)
        .setEmail("david.brown@dremio.test")
        .setFirstName("David")
        .setLastName("Brown");
      userGroupStore.put(uid, new UserInfo().setConfig(userConfig));

      // cleanup
      userGroupService.deleteUser(uid, userConfig.getTag());
    }
  }

  @Test
  public void testDeleteUserWithWrongVersion() throws Exception {
    try(final LegacyKVStoreProvider kvstore =
          LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT)) {
      kvstore.start();
      final SimpleUserService userGroupService = new SimpleUserService(() -> kvstore);
      final LegacyIndexedStore<UID, UserInfo> userGroupStore = kvstore.getStore(UserGroupStoreBuilder.class);
      // save an remote user
      final UID uid = new UID(UUID.randomUUID().toString());
      final UserConfig userConfig = new UserConfig()
        .setUid(uid)
        .setUserName("DavidBrown")
        .setCreatedAt(System.currentTimeMillis())
        .setType(UserType.REMOTE)
        .setEmail("david.brown@dremio.test")
        .setFirstName("David")
        .setLastName("Brown");
      userGroupStore.put(uid, new UserInfo().setConfig(userConfig));

      // cleanup
      assertThatThrownBy(() -> userGroupService.deleteUser(uid, userConfig.getTag() + UUID.randomUUID()))
        .isInstanceOf(ConcurrentModificationException.class);
    }
  }

  @Test
  public void testGetSystemUser() throws Exception {
    try(final LegacyKVStoreProvider kvstore =
          LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT)) {
      kvstore.start();
      final SimpleUserService userService = new SimpleUserService(() -> kvstore);

      assertEquals(SystemUser.SYSTEM_USER, userService.getUser(SystemUser.SYSTEM_USERNAME));
      assertEquals(SystemUser.SYSTEM_USER, userService.getUser(SystemUser.SYSTEM_USER.getUID()));
    }
  }
}
