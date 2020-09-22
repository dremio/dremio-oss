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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
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
      userGroupService.createUser(db, db.getUserName() + "1");
      assertEquals(4, Iterables.size(userGroupService.getAllUsers(null)));
      assertEquals(4, Iterables.size(userGroupService.searchUsers(null, null, null, null)));
      assertEquals(3, Iterables.size(userGroupService.searchUsers(null, null, null, 3)));

      assertEquals(3, Iterables.size(userGroupService.searchUsers("David", null, null, null)));
      assertEquals(1, Iterables.size(userGroupService.searchUsers("Johnson", null, null, null)));
      assertEquals(2, Iterables.size(userGroupService.searchUsers("Mark", null, null, null)));

      // update user
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
}
