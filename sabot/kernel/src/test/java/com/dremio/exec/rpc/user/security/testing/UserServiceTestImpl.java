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
package com.dremio.exec.rpc.user.security.testing;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.service.users.AuthResult;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserLoginException;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;
import com.google.common.base.StandardSystemProperty;

/**
 * Implement {@link UserService} for testing:
 * + UserAuthenticator and authentication of users from Java client to SabotNode.
 * + {@link TestInboundImpersonation user delegation}.
 */
public class UserServiceTestImpl implements UserService {
  public static final String ANONYMOUS = "anonymous";
  public static final String TEST_USER_1 = "testUser1";
  public static final String TEST_USER_2 = "testUser2";
  public static final String TEST_USER_3 = "test";
  public static final String ADMIN_USER = "admin";
  public static final String PROCESS_USER = StandardSystemProperty.USER_NAME.value();
  public static final String TEST_USER_1_PASSWORD = "testUser1Password";
  public static final String TEST_USER_2_PASSWORD = "testUser2Password";
  public static final String TEST_USER_3_PASSWORD = "testPassword";
  public static final String ADMIN_USER_PASSWORD = "adminUserPw";
  public static final String PROCESS_USER_PASSWORD = "processUserPw";

  public static final String DEFAULT_PASSWORD = "dremio123";

  private final Map<String, String> userPasswordMap = new HashMap<String, String>() {{
    put(TEST_USER_1.toLowerCase(Locale.ROOT), TEST_USER_1_PASSWORD);
    put(TEST_USER_2.toLowerCase(Locale.ROOT), TEST_USER_2_PASSWORD);
    put(TEST_USER_3.toLowerCase(Locale.ROOT), TEST_USER_3_PASSWORD);
    put(ADMIN_USER.toLowerCase(Locale.ROOT), ADMIN_USER_PASSWORD);
    put(PROCESS_USER.toLowerCase(Locale.ROOT), PROCESS_USER_PASSWORD);
    put(ANONYMOUS.toLowerCase(Locale.ROOT), DEFAULT_PASSWORD);
  }};

  public static final String ADMIN_GROUP = "admingrp";

  static {
    UserGroupInformation.createUserForTesting("testUser1", new String[]{"g1", ADMIN_GROUP});
    UserGroupInformation.createUserForTesting("testUser2", new String[]{ "g1" });
    UserGroupInformation.createUserForTesting("admin", new String[]{ ADMIN_GROUP });
  }

  @Override
  public User getUser(UID uid) throws UserNotFoundException {
    final String userName = uid.getId();

    if (userPasswordMap.containsKey(userName.toLowerCase(Locale.ROOT))) {
      return SimpleUser.newBuilder()
        .setUID(new UID(userName))
        .setUserName(userName)
        .setEmail(userName + "@dremio.test")
        .setFirstName(userName + " FN")
        .setLastName(userName + " LN")
        .build();
    }

    if (userName.equals(SystemUser.SYSTEM_USERNAME)) {
      return SystemUser.SYSTEM_USER;
    }

    throw new UserNotFoundException(userName);
  }

  @Override
  public User getUser(String userName) throws UserNotFoundException {
    if (userPasswordMap.containsKey(userName.toLowerCase(Locale.ROOT))) {
      return SimpleUser.newBuilder()
        .setUID(new UID(userName))
        .setUserName(userName)
        .setEmail(userName + "@dremio.test")
        .setFirstName(userName + " FN")
        .setLastName(userName + " LN")
        .build();
    }

    if (userName.equals(SystemUser.SYSTEM_USERNAME)) {
      return SystemUser.SYSTEM_USER;
    }

    throw new UserNotFoundException(userName);
  }

  @Override
  public User createUser(User userConfig, String authKey)
      throws IOException, IllegalArgumentException {
    userPasswordMap.put(userConfig.getUserName().toLowerCase(Locale.ROOT), authKey);
    return userConfig;
  }

  @Override
  public User updateUser(User userConfig, String authKey)
      throws IOException, IllegalArgumentException, UserNotFoundException {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public User updateUserName(String oldUserName, String newUserName, User userConfig, String authKey)
      throws IOException, IllegalArgumentException, UserNotFoundException {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public void deleteUser(String userName, String version) throws UserNotFoundException, IOException {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public AuthResult authenticate(String userName, String password) throws UserLoginException {
    String user = userName;
    if (ANONYMOUS.equals(user)) {
      // Allow user "anonymous" for test framework to work.
      return AuthResult.of(ANONYMOUS);
    }

    String lcUser = userName.toLowerCase(Locale.ROOT);

    if (
        !(PROCESS_USER.equals(user) && PROCESS_USER_PASSWORD.equals(password)) &&
        !(userPasswordMap.containsKey(lcUser) && password.equals(userPasswordMap.get(lcUser)))) {
      throw new UserLoginException(userName, "Invalid credentials.");
    }

    return AuthResult.of(userName);
  }

  @Override
  public List<User> getAllUsers(Integer pageSize)
      throws IOException {
    return null;
  }

  @Override
  public boolean hasAnyUser() throws IOException {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public List<User> searchUsers(String searchTerm, String sortColumn, SortOrder order, Integer pageSize)
      throws IOException {
    return null;
  }
}
