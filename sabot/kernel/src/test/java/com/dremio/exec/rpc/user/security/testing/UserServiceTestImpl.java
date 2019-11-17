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
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.datastore.SearchTypes.SortOrder;
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
  public static final String TEST_USER_1 = "testUser1";
  public static final String TEST_USER_2 = "testUser2";
  public static final String ADMIN_USER = "admin";
  public static final String PROCESS_USER = StandardSystemProperty.USER_NAME.value();
  public static final String TEST_USER_1_PASSWORD = "testUser1Password";
  public static final String TEST_USER_2_PASSWORD = "testUser2Password";
  public static final String ADMIN_USER_PASSWORD = "adminUserPw";
  public static final String PROCESS_USER_PASSWORD = "processUserPw";

  public static final String ADMIN_GROUP = "admingrp";

  static {
    UserGroupInformation.createUserForTesting("testUser1", new String[]{"g1", ADMIN_GROUP});
    UserGroupInformation.createUserForTesting("testUser2", new String[]{ "g1" });
    UserGroupInformation.createUserForTesting("admin", new String[]{ ADMIN_GROUP });
  }

  @Override
  public User getUser(UID uid) throws UserNotFoundException {
    final String userName = uid.getId();
    switch (userName) {
    case TEST_USER_1:
    case TEST_USER_2:
    case ADMIN_USER:
    case "anonymous":
      return SimpleUser.newBuilder()
          .setUID(new UID(userName))
          .setUserName(userName)
          .setEmail(userName + "@dremio.test")
          .setFirstName(userName + " FN")
          .setLastName(userName + " LN")
          .build();

    case SystemUser.SYSTEM_USERNAME:
      return SystemUser.SYSTEM_USER;
    default:
      throw new UserNotFoundException(userName);
    }
  }

  @Override
  public User getUser(String userName) throws UserNotFoundException {
    switch (userName) {
      case TEST_USER_1:
      case TEST_USER_2:
      case ADMIN_USER:
      case "anonymous":
        return SimpleUser.newBuilder()
            .setUID(new UID(userName))
            .setUserName(userName)
            .setEmail(userName + "@dremio.test")
            .setFirstName(userName + " FN")
            .setLastName(userName + " LN")
            .build();
      case SystemUser.SYSTEM_USERNAME:
        return SystemUser.SYSTEM_USER;
      default:
        throw new UserNotFoundException(userName);
    }
  }

  @Override
  public User createUser(User userConfig, String authKey)
      throws IOException, IllegalArgumentException {
    throw new UnsupportedOperationException("not supported");
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
  public void authenticate(String userName, String password) throws UserLoginException {
    String user = userName;
    if ("anonymous".equals(user)) {
      // Allow user "anonymous" for test framework to work.
      return;
    }

    if (
        !(PROCESS_USER.equals(user) && PROCESS_USER_PASSWORD.equals(password)) &&
            /**
             * Used in {@link com.dremio.exec.rpc.user.security.TestCustomUserAuthenticator}
             */
            !(TEST_USER_1.equals(user) && TEST_USER_1_PASSWORD.equals(password)) &&
            !(TEST_USER_2.equals(user) && TEST_USER_2_PASSWORD.equals(password)) &&
            !(ADMIN_USER.equals(user) && ADMIN_USER_PASSWORD.equals(password))) {
      throw new UserLoginException(userName, "Invalid credentials.");
    }
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
