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
package com.dremio.service.users;


import java.io.IOException;

import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.service.users.proto.UID;

/**
 * User service interface.
 */
public interface UserService {

  // Only admin and logged in user should be able to get user's info.
  User getUser(String userName) throws UserNotFoundException;

  User getUser(UID uid) throws UserNotFoundException;

  // Admin only.
  User createUser(User userConfig, String authKey) throws IOException, IllegalArgumentException;

  // Edit user. User only.
  User updateUser(User userConfig, String authKey) throws IOException, IllegalArgumentException, UserNotFoundException;

  User updateUserName(String oldUserName, String newUserName, User userConfig, String authKey) throws IOException, IllegalArgumentException, UserNotFoundException;

  void deleteUser(String userName, String version) throws UserNotFoundException, IOException;

  void authenticate(String userName, String password) throws UserLoginException;

  Iterable<? extends User> getAllUsers(Integer pageSize) throws IOException;

  boolean hasAnyUser() throws IOException;

  Iterable<? extends User> searchUsers(final String searchTerm, String sortColumn, SortOrder order, Integer limit) throws IOException;
}
