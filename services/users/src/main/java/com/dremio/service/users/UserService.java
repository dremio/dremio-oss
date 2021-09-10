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


import java.io.IOException;

import com.dremio.datastore.SearchTypes;
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

  default User createUser(User userConfig) throws IOException, IllegalArgumentException{
    return createUser(userConfig, null);
  }

  /**
   * Fetch original user by userName and replace its info.
   */
  // Edit user. User only.
  User updateUser(User userConfig, String authKey) throws IOException, IllegalArgumentException, UserNotFoundException;

  /**
   * Fetch original user by userId and replace its info.
   */
  default User updateUserById(
    User userConfig,
    String authKey
  ) throws IllegalArgumentException, UserNotFoundException {
    throw new UnsupportedOperationException("Not yet supported");
  }

  User updateUserName(String oldUserName, String newUserName, User userConfig, String authKey) throws IOException, IllegalArgumentException, UserNotFoundException;

  void deleteUser(String userName, String version) throws UserNotFoundException, IOException;

  /**
   * Delete a user with corresponding UID
   * @param uid UID of user to delete
   * @throws UserNotFoundException if no user with UID is found
   */
  default void deleteUser(UID uid) throws UserNotFoundException, IOException {
    throw new UnsupportedOperationException();
  }

  // TODO(DX-33891): use @CheckReturnValue
  AuthResult authenticate(String userName, String password) throws UserLoginException;

  Iterable<? extends User> getAllUsers(Integer pageSize) throws IOException;

  boolean hasAnyUser() throws IOException;

  /**
   * Performs case SENSITIVE search for the users.
   *
   * Search looking through full name, first name, last name and email fields and returns any record that has
   * {@code searchTerm} as substring.
   * @param searchTerm - a string to search
   * @param sortColumn - sort column
   * @param order - sort order
   * @param limit - number of records to return
   * @return a collection of users that not exceeds {@code limit} number
   * @throws IOException
   */
  Iterable<? extends User> searchUsers(final String searchTerm, String sortColumn, SortOrder order, Integer limit) throws IOException;

  /**
   * Performs case SENSITIVE search for the users.
   *
   * Search looking through full name, first name, last name and email fields and returns any record that has
   * @param searchQuery query to search by
   * @param sortColumn - sort column
   * @param order - sort order
   * @param startIndex - starting offset to return for pagination
   * @param pageSize - size of page to return for pagination
   * @return a collection of users that not exceeds {@code limit} number
   * @throws IOException
   */
  default Iterable<? extends User> searchUsers(
    SearchTypes.SearchQuery searchQuery,
    String sortColumn,
    SortOrder order,
    Integer startIndex,
    Integer pageSize
  ) throws IOException {
    throw new UnsupportedOperationException("Not yet supported");
  }

  /**
   * Provide a count of the number of users entries that match the
   * requested condition.
   *
   * @param searchQuery - query to search by, defaults to match all
   * @return number of users matching the searchTerm
   */
   default Integer getNumUsers(SearchTypes.SearchQuery searchQuery) {
    throw new UnsupportedOperationException("Not yet supported");
  }
}
