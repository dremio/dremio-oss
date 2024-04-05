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
package com.dremio.dac.daemon;

import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.service.users.AuthResult;
import com.dremio.service.users.User;
import com.dremio.service.users.UserLoginException;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;
import java.io.IOException;

/** A special user service for executors which always throws UnsupportedOperationException */
public class ExecutorUserService implements UserService {

  @Override
  public User getUser(String userName) throws UserNotFoundException {
    throw new UnsupportedOperationException("User service is not available on executors");
  }

  @Override
  public User getUser(UID uid) throws UserNotFoundException {
    throw new UnsupportedOperationException("User service is not available on executors");
  }

  @Override
  public User createUser(User userConfig, String authKey)
      throws IOException, IllegalArgumentException {
    throw new UnsupportedOperationException("User service is not available on executors");
  }

  @Override
  public User updateUser(User userConfig, String authKey)
      throws IOException, IllegalArgumentException, UserNotFoundException {
    throw new UnsupportedOperationException("User service is not available on executors");
  }

  @Override
  public User updateUserName(
      String oldUserName, String newUserName, User userConfig, String authKey)
      throws IOException, IllegalArgumentException, UserNotFoundException {
    throw new UnsupportedOperationException("User service is not available on executors");
  }

  @Override
  public void deleteUser(String userName, String version)
      throws UserNotFoundException, IOException {
    throw new UnsupportedOperationException("User service is not available on executors");
  }

  @Override
  public AuthResult authenticate(String userName, String password) throws UserLoginException {
    throw new UnsupportedOperationException("User service is not available on executors");
  }

  @Override
  public Iterable<? extends User> getAllUsers(Integer pageSize) throws IOException {
    throw new UnsupportedOperationException("User service is not available on executors");
  }

  @Override
  public boolean hasAnyUser() throws IOException {
    throw new UnsupportedOperationException("User service is not available on executors");
  }

  @Override
  public Iterable<? extends User> searchUsers(
      String searchTerm, String sortColumn, SortOrder order, Integer limit) throws IOException {
    throw new UnsupportedOperationException("User service is not available on executors");
  }
}
