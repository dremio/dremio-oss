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
package com.dremio.dac.service.users;

import java.io.IOException;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.HomePath;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;

/**
 * UserService Helper
 */
public class UserServiceHelper {
  private static final Logger logger = LoggerFactory.getLogger(UserServiceHelper.class);

  private final UserService userService;
  private final HomeFileTool fileStore;
  private final NamespaceService namespaceService;

  @Inject
  public UserServiceHelper(UserService userService, HomeFileTool fileStore, NamespaceService namespaceService) {
    this.userService = userService;
    this.fileStore = fileStore;
    this.namespaceService = namespaceService;
  }

  public boolean deleteUser(UID uid) throws UserNotFoundException, IOException {
    final String userName = userService.getUser(uid).getUserName();
    userService.deleteUser(uid);
    return handleHomeSpace(userName);
  }

  public boolean deleteUser(String userName, String version) throws IOException, UserNotFoundException {
    userService.deleteUser(userName, version);
    return handleHomeSpace(userName);
  }

  private boolean handleHomeSpace(String userName) {
    try {
      final NamespaceKey homeKey = new HomePath(HomeName.getUserHomePath(userName)).toNamespaceKey();
      final HomeConfig homeConfig = namespaceService.getHome(homeKey);
      namespaceService.deleteHome(homeKey, homeConfig.getTag());
    } catch (NamespaceNotFoundException ex) {
      logger.debug("Home space is not found", ex);
    } catch (NamespaceException ex) {
      logger.error("Failed to delete home space for user '{}'", userName, ex);
      return false;
    }

    try {
      fileStore.deleteHomeAndContents(HomeName.getUserHomePath(userName).getName());
    } catch (Exception e) {
      logger.error("Failed to delete user home contents '{}", userName, e);
      return false;
    }

    return true;
  }
}
