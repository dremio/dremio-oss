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
package com.dremio.dac.server.test;

import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.HomePath;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.google.common.base.StandardSystemProperty;
import java.io.IOException;

/** Utility class that provides methods to populate Dremio with data for testing */
public final class DataPopulatorUtils {
  private static final String DEFAULT_USER_NAME = "dremio";
  private static final String PASSWORD = "dremio123";
  private static final String DEFAULT_USER_FIRSTNAME = "Dre";
  private static final String DEFAULT_USER_LASTNAME = "Mio";
  private static final boolean ADD_PROCESS_USER = Boolean.getBoolean("dremio.test.add-processuser");

  private DataPopulatorUtils() {}

  public static void addDefaultDremioUser(
      final UserService userService, final NamespaceService namespaceService) throws Exception {
    if (!userService.hasAnyUser()) {
      createUserIfNotExists(
          userService,
          namespaceService,
          DEFAULT_USER_NAME,
          PASSWORD,
          DEFAULT_USER_FIRSTNAME,
          DEFAULT_USER_LASTNAME);
      // Special case for regression until we move away from views as physical files.
      // View expansion requires the user who wrote the file on the filesystem
      // to be present in the usergroup db
      if (ADD_PROCESS_USER) {
        createUserIfNotExists(
            userService,
            namespaceService,
            StandardSystemProperty.USER_NAME.value(),
            PASSWORD,
            DEFAULT_USER_FIRSTNAME,
            DEFAULT_USER_LASTNAME);
      }
    }
  }

  public static void createUserIfNotExists(
      UserService userService,
      NamespaceService ns,
      String user,
      String passwd,
      String firstName,
      String lastName)
      throws IOException, NamespaceException {
    try {
      userService.getUser(user);
    } catch (UserNotFoundException e) {
      User userConfig =
          SimpleUser.newBuilder()
              .setUserName(user)
              .setEmail(user + "@dremio.test")
              .setFirstName(firstName)
              .setLastName(lastName)
              .build();
      userService.createUser(userConfig, passwd);
    }

    // Now create the home folder for the user
    createHomeDir(ns, user);
  }

  private static void createHomeDir(NamespaceService ns, String user) throws NamespaceException {
    final NamespaceKey homeKey = new HomePath(HomeName.getUserHomePath(user)).toNamespaceKey();
    if (!ns.exists(homeKey, NameSpaceContainer.Type.HOME)) {
      final HomeConfig homeConfig = new HomeConfig().setOwner(user);
      ns.addOrUpdateHome(homeKey, homeConfig);
    }
  }
}
