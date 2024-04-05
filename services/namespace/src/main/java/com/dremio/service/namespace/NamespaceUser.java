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
package com.dremio.service.namespace;

import com.dremio.service.users.User;
import com.google.inject.Provider;

/** Represents a namespace user. */
public class NamespaceUser implements NamespaceIdentity {
  private final Provider<User> userProvider;

  public NamespaceUser(Provider<User> userProvider) {
    this.userProvider = userProvider;
  }

  @Override
  public String getName() {
    return userProvider.get().getUserName();
  }

  @Override
  public String getId() {
    return userProvider.get().getUID().getId();
  }

  public User getUser() {
    return userProvider.get();
  }
}
