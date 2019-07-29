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
package com.dremio.dac.model.usergroup;

import static java.util.Arrays.asList;

import java.util.List;

import com.dremio.dac.model.common.ResourcePath;

/**
 * User resource path /user/{username}
 */
public class UserResourcePath extends ResourcePath {

  private final UserName userName;

  public UserResourcePath(UserName userName) {
    this.userName = userName;
  }

  public UserResourcePath(String userPath) {
    List<String> path = parse(userPath, "user");
    if (path.size() != 1) {
      throw new IllegalArgumentException("path should be of form: /user/{userName}, found " + userPath);
    }
    this.userName = new UserName(path.get(0));
  }

  @Override
  public List<?> asPath() {
    return asList("user", userName.getName());
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
