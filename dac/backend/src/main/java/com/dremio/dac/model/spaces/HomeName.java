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
package com.dremio.dac.model.spaces;

import com.dremio.dac.model.common.RootEntity;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Home space names start with a reserved letter @followed by user name.
 */
public final class HomeName extends RootEntity {

  public static final String HOME_PREFIX = "@";
  public static final String HOME_URL = "home";

  public static final HomeName getUserHomePath(final String userName) {
    return new HomeName(HOME_PREFIX + userName);
  }

  @JsonCreator
  public HomeName(String name) {
    super(name);
    if (!name.startsWith(HOME_PREFIX)) {
      throw new IllegalArgumentException("Invalid home name: " + name);
    }
  }

  @Override
  public String getRootUrl() {
    return HOME_URL;
  }

  @Override
  public RootType getRootType() {
    return RootType.HOME;
  }

  public String getUserName() {
    return this.getName().substring(1);
  }
}
