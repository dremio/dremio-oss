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
package com.dremio.dac.model.usergroup;

import com.dremio.service.users.User;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

/**
 * Create/Edit user form.
 */
public class UserForm {
  private final User userConfig;
  private final String password;

  @JsonCreator
  public UserForm(@JsonUnwrapped User userConfig,
                  @JsonProperty("password") String password) {
    this.userConfig = userConfig;
    this.password = password;
  }

  public UserForm(User userGroup) {
    this.userConfig = userGroup;
    this.password = null;
  }

  public UserForm(String password) {
    this.password = password;
    this.userConfig = null;
  }

  @JsonUnwrapped
  public User getUserConfig() {
    return userConfig;
  }

  public String getPassword() {
    return password;
  }
}
