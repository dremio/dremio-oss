/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.ArrayList;
import java.util.List;

import com.dremio.dac.util.JSONUtil;

/**
 * Users model.
 */
public class UsersUI {
  private List<UserUI> users;

  public UsersUI() {
    users = new ArrayList<>();
  }

  public void add(UserUI user) {
    users.add(user);
  }

  public List<UserUI> getUsers() {
    return users;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }
}
