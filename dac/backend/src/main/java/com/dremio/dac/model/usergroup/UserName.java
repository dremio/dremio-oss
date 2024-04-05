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

import com.dremio.common.utils.SqlUtils;
import com.dremio.dac.model.common.Name;
import com.fasterxml.jackson.annotation.JsonCreator;

/** User name. */
public class UserName extends Name {
  @JsonCreator
  public UserName(String name) {
    super(name);
    // DX-8156 - " and : currently cause trouble.  @Pattern doesn't work here, probably because the
    // JSON is deserialized
    // to a UserForm.
    if (name.contains(String.valueOf(SqlUtils.QUOTE)) || name.contains(":")) {
      throw new IllegalArgumentException(
          String.format("Username “%s” can not contain quotes or colons.", name));
    }
  }
}
