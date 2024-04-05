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
package com.dremio.exec.store.sys.accesscontrol;

import java.util.Objects;

/** Schema for an entry in system table sys.roles. */
public class SysTableUserInfo {
  public final String user_name;
  public final UserSource source;
  public final String owner_name;
  public final String owner_type;

  /** enum for sources of a role */
  public enum UserSource {
    LOCAL,
    EXTERNAL
  }

  public SysTableUserInfo(
      final String user_name,
      final UserSource source,
      final String owner_name,
      final String owner_type) {
    this.user_name = user_name;
    this.source = source;
    this.owner_name = owner_name;
    this.owner_type = owner_type;
  }

  public String getUserName() {
    return user_name;
  }

  public UserSource getSource() {
    return source;
  }

  public String getOwner_name() {
    return owner_name;
  }

  public String getOwner_type() {
    return owner_type;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SysTableUserInfo that = (SysTableUserInfo) o;
    return user_name.equals(that.user_name)
        && source.equals(that.source)
        && owner_name.equals(that.owner_name)
        && owner_type.equals(that.owner_type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(user_name, source, owner_name, owner_type);
  }

  @Override
  public String toString() {
    return "SysTableUserInfo{"
        + "user_name='"
        + user_name
        + '\''
        + ", source='"
        + source
        + '\''
        + ", owner_name='"
        + owner_name
        + '\''
        + ", owner_type='"
        + owner_type
        + '\''
        + '}';
  }
}
