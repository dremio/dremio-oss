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

/** Schema for system table entry for sys.membership. */
public class SysTableMembershipInfo {
  public final String role_name;
  public final String member_name;
  public final String member_type;

  public SysTableMembershipInfo(String role_name, String member_name, String member_type) {
    this.role_name = role_name;
    this.member_name = member_name;
    this.member_type = member_type;
  }

  public String getRole_name() {
    return role_name;
  }

  public String getMember_name() {
    return member_name;
  }

  public String getMember_type() {
    return member_type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SysTableMembershipInfo that = (SysTableMembershipInfo) o;
    return Objects.equals(role_name, that.role_name)
        && Objects.equals(member_name, that.member_name)
        && Objects.equals(member_type, that.member_type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(role_name, member_name, member_type);
  }

  @Override
  public String toString() {
    return "SysTableMembership{"
        + "role_name='"
        + role_name
        + '\''
        + ", member_name='"
        + member_name
        + '\''
        + ", member_type='"
        + member_type
        + '\''
        + '}';
  }
}
