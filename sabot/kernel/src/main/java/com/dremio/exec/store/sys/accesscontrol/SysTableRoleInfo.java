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

/**
 * Schema for an entry in system table sys.roles.
 */
public class SysTableRoleInfo {
  public final String role_id;
  public final String role_name;
  public final String role_type;
  public final String owner_id;
  public final String owner_type;
  public final String created_by;

  /**
   * enum for sources of a role
   */
  public enum RoleSource {
    LOCAL, EXTERNAL
  }

  public SysTableRoleInfo(String role_id, String role_name, String role_type, String owner_id, String owner_type, String created_by) {
    this.role_id = role_id;
    this.role_name = role_name;
    this.role_type = role_type;
    this.owner_id = owner_id;
    this.owner_type = owner_type;
    this.created_by = created_by;
  }

  public String getRole_id() {
    return role_id;
  }

  public String getRole_name() {
    return role_name;
  }

  public String getRole_type() {
    return role_type;
  }

  public String getOwner_id() {
    return owner_id;
  }

  public String getOwner_type() {
    return owner_type;
  }

  public String getCreated_by() {
    return created_by;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SysTableRoleInfo that = (SysTableRoleInfo) o;
    return Objects.equals(role_id, that.role_id) &&
      Objects.equals(role_name, that.role_name) &&
      Objects.equals(role_type, that.role_type) &&
      Objects.equals(owner_id, that.owner_id) &&
      Objects.equals(owner_type, that.owner_type) &&
      Objects.equals(created_by, that.created_by);
  }

  @Override
  public int hashCode() {
    return Objects.hash(role_name, role_id, role_type);
  }

  @Override
  public String toString() {
    return "SysTableRole{" +
      "role_id='" + role_id + '\'' +
      ", role_name='" + role_name + '\'' +
      ", role_type='" + role_type + '\'' +
      ", owner_id='" + owner_id + '\'' +
      ", owner_type='" + owner_type + '\'' +
      ", created_by='" + created_by + '\'' +
      '}';
  }
}
