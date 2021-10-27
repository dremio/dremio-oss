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
  public final String role_name;
  public final String source;
  public final String role_type;
  public final String owner_name;
  public final String owner_type;

  /**
   * enum for sources of a role
   */
  public enum RoleSource {
    LOCAL, EXTERNAL
  }

  public SysTableRoleInfo(String role_name, String source, String role_type, String owner_name, String owner_type) {
    this.role_name = role_name;
    this.source = source;
    this.role_type = role_type;
    this.owner_name = owner_name;
    this.owner_type = owner_type;
  }

  public String getRole_name() {
    return role_name;
  }

  public String getSource() {
    return source;
  }

  public String getRole_type() {
    return role_type;
  }

  public String getOwner_name() {
    return owner_name;
  }

  public String getOwner_type() {
    return owner_type;
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
    return Objects.equals(role_name, that.role_name) &&
      Objects.equals(source, that.source) &&
      Objects.equals(role_type, that.role_type) &&
      Objects.equals(owner_name, that.owner_name) &&
      Objects.equals(owner_type, that.owner_type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(role_name, source, role_type);
  }

  @Override
  public String toString() {
    return "SysTableRole{" +
      "role_name='" + role_name + '\'' +
      ", source='" + source + '\'' +
      ", role_type='" + role_type + '\'' +
      ", owner_name='" + owner_name + '\'' +
      ", owner_type='" + owner_type + '\'' +
      '}';
  }
}
