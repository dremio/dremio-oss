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

/**
 * System table entry for a privilege.
 */
public class SysTablePrivilegeInfo {
  public final String grantee_type;
  public final String grantee;
  public final String object_type;
  public final String object;
  public final String privilege;

  public SysTablePrivilegeInfo(String grantee_type,
                               String grantee,
                               String object_type,
                               String object,
                               String privilege) {
    this.grantee_type = grantee_type;
    this.grantee = grantee;
    this.object_type = object_type;
    this.object = object;
    this.privilege = privilege;
  }

  public String getGrantee_type() {
    return grantee_type;
  }

  public String getGrantee() {
    return grantee;
  }

  public String getObject_type() {
    return object_type;
  }

  public String getObject() {
    return object;
  }

  public String getPrivilege() {
    return privilege;
  }
}
