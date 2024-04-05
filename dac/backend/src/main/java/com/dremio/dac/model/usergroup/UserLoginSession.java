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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Create/Validates User login sessions using cookie. */
public class UserLoginSession {

  private final String token;
  private final String userName;
  private final String firstName;
  private final String lastName;
  private final long expires;
  private final String email;
  private final String userId;
  private final boolean admin;
  private final Long userCreatedAt;
  private final String clusterId;
  private final String version;
  private final long clusterCreatedAt;
  private final SessionPermissions permissions;

  @JsonCreator
  public UserLoginSession(
      @JsonProperty("token") String token,
      @JsonProperty("userName") String userName,
      @JsonProperty("firstName") String firstName,
      @JsonProperty("lastName") String lastName,
      @JsonProperty("expires") long expires,
      @JsonProperty("email") String email,
      @JsonProperty("userId") String userId,
      @JsonProperty("admin") boolean admin,
      @JsonProperty("userCreateTime") Long userCreatedAt,
      @JsonProperty("clusterId") String clusterId,
      @JsonProperty("clusterCreatedAt") long clusterCreatedAt,
      @JsonProperty("version") String version,
      @JsonProperty("permissions") SessionPermissions permissions) {
    this.token = token;
    this.userName = userName;
    this.firstName = firstName;
    this.lastName = lastName;
    this.expires = expires;
    this.userId = userId;
    this.admin = admin;
    this.email = email;
    this.userCreatedAt = userCreatedAt;
    this.clusterId = clusterId;
    this.version = version;
    this.clusterCreatedAt = clusterCreatedAt;
    this.permissions = permissions;
  }

  public long getClusterCreatedAt() {
    return clusterCreatedAt;
  }

  public String getVersion() {
    return version;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getUserName() {
    return userName;
  }

  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public String getEmail() {
    return email;
  }

  public String getUserId() {
    return userId;
  }

  public boolean isAdmin() {
    return admin;
  }

  public long getExpires() {
    return expires;
  }

  public String getToken() {
    return token;
  }

  public Long getUserCreatedAt() {
    return userCreatedAt;
  }

  public SessionPermissions getPermissions() {
    return permissions;
  }
}
