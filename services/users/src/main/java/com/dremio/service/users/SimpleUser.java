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
package com.dremio.service.users;

import java.util.Objects;

import com.dremio.service.users.proto.UID;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Object representing a Dremio user.
 */
public final class SimpleUser implements User {
  /**
   * A builder for {@code User}
   */
  public static final class Builder {
    private UID uid;
    private String userName;
    private String firstName;
    private String lastName;
    private String email;
    private long createdAt;
    private long modifiedAt;
    private String version;

    private Builder() {}

    private Builder(UID uid, String userName, String firstName, String lastName, String email,
        long createdAt, long modifiedAt, String version) {
      super();
      this.uid = uid;
      this.userName = userName;
      this.firstName = firstName;
      this.lastName = lastName;
      this.email = email;
      this.createdAt = createdAt;
      this.modifiedAt = modifiedAt;
      this.version = version;
    }

    public UID getUID() {
      return uid;
    }

    public Builder setUID(UID uid) {
      this.uid = uid;
      return this;
    }

    public String getUserName() {
      return userName;
    }

    public Builder setUserName(String userName) {
      this.userName = userName;
      return this;
    }

    public String getFirstName() {
      return firstName;
    }

    public Builder setFirstName(String firstName) {
      this.firstName = firstName;
      return this;
    }

    public String getLastName() {
      return lastName;
    }

    public Builder setLastName(String lastName) {
      this.lastName = lastName;
      return this;
    }

    public String getEmail() {
      return email;
    }

    public Builder setEmail(String email) {
      this.email = email;
      return this;
    }

    public long getCreatedAt() {
      return createdAt;
    }

    public Builder setCreatedAt(long createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    public long getModifiedAt() {
      return modifiedAt;
    }

    public Builder setModifiedAt(long modifiedAt) {
      this.modifiedAt = modifiedAt;
      return this;
    }

    public String getVersion() {
      return version;
    }

    public Builder setVersion(String version) {
      this.version = version;
      return this;
    }

    public SimpleUser build() {
      return new SimpleUser(uid, userName, firstName, lastName, email, createdAt, modifiedAt, version);
    }
  }

  private final UID uid;
  private final String userName;
  private final String firstName;
  private final String lastName;
  private final String email;
  private final long createdAt;
  private final long modifiedAt;
  private final String version;

  @JsonCreator
  private SimpleUser(
      @JsonProperty("uid") UID uid,
      @JsonProperty("userName") String userName,
      @JsonProperty("firstName") String firstName,
      @JsonProperty("lastName") String lastName,
      @JsonProperty("email") String email,
      @JsonProperty("createdAt") long createdAt,
      @JsonProperty("modifiedAt") long modifiedAt,
      @JsonProperty("version") String version) {
    super();
    this.uid = uid;
    this.userName = userName;
    this.firstName = firstName;
    this.lastName = lastName;
    this.email = email;
    this.createdAt = createdAt;
    this.modifiedAt = modifiedAt;
    this.version = version;
  }

  @Override
  public UID getUID() {
    return uid;
  }

  @Override
  public String getUserName() {
    return userName;
  }
  @Override
  public String getFirstName() {
    return firstName;
  }

  @Override
  public String getLastName() {
    return lastName;
  }

  @Override
  public String getEmail() {
    return email;
  }

  @Override
  public long getCreatedAt() {
    return createdAt;
  }

  @Override
  public long getModifiedAt() {
    return modifiedAt;
  }

  @Override
  public String getVersion() {
    return version;
  }

  @Override
  public int hashCode() {
    return Objects.hash(uid, userName, firstName, lastName, email, createdAt, modifiedAt, version);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (!(obj instanceof SimpleUser)) {
      return false;
    }

    SimpleUser other = (SimpleUser) obj;
    return Objects.equals(uid, other.uid)
        && Objects.equals(userName, other.userName)
        && Objects.equals(firstName, other.firstName)
        && Objects.equals(lastName, other.lastName)
        && Objects.equals(email, other.email)
        && Objects.equals(createdAt, other.createdAt)
        && Objects.equals(modifiedAt, other.modifiedAt)
        && Objects.equals(version, other.version);
  }

  @Override
  public String toString() {
    return "SimpleUser [uid=" + uid + ", userName=" + userName + ", firstName=" + firstName + ", lastName=" + lastName
        + ", email=" + email + ", createdAt=" + createdAt + ", modifiedAt=" + modifiedAt
        + ", version=" + version + "]";
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(User user) {
    return new Builder(user.getUID(), user.getUserName(), user.getFirstName(), user.getLastName(),
        user.getEmail(), user.getCreatedAt(), user.getModifiedAt(), user.getVersion());
  }
}
