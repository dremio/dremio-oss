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

import com.dremio.service.users.User;
import com.dremio.service.users.proto.UID;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A trimmed down User Object with basic/non-sensitive info. Used mainly in context of searching the
 * user from UI.
 */
public final class UserLite implements User {
  private final UID uid;
  private final String userName;
  private final String firstName;
  private final String lastName;
  private final boolean active;

  @JsonCreator
  private UserLite(
      @JsonProperty("uid") UID uid,
      @JsonProperty("userName") String userName,
      @JsonProperty("firstName") String firstName,
      @JsonProperty("lastName") String lastName,
      @JsonProperty("active") Boolean active) {
    this.uid = uid;
    this.userName = userName;
    this.firstName = firstName;
    this.lastName = lastName;
    this.active = active == null || active;
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
  @JsonIgnore
  public String getEmail() {
    return null;
  }

  @Override
  @JsonIgnore
  public long getCreatedAt() {
    return 0;
  }

  @Override
  @JsonIgnore
  public long getModifiedAt() {
    return 0;
  }

  @Override
  @JsonIgnore
  public String getVersion() {
    return null;
  }

  @Override
  public String getExtra() {
    return null;
  }

  @Override
  public boolean isActive() {
    return active;
  }

  /** A builder for {@code UserLite} */
  public static final class Builder {
    private UID uid;
    private String userName;
    private String firstName;
    private String lastName;

    private boolean active;

    private Builder() {}

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

    public boolean getActive() {
      return active;
    }

    public Builder setActive(boolean active) {
      this.active = active;
      return this;
    }

    public UserLite build() {
      return new UserLite(uid, userName, firstName, lastName, active);
    }
  }

  public static UserLite.Builder newBuilder() {
    return new UserLite.Builder();
  }
}
