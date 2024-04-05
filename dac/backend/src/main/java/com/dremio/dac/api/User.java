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
package com.dremio.dac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/** User */
public class User {
  private final String id;
  private final String name;
  private final String firstName;
  private final String lastName;
  private final String email;
  private final String tag;
  private final String extra;
  private final Boolean active;

  /**
   * A password. Used only when we going to update a user. So api consumer could send a password,
   * but could not read it (A internal getter)
   */
  private final String password;

  @JsonCreator
  public User(
      @JsonProperty("id") String id,
      @JsonProperty("name") String name,
      @JsonProperty("firstName") String firstName,
      @JsonProperty("lastName") String lastName,
      @JsonProperty("email") String email,
      @JsonProperty("tag") String tag,
      @JsonProperty("password") String password,
      @JsonProperty("extra") String extra,
      @JsonProperty("active") Boolean active) {
    this.id = id;
    this.name = name;
    this.firstName = firstName;
    this.lastName = lastName;
    this.email = email;
    this.tag = tag;
    this.password = password;
    this.extra = extra;
    this.active = active == null || active;
  }

  public User(
      String id,
      String name,
      String firstName,
      String lastName,
      String email,
      String tag,
      String password,
      String extra) {
    this(id, name, firstName, lastName, email, tag, password, extra, true);
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
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

  public String getTag() {
    return tag;
  }

  public String getExtra() {
    return extra;
  }

  @JsonIgnore
  public String getPassword() {
    return password;
  }

  public static User fromUser(com.dremio.service.users.User user) {
    return new User(
        user.getUID().getId(),
        user.getUserName(),
        user.getFirstName(),
        user.getLastName(),
        user.getEmail(),
        user.getVersion(),
        null,
        user.getExtra(),
        user.isActive()); // never send a password to a consumer
  }

  public Boolean isActive() {
    return active;
  }
}
