/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import com.dremio.dac.model.common.AddressableResource;
import com.dremio.service.users.User;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Model for user information.
 */
@JsonIgnoreProperties(value={"links", "id"}, allowGetters=true)
public class UserUI implements AddressableResource, Principal {
  private final UserName userName;

  private final User user;

  private UserResourcePath resourcePath;

  @JsonCreator
  public UserUI(@JsonProperty("resourcePath") UserResourcePath resourcePath,
                @JsonProperty("userName") UserName userName,
                @JsonProperty("userConfig") User userGroup) {
    this.resourcePath = resourcePath;
    this.userName = userName;
    this.user = userGroup;
  }

  @Override
  public String getName() {
    return user.getUserName();
  }

  public String getId() {
    return user.getUID().getId();
  }

  public void setName(String name) {
    // noop
  }

  @Override
  public UserResourcePath getResourcePath() {
    return resourcePath;
  }

  public UserName getUserName() {
    return userName;
  }

  @JsonProperty("userConfig")
  public User getUser() {
    return user;
  }

  public Map<String, String> getLinks() throws UnsupportedEncodingException {
    Map<String, String> links = new HashMap<>();
    links.put("self", resourcePath.toString());

    return links;
  }
}
