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

package com.dremio.dac.model.spaces;

import static java.util.Arrays.asList;

import java.util.List;

import com.dremio.dac.model.common.ResourcePath;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * "/home/@user"
 */
public class HomeResourcePath extends ResourcePath {

  private final HomeName home;

  public HomeResourcePath(HomeName home) {
    this.home = home;
  }

  @JsonCreator
  public HomeResourcePath(String homePath) {
    List<String> path = parse(homePath, "home");
    if (path.size() != 1) {
      throw new IllegalArgumentException("path should be of form: /home/@{homeName}, found " + homePath);
    }
    this.home = new HomeName(path.get(0));
  }

  @Override
  public List<String> asPath() {
    return asList("home", home.getName());
  }

  public HomeName getHome() {
    return this.home;
  }
}
