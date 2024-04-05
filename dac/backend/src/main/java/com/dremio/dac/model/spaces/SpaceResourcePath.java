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

import com.dremio.dac.model.common.ResourcePath;
import java.util.List;

/** "/space/{space}" */
public class SpaceResourcePath extends ResourcePath {

  private final SpaceName spaceName;

  public SpaceResourcePath(String spacePath) {
    List<String> path = parse(spacePath, "space");
    if (path.size() != 1) {
      throw new IllegalArgumentException(
          "path should be of form: /space/{spaceName}, found " + spacePath);
    }
    this.spaceName = new SpaceName(path.get(0));
  }

  public SpaceResourcePath(SpaceName spaceName) {
    this.spaceName = spaceName;
  }

  @Override
  public List<String> asPath() {
    return asList("space", spaceName.getName());
  }

  public SpaceName getSpaceName() {
    return spaceName;
  }
}
