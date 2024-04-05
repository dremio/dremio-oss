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
package com.dremio.dac.service.errors;

import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpaceResourcePath;

/** Thrown when a space is not found for a given path. */
public class SpaceNotFoundException extends NotFoundException {
  private static final long serialVersionUID = 1L;

  private final String spaceName;

  public SpaceNotFoundException(String spaceName, Exception error) {
    super(new SpaceResourcePath(new SpaceName(spaceName)), "space " + spaceName, error);
    this.spaceName = spaceName;
  }

  public String getSpaceName() {
    return spaceName;
  }
}
