/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import com.dremio.dac.model.common.RootEntity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Preconditions;

/**
 * Temp space entity
 */
public class TempSpace extends RootEntity {

  private static final String TEMP_SPACE = "tmp";

  public static TempSpace impl() {
    return new TempSpace(TEMP_SPACE);
  }

  @JsonCreator
  public TempSpace(String name) {
    super(name);

    Preconditions.checkArgument(TEMP_SPACE.equals(name), "tmp space can't be called %s", name);
  }

  public static boolean isTempSpace(final String spaceName) {
    if (TEMP_SPACE.equals(spaceName)) {
      return true;
    }
    return false;
  }

  @Override
  public String getRootUrl() {
    return TEMP_SPACE;
  }

  @Override
  public RootType getRootType() {
    return RootType.TEMP;
  }
}
