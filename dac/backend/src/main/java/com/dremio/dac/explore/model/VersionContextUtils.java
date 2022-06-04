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
package com.dremio.dac.explore.model;

import com.dremio.exec.catalog.VersionContext;

public class VersionContextUtils {
  private VersionContextUtils() {
  }

  public static VersionContext map(VersionContextReq from) {
    if (from == null) {
      return VersionContext.NOT_SPECIFIED;
    }
    switch(from.getType()) {
    case BRANCH:
      return VersionContext.ofBranch(from.getValue());
    case TAG:
      return VersionContext.ofTag(from.getValue());
    case COMMIT:
      return VersionContext.ofBareCommit(from.getValue());
    default:
      throw new IllegalStateException("Unexpected value: " + from.getType());
    }
  }
}
