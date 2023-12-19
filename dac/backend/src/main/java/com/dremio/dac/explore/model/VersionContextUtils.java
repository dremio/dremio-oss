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

import com.dremio.catalog.model.VersionContext;

/**
 * Utility class for parsing version context.
 */
public final class VersionContextUtils {
  private VersionContextUtils() {
  }

  // TODO (DX-82787): Move this method to Version Context class.
  public static VersionContext parse(String refType, String refValue) {
    VersionContextReq from = VersionContextReq.tryParse(refType, refValue);
    if (from == null) {
      return VersionContext.NOT_SPECIFIED;
    }
    switch(from.getType()) {
    case BRANCH:
      return VersionContext.ofBranch(from.getValue());
    case TAG:
      return VersionContext.ofTag(from.getValue());
    case COMMIT:
      return VersionContext.ofCommit(from.getValue());
    default:
      throw new IllegalStateException("Unexpected value: " + from.getType());
    }
  }
}
