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
package com.dremio.exec.planner.sql.parser;

import org.apache.calcite.sql.SqlIdentifier;

import com.dremio.exec.catalog.VersionContext;

public class ReferenceTypeUtils {
  private ReferenceTypeUtils() {
  }

  public static VersionContext map(ReferenceType refType, SqlIdentifier refValue) {
    if (refType == null || refValue == null) {
      return VersionContext.NOT_SPECIFIED;
    }
    String refValueString = refValue.toString();
    switch (refType) {
      case REFERENCE:
        return VersionContext.ofRef(refValueString);
      case BRANCH:
        return VersionContext.ofBranch(refValueString);
      case TAG:
        return VersionContext.ofTag(refValueString);
      case COMMIT:
        return VersionContext.ofBareCommit(refValueString);
      default:
        throw new IllegalStateException("Unexpected value: " + refType);
    }
  }
}
