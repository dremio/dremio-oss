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

import java.time.Instant;
import java.util.Locale;

import javax.annotation.Nullable;

import org.apache.calcite.sql.SqlIdentifier;

import com.dremio.catalog.model.VersionContext;

public class ReferenceTypeUtils {
  private ReferenceTypeUtils() {
  }

  public static VersionContext map(ReferenceType refType, SqlIdentifier refValue, @Nullable Instant timestamp) {
    if (refType == null || refValue == null) {
      return VersionContext.NOT_SPECIFIED;
    }

    String refValueString = refValue.toString();

    if (timestamp != null) {
      switch (refType) {
        case REFERENCE:
          return VersionContext.ofRefAsOfTimestamp(refValueString, timestamp);
        case BRANCH:
          return VersionContext.ofBranchAsOfTimestamp(refValueString, timestamp);
        case TAG:
          return VersionContext.ofTagAsOfTimestamp(refValueString, timestamp);
        case COMMIT:
          throw new IllegalStateException("Reference type COMMIT does not support specifying a timestamp.");
        default:
          throw new IllegalStateException("Unexpected value: " + refType);
      }
    }

    switch (refType) {
      case REFERENCE:
        return VersionContext.ofRef(refValueString);
      case BRANCH:
        return VersionContext.ofBranch(refValueString);
      case TAG:
        return VersionContext.ofTag(refValueString);
      case COMMIT:
        return VersionContext.ofCommit(refValueString);
      default:
        throw new IllegalStateException("Unexpected value: " + refType);
    }
  }

  public static VersionContext map(String refType, String refValue) {
    if (refType == null || refValue == null) {
      return VersionContext.NOT_SPECIFIED;
    }
    switch (ReferenceType.valueOf(refType.toUpperCase(Locale.ROOT))) {
      case REFERENCE:
        return VersionContext.ofRef(refValue);
      case BRANCH:
        return VersionContext.ofBranch(refValue);
      case TAG:
        return VersionContext.ofTag(refValue);
      case COMMIT:
        return VersionContext.ofCommit(refValue);
      default:
        throw new IllegalStateException("Unexpected value: " + refType);
    }
  }
}
