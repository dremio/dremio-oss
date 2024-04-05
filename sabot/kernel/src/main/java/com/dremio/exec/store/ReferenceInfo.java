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

package com.dremio.exec.store;

import java.util.Objects;

/** Reference info used to support versioning. */
public final class ReferenceInfo {
  public final String type;
  public final String refName;
  public final String commitHash;

  public ReferenceInfo(String type, String refName, String commitHash) {
    this.type = type;
    this.refName = refName;
    this.commitHash = commitHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReferenceInfo that = (ReferenceInfo) o;
    return Objects.equals(type, that.type)
        && Objects.equals(refName, that.refName)
        && Objects.equals(commitHash, that.commitHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, refName, commitHash);
  }
}
