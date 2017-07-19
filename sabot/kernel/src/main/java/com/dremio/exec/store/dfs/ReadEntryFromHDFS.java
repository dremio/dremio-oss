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
package com.dremio.exec.store.dfs;

import com.dremio.exec.store.dfs.easy.FileWork;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class ReadEntryFromHDFS extends ReadEntryWithPath implements FileWork{

  private long start;
  private long length;

  @JsonCreator
  public ReadEntryFromHDFS(@JsonProperty("path") String path,@JsonProperty("start") long start, @JsonProperty("length") long length) {
    this.path = path;
    this.start = start;
    this.length = length;
  }

  public long getStart() {
    return start;
  }

  public long getLength() {
    return length;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ReadEntryFromHDFS that = (ReadEntryFromHDFS) o;
    return start == that.start &&
        length == that.length;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), start, length);
  }
}
