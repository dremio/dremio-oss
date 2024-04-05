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
package com.dremio.exec.dotfile;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;

public class DotFile {

  private FileAttributes attributes;
  private DotFileType type;
  private FileSystem fs;

  public static DotFile create(FileSystem fs, FileAttributes attributes) {
    for (DotFileType d : DotFileType.values()) {
      if (!attributes.isDirectory() && d.matches(attributes)) {
        return new DotFile(fs, attributes, d);
      }
    }
    return null;
  }

  private DotFile(FileSystem fs, FileAttributes attributes, DotFileType type) {
    this.fs = fs;
    this.attributes = attributes;
    this.type = type;
  }

  public DotFileType getType() {
    return type;
  }

  /**
   * @return Return owner of the file in underlying file system.
   */
  public String getOwner() {
    return attributes.owner().getName();
  }

  /**
   * Return base file name without the parent directory and extensions.
   *
   * @return Base file name.
   */
  public String getBaseName() {
    final String fileName = attributes.getPath().getName();
    return fileName.substring(0, fileName.lastIndexOf(type.getEnding()));
  }

  public View getView(LogicalPlanPersistence lpPersistence) throws IOException {
    Preconditions.checkArgument(type == DotFileType.VIEW);
    try (InputStream is = fs.open(attributes.getPath())) {
      return lpPersistence.getMapper().readValue(is, View.class);
    }
  }
}
