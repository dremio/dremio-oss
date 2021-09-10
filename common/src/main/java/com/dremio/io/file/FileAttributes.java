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
package com.dremio.io.file;

import java.io.IOException;
import java.nio.file.NotLinkException;
import java.nio.file.attribute.PosixFileAttributes;

import static com.dremio.io.file.PathFilters.NO_HIDDEN_FILES;

/**
 * File attributes associated with file
 */
public interface FileAttributes extends PosixFileAttributes {
  /**
   * Gets the path to the file
   * @return the path
   */
  Path getPath();

  /**
   * Gets the symbolic link associated with the file
   *
   * @return the target path of the link
   * @throws NotLinkException if the original file is not a symbolic link
   * @throws IOException
   */
  Path getSymbolicLink() throws NotLinkException, IOException;

  @Override
  int hashCode();

  @Override
  boolean equals(Object obj);

  default boolean isRegularAndNoHiddenFile() {
    return isRegularFile() && NO_HIDDEN_FILES.test(getPath());
  }
}
