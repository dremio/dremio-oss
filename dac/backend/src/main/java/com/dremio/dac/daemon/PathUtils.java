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
package com.dremio.dac.daemon;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;

/**
 * Utilities for working with paths.
 */
public final class PathUtils {

  private PathUtils(){}

  /**
   * Check if the provided path is a directory and is readable/writable/executable
   *
   * If the path doesn't exist, attempt to create a directory
   *
   * @param path the path to check
   * @throws IOException if the directory cannot be created or is not accessible
   */
  public static void checkWritePath(String path) throws IOException {
    java.nio.file.Path npath = new File(path).toPath();

    // Attempt to create the directory if it doesn't exist
    if (Files.notExists(npath, LinkOption.NOFOLLOW_LINKS)) {
      Files.createDirectories(npath);
      return;
    }

    if (!Files.isDirectory(npath)) {
      throw new IOException(format("path %s is not a directory.", npath));
    }

    if (!Files.isReadable(npath)) {
      throw new IOException(format("path %s is not readable.", npath));
    }

    if (!Files.isWritable(npath)) {
      throw new IOException(format("path %s is not writable.", npath));
    }

    if (!Files.isExecutable(npath)) {
      throw new IOException(format("path %s is not executable.", npath));
    }
  }
}
