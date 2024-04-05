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
package com.dremio.common.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.Resources;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

public class FileUtils {
  public static final char separatorChar = '/';

  public static final String separator = "" + separatorChar;

  private static URL getResource(String fileName) throws IOException {
    try {
      return Resources.getResource(FileUtils.class, fileName);
    } catch (IllegalArgumentException e) {
      throw new FileNotFoundException(String.format("Unable to find file on path %s", fileName));
    }
  }

  public static File getResourceAsFile(String fileName) throws IOException {
    return new File(getResource(fileName).getPath());
  }

  public static String getResourceAsString(String fileName) throws IOException {
    return Resources.toString(getResource(fileName), UTF_8);
  }
}
