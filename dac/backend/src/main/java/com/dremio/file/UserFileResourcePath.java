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
package com.dremio.file;

import static java.util.Arrays.asList;

import java.util.List;

import com.dremio.dac.model.common.ResourcePath;
import com.dremio.dac.model.common.RootEntity;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * UserFileResourcePath represents the user's file resource path, which can be defined from a JSON or FilePath object. Such as example: (String),
 * such as "/file/{@home}.{folder.▪*name}".
 */
public class UserFileResourcePath extends ResourcePath {

  private final FilePath filePath;

  /**
   * Constructs a UserFileResourcePath object.
   *
   * @param path the user file path resource
   */
  public UserFileResourcePath(FilePath path) {
    this.filePath = path;
  }

  /**
   * Constructs a UserFileResourcePath object.
   *
   * @param filePath the user file path resource in JSON format
   * @throws IllegalArgumentException If the JSON format is incorrect or the file path is invalid for home
   */
  @JsonCreator
  public UserFileResourcePath(String filePath) {
    List<String> path = parse(filePath, "file");
    if (path.size() != 1) {
      throw new IllegalArgumentException("path should be of form: /file/{filePath}, found " + filePath);
    }
    this.filePath = new FilePath(path.get(0));
    if (this.filePath.getRootType() != RootEntity.RootType.HOME) {
      throw new IllegalArgumentException("Invalid file path for home, " + filePath);
    }
  }

  /**
   * Gets the path of a file.
   * <p>
   * The file path returns a list of string, that the first element is the "file" and second element is the file path.
   *
   * @return the path of a file
   */
  @Override
  public List<String> asPath() {
    return asList("file", filePath.toPathString());
  }

  /**
   * Gets the full path to a file.
   *
   * @return the full path to a file
   */
  public FilePath getFile() {
    return filePath;
  }
}
