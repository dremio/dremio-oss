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
package com.dremio.file;

import static java.util.Arrays.asList;

import java.util.List;

import com.dremio.dac.model.common.ResourcePath;
import com.dremio.dac.model.sources.SourceName;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * "/source/{sourceName}/file/{source}.[folder.]*name}"
 */
public class SourceFileResourcePath extends ResourcePath {

  private final SourceFilePath filePath;
  private final SourceName sourceName;

  public SourceFileResourcePath(SourceName sourceName, SourceFilePath path) {
    this.sourceName = sourceName;
    this.filePath = path;
  }

  @JsonCreator
  public SourceFileResourcePath(String filePath) {
    List<String> path = parse(filePath, "source", "file");
    if (path.size() != 2) {
      throw new IllegalArgumentException("path should be of form: /source/{sourceName}/file/{filePath}, found " + filePath);
    }
    this.sourceName = new SourceName(path.get(0));
    this.filePath = new SourceFilePath(path.get(1));
  }

  @Override
  public List<String> asPath() {
    return asList("source", sourceName.getName(), "file", filePath.toPathString());
  }

  public SourceName getSourceName() {
    return sourceName;
  }

  public SourceFilePath getFile() {
    return filePath;
  }
}
