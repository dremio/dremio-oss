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
package com.dremio.dac.model.sources;

import static java.util.Arrays.asList;

import java.util.List;

import com.dremio.dac.model.common.ResourcePath;

/**
 * "/source/{source}"
 */
public class SourceResourcePath extends ResourcePath {

  private final SourceName sourceName;

  public SourceResourcePath(String sourcePath) {
    List<String> path = parse(sourcePath, "source");
    if (path.size() != 1) {
      throw new IllegalArgumentException("path should be of form: /source/{sourceName}, found " + sourcePath);
    }
    this.sourceName = new SourceName(path.get(0));
  }

  public SourceResourcePath(SourceName sourceName) {
    this.sourceName = sourceName;
  }

  @Override
  public List<String> asPath() {
    return asList("source", sourceName.getName());
  }

  public SourceName getSourceName() {
    return sourceName;
  }
}
