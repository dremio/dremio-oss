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
package com.dremio.dac.model.folder;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

import com.dremio.dac.model.common.ResourcePath;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * "/folder/{space}.[folder.]*name}"
 */
public class FolderResourcePath extends ResourcePath {

  private final FolderPath folderPath;

  public FolderResourcePath(FolderPath path) {
    this.folderPath = path;
  }

  @JsonCreator
  public FolderResourcePath(String folderPath) {
    List<String> path = parse(folderPath, "folder");
    if (path.size() != 1) {
      throw new IllegalArgumentException("path should be of form: /folder/{folderPath}, found " + folderPath);
    }
    this.folderPath = new FolderPath(path.get(0));
  }

  @Override
  public List<String> asPath() {
    List<String> pathList = folderPath.toPathList();
    List<String> result = new ArrayList<String>();
    result.addAll(asList("space", pathList.get(0), "folder"));
    result.addAll(pathList.subList(1, pathList.size()));
    return result;
  }

  public FolderPath getFolder() {
    return folderPath;
  }
}
