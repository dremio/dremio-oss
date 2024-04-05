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
package com.dremio.dac.service.errors;

import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.folder.SourceFolderResourcePath;
import com.dremio.dac.model.sources.SourceName;

/** Thrown when a file is not found in source. */
public class SourceFolderNotFoundException extends NotFoundException {
  private static final long serialVersionUID = 1L;

  private final SourceFolderPath path;
  private final SourceName sourceName;

  public SourceFolderNotFoundException(
      SourceName soureName, SourceFolderPath path, Exception error) {
    super(new SourceFolderResourcePath(soureName, path), "folder " + path.toPathString(), error);
    this.path = path;
    this.sourceName = soureName;
  }

  public SourceName getSourceName() {
    return sourceName;
  }

  public SourceFolderPath getPath() {
    return path;
  }
}
