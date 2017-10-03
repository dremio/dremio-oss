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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.Utils;

public final class DefaultPathFilter extends Utils.OutputFileUtils.OutputFilesFilter {

  public static PathFilter INSTANCE = new DefaultPathFilter();

  private DefaultPathFilter() {}

  @Override
  public boolean accept(Path path) {
    if (path.getName().startsWith(FileSystemWrapper.HIDDEN_FILE_PREFIX)) {
      return false;
    }
    if (path.getName().startsWith(FileSystemWrapper.DOT_FILE_PREFIX)) {
      return false;
    }
    return super.accept(path);
  }
}
