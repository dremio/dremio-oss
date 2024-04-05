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
package com.dremio.service.reflection.materialization;

import com.dremio.io.AsyncByteReader;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FilterFileSystem;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/** A file system for accelerations */
class AccelerationFileSystem extends FilterFileSystem {
  public AccelerationFileSystem(FileSystem fs) {
    super(fs);
  }

  @Override
  public AsyncByteReader getAsyncByteReader(
      AsyncByteReader.FileKey fileKey, Map<String, String> options) throws IOException {
    List<String> datasetKey = fileKey.getDatasetKey();
    if (datasetKey.size() == 2) {
      return super.getAsyncByteReader(fileKey, options);
    }

    Preconditions.checkState(
        datasetKey.size() == 3,
        "dataset size is not 3 in acceleration filesystem: %s",
        datasetKey.size());
    // The dataset used by accelerations should not include the materialization id. Strip the
    // materialization id
    return super.getAsyncByteReader(
        AsyncByteReader.FileKey.of(
            fileKey.getPath(),
            fileKey.getVersion(),
            fileKey.getFileType(),
            datasetKey.subList(0, 2)),
        options);
  }
}
