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

package com.dremio.exec.store.deltalake;

import java.util.ArrayList;
import java.util.List;

import com.dremio.io.file.Path;
import com.dremio.service.namespace.file.proto.FileType;

/**
 * Expects path of _delta_log as metaDir. Depending on the version and type
 * will return path either commit json or checkpoint parquet.
 *
 * Filename generated based on the spec
 * See <a href=https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-log-entries">Delta.io spec</a>
 */
public class DeltaFilePathResolver {

  public List<Path> resolve(Path metaDir, Long version, Long subparts, FileType type) {
    List<Path> resolvedPathsList = new ArrayList<>(subparts.intValue());
    if (subparts > 1) {
      for(Long i = 1L; i <= subparts; i++) {
        String versionWithZeroPadding = String.format("%20s.checkpoint.%10s.%10s.parquet", version.toString(), i.toString(), subparts.toString()).replace(' ', '0');

        resolvedPathsList.add(metaDir.resolve(Path.of(versionWithZeroPadding)));
      }
    }else {
      String versionWithZeroPadding = String.format("%20s", version.toString()).replace(' ', '0');

      versionWithZeroPadding = versionWithZeroPadding + getExtension(type);

      resolvedPathsList.add(metaDir.resolve(Path.of(versionWithZeroPadding)));
    }
    return resolvedPathsList;
  }

  private String getExtension(FileType type) {
    switch (type) {
      case JSON:
        return ".json";
      case PARQUET:
        return ".checkpoint.parquet";
      default:
        throw new IllegalArgumentException("File type is not supported " + type);
    }
  }

}
