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
import java.util.Collections;
import java.util.List;

import com.dremio.io.file.Path;
import com.dremio.service.namespace.file.proto.FileType;

/**
 * Expects path of _delta_log as metadataDir. Depending on the version and type
 * will return path either commit json or checkpoint parquet pattern (to address multi-part checkpoint).
 *
 * Filename generated based on the spec
 * See <a href=https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-log-entries">Delta.io spec</a>
 */
public final class DeltaFilePathResolver {

  private DeltaFilePathResolver() {
  }

  public static List<Path> resolve(Path metadataDir, long version, int subparts, FileType type) {
    if (subparts > 1) {
      List<Path> resolvedPathsList = new ArrayList<>(subparts);
      for (int i = 1; i <= subparts; ++i) {
        String versionWithZeroPadding = String.format("%20s.checkpoint.%10s.%10s.parquet", version, i, subparts).replace(' ', '0');
        resolvedPathsList.add(metadataDir.resolve(Path.of(versionWithZeroPadding)));
      }
      return resolvedPathsList;
    }

    String versionWithZeroPadding = String.format("%20s", version).replace(' ', '0');

    versionWithZeroPadding = versionWithZeroPadding + getExtension(type);

    return Collections.singletonList(metadataDir.resolve(Path.of(versionWithZeroPadding)));
  }

  public static Path resolveCheckpointPattern(Path metadataDir, long version) {
    String versionWithZeroPadding = String.format("%20s.checkpoint*.parquet", version).replace(' ', '0');
    return metadataDir.resolve(Path.of(versionWithZeroPadding));
  }

  private static String getExtension(FileType type) {
    switch (type) {
      case JSON:
        return ".json";

      case PARQUET:
        return ".checkpoint.parquet";

      default:
        throw new IllegalArgumentException("File type is not supported " + type);
    }
  }

  public static int getPartCountFromPath(Path path, FileType type) {
    switch (type) {
      case JSON:
        // json commit files are one part
        return 1;

      case PARQUET:
        String[] nameSegments = path.getName().split("\\.");
        if (nameSegments.length == 5) {
          // multipart checkpoint
          try {
            return Integer.parseInt(nameSegments[3]);
          } catch (NumberFormatException e) {
            return 0;
          }
        }
        return 1;

      default:
        throw new IllegalArgumentException("File type is not supported " + type);
    }
  }

  public static long getVersionFromPath(Path path) {
    String[] nameSegments = path.getName().split("\\.");
    return Long.parseLong(nameSegments[0]);
  }
}
