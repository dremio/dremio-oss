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
package com.dremio.exec.hadoop;

import static com.dremio.common.utils.PathUtils.removeLeadingSlash;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.Util;

import com.google.common.base.Joiner;

/**
 * Hadoop Utility Class
 */
public class DremioHadoopUtils {

  public static final String COS_SCHEME = "cosn";

  public static String getHadoopFSScheme(Path path, Configuration conf) {
    return Util.getFs(path, conf).getScheme();
  }

  public static Path toHadoopPath(com.dremio.io.file.Path path) {
    return new Path(path.toString());
  }

  public static Path toHadoopPath(String path) {
    return new Path(path);
  }

  /**
   * Get container name from the path.
   *
   * @param path path
   * @return container name
   */
  public static String getContainerName(Path path) {
    if (COS_SCHEME.equalsIgnoreCase(path.toUri().getScheme())) {
      return path.toString().split(Path.SEPARATOR)[2];
    }
    final List<String> pathComponents = Arrays.asList(
        removeLeadingSlash(Path.getPathWithoutSchemeAndAuthority(path).toString())
            .split(Path.SEPARATOR)
    );
    return pathComponents.get(0);
  }

  public static Path pathWithoutContainer(Path path) {
    List<String> pathComponents = Arrays.asList(removeLeadingSlash(Path.getPathWithoutSchemeAndAuthority(path).toString()).split(Path.SEPARATOR));
    if (COS_SCHEME.equalsIgnoreCase(path.toUri().getScheme())) {
      return new Path("/" + Joiner.on(Path.SEPARATOR).join(pathComponents));
    }
    return new Path("/" + Joiner.on(Path.SEPARATOR).join(pathComponents.subList(1, pathComponents.size())));
  }
}
