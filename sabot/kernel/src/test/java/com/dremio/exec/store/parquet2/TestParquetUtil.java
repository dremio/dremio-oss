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
package com.dremio.exec.store.parquet2;

import com.dremio.BaseTestQuery;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;

public class TestParquetUtil {

  public static String setupParquetFiles(
      String testName, String folderName, String primaryParquet, String workingPath)
      throws Exception {
    /*
     * Copy primary parquet in a temporary folder and promote the same. This way, primary parquet's schema will be
     * taken as the dremio dataset's schema. Then copy remaining files and refresh the dataset.
     */
    final String parquetRefFolder = workingPath + "/src/test/resources/parquet/" + folderName;
    String parquetFiles = Files.createTempDirectory(testName).toString();
    try {
      Files.copy(
          Paths.get(parquetRefFolder, primaryParquet),
          Paths.get(parquetFiles, primaryParquet),
          StandardCopyOption.REPLACE_EXISTING);
      BaseTestQuery.runSQL(
          "SELECT * FROM dfs.\"" + parquetFiles + "\""); // to detect schema and auto promote

      // Copy remaining files
      try (Stream<java.nio.file.Path> stream = Files.walk(Paths.get(parquetRefFolder))) {
        stream
            .filter(Files::isRegularFile)
            .filter(p -> !p.getFileName().toString().equals(primaryParquet))
            .forEach(
                p -> {
                  try {
                    Files.copy(
                        p,
                        Paths.get(parquetFiles, p.getFileName().toString()),
                        StandardCopyOption.REPLACE_EXISTING);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
      }
      BaseTestQuery.runSQL(
          "alter table dfs.\""
              + parquetFiles
              + "\" refresh metadata force update"); // so it detects second parquet
      BaseTestQuery.setEnableReAttempts(true);
      BaseTestQuery.runSQL(
          "select * from dfs.\""
              + parquetFiles
              + "\""); // need to run select * from pds to get correct schema update. Check DX-25496
      // for details.
      return parquetFiles;
    } catch (Exception e) {
      delete(Paths.get(parquetFiles));
      throw e;
    } finally {
      BaseTestQuery.setEnableReAttempts(false);
    }
  }

  public static void delete(java.nio.file.Path dir) throws Exception {
    FileUtils.deleteDirectory(dir.toFile());
  }
}
