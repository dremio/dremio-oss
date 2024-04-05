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
package com.dremio.exec.sql;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.FileUtils;
import com.dremio.test.UserExceptionAssert;
import org.junit.Test;

/*
 * queries .csv files/folders that do not contain the ."csv" extension in the filename, ensuring the expected error message.
 */
public class TestPromotionOfFilesWithoutExtension extends BaseTestQuery {

  /*
   * tests a .csv file without .csv extension.
   * Expected to fail and propagate an error message.
   */
  @Test
  public void testPromoteFileWithoutExtension() throws Exception {
    final String path = "/store/text/testWithoutExtension";
    String root = FileUtils.getResourceAsFile(path).toURI().toString();
    query(root, String.format("SELECT * FROM dfs.\"%s\"", root));
  }

  /*
   * tests a .csv folder consisting of files without .csv extension.
   * Expected to fail and propagate an error message.
   */
  @Test
  public void testPromoteFolderOfFilesWithoutExtension() throws Exception {
    final String path = "/store/text/FolderWithoutExtension";
    String root = FileUtils.getResourceAsFile(path).toURI().toString();
    query(root, String.format("SELECT * FROM dfs.\"%s\"", root));
  }

  /*
   * tests a .csv folder consisting of files without .csv extension by using the ALTER PDS command
   * Expected to fail and propagate error message.
   */
  @Test
  public void testAlterPdsFileWithoutExtension() throws Exception {
    final String path = "/store/text/FolderWithoutExtension";
    String root = FileUtils.getResourceAsFile(path).toURI().toString();
    query(root, String.format("ALTER PDS dfs.\"%s\" REFRESH METADATA AUTO PROMOTION", root));
  }

  private void query(String path, String statement) throws Exception {
    String mssg =
        "The file format for 'dfs.\"%s\"' could not be identified. In order for automatic format detection to succeed, "
            + "files must include a file extension. Alternatively, manual promotion can be used to explicitly specify the format.";
    String error = String.format(mssg, path);
    UserExceptionAssert.assertThatThrownBy(
            () -> {
              runSQL(statement);
            })
        .isInstanceOf(Exception.class)
        .hasMessageContaining(error);
  }
}
