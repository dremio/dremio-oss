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
package com.dremio.exec.sql.hive;

import static com.dremio.exec.store.hive.HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.sql.DeleteTests;
import com.dremio.exec.planner.sql.DmlQueryTestUtils.DmlRowwiseOperationWriteMode;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ITDeleteMorOnHive extends DmlQueryOnHiveTestBase {
  // Defining SOURCE such that you can easily copy and paste the same test across other test variations

  @BeforeClass
  public static void setUp() throws Exception {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_POSITIONAL_DELETE_WRITER, "true");
  }

  @AfterClass
  public static void close() throws Exception {
    resetSystemOption(ExecConstants.ENABLE_ICEBERG_POSITIONAL_DELETE_WRITER.getOptionName());
  }

  private static final String SOURCE = HIVE_TEST_PLUGIN_NAME;

  DmlRowwiseOperationWriteMode dmlWriteMode = DmlRowwiseOperationWriteMode.MERGE_ON_READ;

  @Test
  public void testDeleteAllFail() throws Exception {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_POSITIONAL_DELETE_WRITER, "false");
    Assertions.assertThatThrownBy(() ->
            DeleteTests.testDeleteAll(allocator, SOURCE, dmlWriteMode)
        )
        .isInstanceOf(Exception.class)  // or a more specific exception class if you know what should be thrown
        .hasMessageContaining("The target iceberg table's write.delete.mode table-property is "
            + "set to 'merge-on-read', but dremio does not support this "
            + "write property at this time. Please alter your write.delete.mode table property "
            + "to 'copy-on-write' to proceed.");
    resetSystemOption(ExecConstants.ENABLE_ICEBERG_POSITIONAL_DELETE_WRITER.getOptionName());
  }
}
