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

package com.dremio.exec;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ITHivePartitionPruningV2 extends ITHivePartitionPruning {

  private static AutoCloseable icebergEnabled;

  @BeforeClass
  public static void initialiseDetails() {
    queryPlanKeyword = "IcebergManifestList(table=[";
    usesV2Flow = true;
    icebergEnabled = enableUnlimitedSplitsSupportFlags();
  }

  @AfterClass
  public static void  resetFlags() throws Exception {
    icebergEnabled.close();
  }

}
