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
package com.dremio.exec.store.hive;

import com.dremio.exec.hive.HiveTestBase;
import org.junit.Test;

public class ITHive3PluginNameWithWhitespace extends HiveTestBase {

  @Test
  public void testQuery() throws Exception {
    test(
        "select * from \"" + HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME_WITH_WHITESPACE + "\".kv");
  }
}
