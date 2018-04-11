/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.sequencefile;

import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.FileUtils;

import org.apache.hadoop.io.BytesWritable;

public class TestSequenceFileReader extends BaseTestQuery {

  public static String byteWritableString(String input) throws Exception {
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bout);
    final BytesWritable writable = new BytesWritable(input.getBytes("UTF-8"));
    writable.write(out);
    return new String(bout.toByteArray());
  }

  @Test
  @Ignore("sequence file not supported in Dremio currently")
  public void testSequenceFileReader() throws Exception {
    String root = FileUtils.getResourceAsFile("/sequencefiles/simple.seq").toURI().toString();
    final String query = String.format("select convert_from(t.binary_key, 'UTF8') as k, convert_from(t.binary_value, 'UTF8') as v " +
      "from dfs.`%s` t", root);
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("k", "v")
      .baselineValues(byteWritableString("key0"), byteWritableString("value0"))
      .baselineValues(byteWritableString("key1"), byteWritableString("value1"))
      .build().run();
  }
}
