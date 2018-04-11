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
package com.dremio.exec.fn.hive;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.QueryTestUtil;
import com.dremio.TestBuilder;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.Types;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.compile.ClassTransformer;
import com.dremio.exec.hive.HiveTestBase;
import com.dremio.exec.server.options.OptionValue;
import com.google.common.collect.Lists;

public class TestInbuiltHiveUDFs extends HiveTestBase {

  @Test // DRILL-3273
  public void testConcatWS() throws Exception {
    testBuilder()
        .sqlQuery("SELECT concat_ws(string_field, string_part, '|') as rst from hive.readtest")
        .unOrdered()
        .baselineColumns("rst")
        .baselineValues("stringstringfield|")
        .baselineValues(new Object[] { null })
        .go();
  }

  @Test // DRILL-3273
  public void testEncode() throws Exception {
    testBuilder()
        .sqlQuery("SELECT encode(varchar_field, 'UTF-8') as rst from hive.readtest")
        .unOrdered()
        .baselineColumns("rst")
        .baselineValues("varcharfield".getBytes())
        .baselineValues(new Object[] { null })
        .go();
  }

  @Test
  public void testXpath_Double() throws Exception {
    final String query = "select xpath_double ('<a><b>20</b><c>40</c></a>', 'a/b * a/c') as col \n" +
        "from hive.kv \n" +
        "limit 0";

    final MajorType majorType = Types.optional(MinorType.FLOAT8);

    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test // DRILL-4459
  public void testGetJsonObject() throws Exception {
    setEnableReAttempts(true);
    try {
      testBuilder()
              .sqlQuery("select convert_from(json, 'json') as json from hive.simple_json " +
                      "where GET_JSON_OBJECT(simple_json.json, '$.employee_id') like 'Emp2'")
              .ordered()
              .baselineColumns("json")
              .baselineValues(TestBuilder.mapOf("employee_id", "Emp2", "full_name", "Kamesh",
                      "first_name", "Bh", "last_name", "Venkata", "position", "Store"))
              .go();
    } finally {
      setEnableReAttempts(false);
    }
  }

  @Test // DRILL-3272
  @Ignore("DX-5213")
  public void testIf() throws Exception {
    testBuilder()
        .sqlQuery("select `if`(1999 > 2000, 'latest', 'old') Period from hive.kv limit 1")
        .ordered()
        .baselineColumns("Period")
        .baselineValues("old")
        .go();
  }

  @Test // DRILL-4618
  public void testRand() throws Exception {
    String query = "select 2*rand()=2*rand() col1 from (values (1))";
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("col1")
            .baselineValues(false)
            .go();
  }

  @Test //DRILL-4868
  public void testEmbeddedHiveFunctionCall() throws Exception {
    final String[] queries = {
        "SELECT convert_from(unhex(key2), 'INT_BE') as intkey \n" +
            "FROM cp.`functions/conv/conv.json`",
    };

    for (String query: queries) {
      testBuilder()
          .sqlQuery(query)
          .ordered()
          .baselineColumns("intkey")
          .baselineValues(1244739896)
          .baselineValues(new Object[] { null })
          .baselineValues(1313814865)
          .baselineValues(1852782897)
          .build()
          .run();
    }
  }

}
