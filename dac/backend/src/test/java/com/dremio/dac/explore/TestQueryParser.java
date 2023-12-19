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
package com.dremio.dac.explore;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.QueryMetadata;

/**
 * verify we understand queries
 */
public class TestQueryParser extends BaseTestServer {

  private static String table = "\"cp\".\"tpch/supplier.parquet\"";

  public void validateAncestors(String sql, String... ancestors) {
    QueryMetadata metadata = QueryParser.extract(new SqlQuery(sql, null, DEFAULT_USERNAME), l(SabotContext.class));
    List<SqlIdentifier> actualAncestors = metadata.getAncestors().get();
    String message = "expected: " + Arrays.toString(ancestors) + " actual: " + actualAncestors;
    Assert.assertEquals(message, ancestors.length, actualAncestors.size());
    for (int i = 0; i < ancestors.length; i++) {
      String expectedAncestor = ancestors[i];
      String actualAncestor = actualAncestors.get(i).toString();
      Assert.assertEquals(message, expectedAncestor, actualAncestor);
    }
  }

  @Test
  public void testStar() {
    validateAncestors("select * from " + table, "cp.tpch/supplier.parquet");
  }

  @Test
  public void testFields() {
    validateAncestors("select s_suppkey, s_name, s_address from " + table, "cp.tpch/supplier.parquet");
  }

  @Test
  public void testFieldsAs() {
    validateAncestors("select s_suppkey, s_name as mycol from " + table, "cp.tpch/supplier.parquet");
  }

  @Test
  public void testFlatten() {
    validateAncestors("select flatten(b), a as mycol from cp.\"json/nested.json\"", "cp.json/nested.json");
  }

  @Test
  public void testOrder() {
    validateAncestors("select a from cp.\"json/nested.json\" order by a", "cp.json/nested.json");
  }

  @Test(expected = UserException.class)
  public void testMultipleOrder() {
    validateAncestors("select a, b from cp.\"json/nested.json\" order by b desc, a asc", "cp.json/nested.json");
  }

  @Test
  public void testOrderAlias() {
    validateAncestors("select a as b from cp.\"json/nested.json\" order by b", "cp.json/nested.json");
  }

  @Test
  public void testJoin() {
    validateAncestors("select foo.a from cp.\"json/nested.json\" foo, \"cp\".\"tpch/supplier.parquet\" bar where foo.a = bar.s_suppkey",
        "cp.json/nested.json", "cp.tpch/supplier.parquet");
    validateAncestors("select foo.a from cp.\"json/nested.json\" foo, \"cp\".\"tpch/supplier.parquet\" bar, \"cp\".\"tpch/customer.parquet\" baz where foo.a = bar.s_suppkey and bar.s_suppkey = baz.c_custkey",
        "cp.json/nested.json", "cp.tpch/supplier.parquet", "cp.tpch/customer.parquet");
  }

  @Test
  public void testsubQuery() {
    validateAncestors("select foo.a from (select * from cp.\"json/nested.json\") foo, \"cp\".\"tpch/supplier.parquet\" bar where foo.a = bar.s_suppkey",
        "cp.json/nested.json", "cp.tpch/supplier.parquet");
  }

  @Test
  public void testUnicodeCharacters() {
    // Unicode Equality
    validateAncestors("select * from " + table + " where s_name = 'インターコンチネンタル'", "cp.tpch/supplier.parquet");

    // Unicode LIKE
    validateAncestors("select * from " + table + " where s_name LIKE 'インターコンチネンタル'", "cp.tpch/supplier.parquet");

    // Source: https://github.com/danielmiessler/SecLists/blob/master/Fuzzing/big-list-of-naughty-strings.txt
    String[] unicodeStrings = new String[] {
      // Strings which contain two-byte characters: can cause rendering issues or character-length issues
      "찦차를 타고 온 펲시맨과 쑛다리 똠방각하",

      // Strings which consists of Japanese-style emoticons which are popular on the web
      "(╯°□°）╯︵ ┻━┻)",

      // Emoji
      "\uD83D\uDCA9 ❤️ \uD83D\uDC94 \uD83D\uDC8C \uD83D\uDC95 \uD83D\uDC9E \uD83D\uDC93 \uD83D\uDC97 \uD83D\uDC96 \uD83D\uDC98 \uD83D\uDC9D \uD83D\uDC9F \uD83D\uDC9C \uD83D\uDC9B \uD83D\uDC9A \uD83D\uDC99",

      // Strings which contain "corrupted" text. The corruption will not appear in non-HTML text, however. (via http://www.eeemo.net)
      "Ṱ̺̺̕o͞ ̷i̲̬͇̪͙n̝̗͕v̟̜̘̦͟o̶̙̰̠kè͚̮̺̪̹̱̤ ̖t̝͕̳̣̻̪͞h̼͓̲̦̳̘̲e͇̣̰̦̬͎ ̢̼̻̱̘h͚͎͙̜̣̲ͅi̦̲̣̰̤v̻͍e̺̭̳̪̰-m̢iͅn̖̺̞̲̯̰d̵̼̟͙̩̼̘̳ ̞̥̱̳̭r̛̗̘e͙p͠r̼̞̻̭̗e̺̠̣͟s̘͇̳͍̝͉e͉̥̯̞̲͚̬͜ǹ̬͎͎̟̖͇̤t͍̬̤͓̼̭͘ͅi̪̱n͠g̴͉ ͏͉ͅc̬̟h͡a̫̻̯͘o̫̟̖͍̙̝͉s̗̦̲.̨̹͈̣",

      // Strings which contain unicode with an "upsidedown" effect (via http://www.upsidedowntext.com)
      "˙ɐnbᴉlɐ ɐuƃɐɯ ǝɹolop ʇǝ ǝɹoqɐl ʇn ʇunpᴉpᴉɔuᴉ ɹodɯǝʇ poɯsnᴉǝ op pǝs ʇᴉlǝ ƃuᴉɔsᴉdᴉpɐ ɹnʇǝʇɔǝsuoɔ ʇǝɯɐ ʇᴉs ɹolop ɯnsdᴉ ɯǝɹo˥00˙Ɩ$-",

      // Strings which contain bold/italic/etc. versions of normal characters
      "\uD835\uDCE3\uD835\uDCF1\uD835\uDCEE \uD835\uDCFA\uD835\uDCFE\uD835\uDCF2\uD835\uDCEC\uD835\uDCF4 \uD835\uDCEB\uD835\uDCFB\uD835\uDCF8\uD835\uDD00\uD835\uDCF7 \uD835\uDCEF\uD835\uDCF8\uD835\uDD01 \uD835\uDCF3\uD835\uDCFE\uD835\uDCF6\uD835\uDCF9\uD835\uDCFC \uD835\uDCF8\uD835\uDCFF\uD835\uDCEE\uD835\uDCFB \uD835\uDCFD\uD835\uDCF1\uD835\uDCEE \uD835\uDCF5\uD835\uDCEA\uD835\uDD03\uD835\uDD02 \uD835\uDCED\uD835\uDCF8\uD835\uDCF0",
    };

    for (String unicodeString : unicodeStrings){
      validateAncestors("select * from " + table + " where s_name = '" + unicodeString + "'", "cp.tpch/supplier.parquet");
    }
  }
}
