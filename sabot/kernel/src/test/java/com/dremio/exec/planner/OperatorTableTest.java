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
package com.dremio.exec.planner;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.dremio.BaseTestQuery;
import java.math.BigDecimal;
import org.junit.Ignore;
import org.junit.Test;

public class OperatorTableTest extends BaseTestQuery {

  @Test
  public void testRound() throws Exception {
    testBuilder()
        .sqlQuery("SELECT ROUND(CAST(99.99 AS NUMERIC(4,2))) AS my_field")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues(new BigDecimal("100.0"))
        .go();
  }

  @Test
  public void testRoundUsingJdbcEscaping() throws Exception {
    testBuilder()
        .sqlQuery("SELECT {fn ROUND(CAST (99.99 AS NUMERIC(4,2)), 0)} AS my_field")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues(new BigDecimal("100.0"))
        .go();
  }

  @Test
  public void testTruncate() throws Exception {
    testBuilder()
        .sqlQuery("SELECT TRUNCATE(CAST(99.99 AS NUMERIC(4,2)), 0) AS my_field")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues(new BigDecimal("99"))
        .go();
    testBuilder()
        .sqlQuery("SELECT {fn TRUNCATE(CAST (99.99 AS NUMERIC(4,2)), 0)} AS my_field")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues(new BigDecimal("99"))
        .go();
  }

  @Test
  public void testGreatestOperator() throws Exception {
    testBuilder()
        .sqlQuery(
            "SELECT GREATEST(CAST(1.2 AS float), CAST(123.12 AS decimal(5,2))) AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues(123.12D)
        .go();
    testBuilder()
        .sqlQuery(
            "SELECT GREATEST(CAST(123.5 AS float), CAST(123.12 AS decimal(5,2))) AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues(123.5D)
        .go();
  }

  @Ignore // we are exclude oracles decode, because it conflicts with the existing hive decode
  @Test
  public void testDecode() throws Exception {
    testBuilder()
        .sqlQuery(
            ""
                + "SELECT DECODE(my_value, 1, 'hello', 2, 'good bye') AS \"my_field\"\n"
                + "FROM (VALUES (1), (2)) as my_table(my_value) ")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("hello")
        .baselineValues("good bye")
        .go();
  }

  @Test
  public void testConvertTo() throws Exception {
    testBuilder()
        .sqlQuery("SELECT CONVERT_TO('my_value', 'UTF8') AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("my_value".getBytes(UTF_8))
        .go();
  }

  @Test
  public void testConvertFROM() throws Exception {
    testBuilder()
        .sqlQuery(
            "SELECT CONVERT_FROM(BINARY_STRING('\\x68\\x65\\x6c\\x6c\\x6f'), 'UTF8') AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("hello")
        .go();
  }

  @Test
  public void testTranslate() throws Exception {
    testBuilder()
        .sqlQuery("SELECT TRANSLATE('good bye', 'by', 'pi') AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("good pie")
        .go();
  }

  @Test
  public void testTranslateWithCast() throws Exception {
    testBuilder()
        .sqlQuery("SELECT TRANSLATE('good 12e', CAST('12' AS VARCHAR), 'pi') AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("good pie")
        .go();
  }

  @Test
  public void testLeastOperator() throws Exception {
    testBuilder()
        .sqlQuery("SELECT LEAST(CAST(1.5 AS float), CAST(123.12 AS decimal(5,2))) AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues(1.5D)
        .go();
    testBuilder()
        .sqlQuery(
            "SELECT LEAST(CAST(123.5 AS float), CAST(123.12 AS decimal(5,2))) AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues(123.12D)
        .go();
  }

  @Test
  public void testTrimOperator() throws Exception {
    testBuilder()
        .sqlQuery("SELECT LTRIM(' hello ') AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("hello ")
        .go();
    testBuilder()
        .sqlQuery("SELECT LTRIM('helloworld','hello') AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("world")
        .go();
    testBuilder()
        .sqlQuery("SELECT RTRIM(' hello ') AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues(" hello")
        .go();
    testBuilder()
        .sqlQuery("SELECT RTRIM('helloworld','world') AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("he")
        .go();
  }

  @Test
  public void testSubStr() throws Exception {
    testBuilder()
        .sqlQuery("SELECT substr(' hello ', 2, 5) AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("hello")
        .go();
    testBuilder()
        .sqlQuery("SELECT substr('ABC', -1) AS \"my_field\"")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("C");
  }

  @Test
  public void testNvlOperator() throws Exception {
    testBuilder()
        .sqlQuery(
            ""
                + "SELECT NVL(hello, good_bye) AS \"my_field\"\n"
                + "FROM (VALUES ('hello', 'good bye')) AS my_table(hello, good_bye)")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("hello")
        .go();
    testBuilder()
        .sqlQuery(
            ""
                + "SELECT NVL(hello, good_bye) AS \"my_field\"\n"
                + "FROM (VALUES ('hello', CAST(null AS VARCHAR))) AS my_table(hello, good_bye)")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("hello")
        .go();
    testBuilder()
        .sqlQuery(
            ""
                + "SELECT NVL(hello, good_bye) AS \"my_field\"\n"
                + "FROM (VALUES (CAST(null AS VARCHAR), 'good bye')) AS my_table(hello, good_bye)")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues("good bye")
        .go();
    testBuilder()
        .sqlQuery(
            ""
                + "SELECT NVL(hello, good_bye) AS \"my_field\"\n"
                + "FROM (VALUES (CAST(null AS VARCHAR), CAST(null AS VARCHAR))) AS my_table(hello, good_bye)")
        .unOrdered()
        .baselineColumns("my_field")
        .baselineValues((Object) null)
        .go();
  }
}
