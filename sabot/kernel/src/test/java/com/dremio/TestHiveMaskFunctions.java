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
package com.dremio;

import org.junit.Test;

public class TestHiveMaskFunctions extends PlanTestBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestExampleQueries.class);

  @Test
  public void testMaskHashChar() throws Exception {
    String query = "select mask_hash(c1) as cc from (values('TestString-123')) as t1(c1)";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("cc")
      .baselineValues("8b44d559dc5d60e4453c9b4edf2a455fbce054bb8504cd3eb9b5f391bd239c90")
      .build()
      .run();
  }

  @Test
  public void testMaskHashCharWithLen() throws Exception {
    String query = "select mask_hash(cast(c1 as char(24))) as cc from (values('TestString-123')) as t1(c1)";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("cc")
      .baselineValues("8b44d559dc5d60e4453c9b4edf2a455fbce054bb8504cd3eb9b5f391bd239c90")
      .build()
      .run();
  }

  @Test
  public void testMaskHashVarChar() throws Exception {
    String query = "select mask_hash(cast(c1 as varchar(24))) as cc from (values('TestString-123')) as t1(c1)";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("cc")
      .baselineValues("8b44d559dc5d60e4453c9b4edf2a455fbce054bb8504cd3eb9b5f391bd239c90")
      .build()
      .run();
  }

  @Test
  public void testMaskHashVarCharFromDataset() throws Exception {
    String query = "select mask_hash(v) as cc from cp.\"parquet/test_col_masking.parquet\"";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("cc")
      .baselineValues("6d3a4335fbfb50467dc0ee18646d7472e0745af9a8b8a703fdbf20903dfff814")
      .build()
      .run();
  }

  @Test
  public void testMaskHashInt() throws Exception {
    String query = "select mask_hash(cast(c1 as int)) as cc from (values(123)) as t1(c1)";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("cc")
      .baselineValues(null)
      .build()
      .run();
  }

  @Test
  public void testMaskHashBigInt() throws Exception {
    String query = "select mask_hash(cast(c1 as bigint)) as cc from (values(123)) as t1(c1)";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("cc")
      .baselineValues(null)
      .build()
      .run();
  }

  @Test
  public void testMaskHashDate() throws Exception {
    String query = "select mask_hash(cast(c1 as date)) as cc from (values('2016-04-20')) as t1(c1)";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("cc")
      .baselineValues(null)
      .build()
      .run();
  }

  @Test
  public void testMaskChar() throws Exception {
    String query = "select mask('TestString-12345') as c1";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("XxxxXxxxxx-nnnnn")
      .build()
      .run();
  }

  @Test
  public void testMaskCharFromDataset() throws Exception {
    String query = "select mask(v) as c1 from cp.\"parquet/test_col_masking.parquet\"";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("XxxxXxxxxx-nnnnn")
      .build()
      .run();
  }

  @Test
  public void testMaskCharTooManyArgs() throws Exception {
    String query = "select mask('TestString-12345', 'Y', 'y', '0', ':', 6, 7, 8, 9, 10) as c1";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("YyyyYyyyyy:00000")
      .build()
      .run();
  }

  @Test
  public void testMaskCharWithLen() throws Exception {
    String query = "select \n" +
      " mask(cast('TestString-12345' as char(24)), 'Y', 'y', '0', ':') as c1";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      //.baselineValues("YyyyYyyyyy:00000:::::::") // we don't really have a char type in Dremio
      .baselineValues("YyyyYyyyyy:00000")
      .build()
      .run();
  }

  /** bug most likely unrelated to the mask function - select from dataset is ok
  @Test
  public void testMaskCharUnicodeArg() throws Exception {
    String query = "select \n" +
      " mask(cast('TestString-12345' as varchar(100)), '大', '小', '数', ':') as c1";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("大小小小大小小小小小:数数数数数")
      .build()
      .run();
  }
  */

  /** bug unrelated to the mask function - select 'TestString-世界-你好-一二三-12345'  also fails
  @Test
  public void testMaskCharUnicodeInput() throws Exception {
    String query = "select \n" +
      " mask(cast('TestString-世界-你好-一二三-12345' as varchar(100)), 'Y', 'y', '0', ':') as c1";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("YyyyYyyyyy:::::::::::00000")
      .build()
      .run();
  }
  */

  @Test
  public void testMaskCharUnicodeArgFromDataset() throws Exception {
   String query = "select \n" +
     " mask(v, '大', '小', '数', ':') as c1 from cp.\"parquet/test_col_masking.parquet\"";

   testBuilder()
     .unOrdered()
     .sqlQuery(query)
     .baselineColumns("c1")
     .baselineValues("大小小小大小小小小小:数数数数数")
     .build()
     .run();
  }

  @Test
  public void testMaskCharUnicodeInputFromDataset() throws Exception {
   String query = "select \n" +
     " mask(v) as c1 from cp.\"parquet/test_col_masking_unicode.parquet\"";

   testBuilder()
     .unOrdered()
     .sqlQuery(query)
     .baselineColumns("c1")
     .baselineValues("XxxxXxxxxx-世界-你好-一二三-nnnnn")
     .build()
     .run();
  }

  @Test
  public void testMaskCharUnicodeInputWithArgsFromDataset() throws Exception {
     String query = "select \n" +
       " mask(v, 'Y', 'y', '0', ':') as c1 from cp.\"parquet/test_col_masking_unicode.parquet\"";

     testBuilder()
       .unOrdered()
       .sqlQuery(query)
       .baselineColumns("c1")
       .baselineValues("YyyyYyyyyy:::::::::::00000")
       .build()
       .run();
  }

  @Test
  public void testMaskCharUnicodeInputAndArgFromDataset() throws Exception {
    String query = "select \n" +
      " mask(v, '大', '小', '数', '特') as c1 from cp.\"parquet/test_col_masking_unicode.parquet\"";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("大小小小大小小小小小特特特特特特特特特特特数数数数数")
      .build()
      .run();
  }

  @Test
  public void testMaskCharWithArgs() throws Exception {
    String query = "select \n" +
      " mask('TestString-12345') as c1,\n" +
      " mask('TestString-12345', 'Y', 'y', 'z') as c2,\n" +
      " mask('TestString-12345', 'Y', 'y', '0', ':') as c3,\n" +
      " mask(cast('TestString-12345' as varchar(24)), 'x', 'x', 'x', -1, '1', 1, 0, -1) as c4\n" //Ranger Policy: show only year
      ;

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1",
        "c2",
        "c3",
        "c4"
         )
      .baselineValues("XxxxXxxxxx-nnnnn",
        "YyyyYyyyyy-zzzzz",
        "YyyyYyyyyy:00000",
        "xxxxxxxxxx-xxxxx"
        )
      .build()
      .run();
  }

  @Test
  public void testMaskInt() throws Exception {
    String query = "select \n" +
      " mask(cast(23456 as int)) as c1";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(11111)
      .build()
      .run();
  }

  @Test
  public void testMaskInt0() throws Exception {
    String query = "select \n" +
      " mask(cast(0 as int)) as c1";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(0)
      .build()
      .run();
  }

  @Test
  public void testMaskIntWithArgs() throws Exception {
    String query = "select \n" +
      " mask(cast(-23456 as int), -1, -1, -1, -1, 8) as c1," +
      " mask(cast(23456 as int), -1, -1, -1, -1, 88) as c2";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1", "c2")
      .baselineValues(-88888, 11111)
      .build()
      .run();
  }

  @Test
  //Ranger Policy: show only year
  public void testMaskIntWithArgsFromDataset() throws Exception {
    String query = "select \n" +
      " mask(i, 'x', 'x', 'x', -1, '1', 1, 0, -1) as c1 from cp.\"parquet/test_col_masking.parquet\"";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(11111)
      .build()
      .run();
  }

  @Test
  public void testMaskIntTooManyArgs() throws Exception {
    String query = "select \n" +
      " mask(cast(23456 as int), -1, -1, -1, -1, '8', 1, 2, 3, 4, 5, 6, 7, 8, 9) as c1";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(88888)
      .build()
      .run();
  }

  @Test
  public void testMaskBigInt() throws Exception {
    String query = "select \n" +
      " mask(cast(23456 as bigint)) as c1";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(11111L)
      .build()
      .run();
  }

  @Test
  public void testMaskBigIntFromDataset() throws Exception {
    String query = "select \n" +
      " mask(cast(i as bigint)) as c1 from cp.\"parquet/test_col_masking.parquet\"";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(11111L)
      .build()
      .run();
  }

  @Test
  public void testMaskFloatFromDataset() throws Exception {
    String query = "select \n" +
      " mask(f) as c1 from cp.\"parquet/test_col_masking.parquet\"";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(null)
      .build()
      .run();
  }

  @Test
  public void testMaskTimestampFromDataset() throws Exception {
    String query = "select \n" +
      " mask(t) as c1 from cp.\"parquet/test_col_masking.parquet\"";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(null)
      .build()
      .run();
  }

  @Test
  public void testMaskDateFromDataset() throws Exception {
    String query = "select extract(year from masked_date) y, extract(month from masked_date) m, extract(day from masked_date) d from\n" +
      " (select mask(a) as masked_date from cp.\"parquet/test_col_masking_unicode.parquet\")";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("y", "m", "d")
      .baselineValues(0L,1L,1L)
      .build()
      .run();
  }

  @Test
  public void testMaskDateWithArg() throws Exception {
    String query = "select extract(year from masked_date) y, extract(month from masked_date) m, extract(day from masked_date) d from\n" +
      " (select mask(cast('2016-04-20' as DATE),1,1,1,1,1,8,7,8) as masked_date)";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("y", "m", "d")
      .baselineValues(8L,8L,8L)
      .build()
      .run();
  }

  @Test
  public void testMaskDateWithArg1() throws Exception {
    String query = "select extract(year from masked_date) y, extract(month from masked_date) m, extract(day from masked_date) d from\n" +
      " (select mask(cast('2016-04-20' as DATE),-1,-1,-1,-1,-1,-1,0,0) as masked_date)";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("y", "m", "d")
      .baselineValues(0L,1L,20L)
      .build()
      .run();
  }

  @Test
  //Ranger Policy: Show only year
  public void testMaskDateWithArg2() throws Exception {
    String query = "select extract(year from masked_date) y, extract(month from masked_date) m, extract(day from masked_date) d from\n" +
      " (select mask(a, 'x', 'x', 'x', -1, '1', 1, 0, -1) as masked_date from cp.\"parquet/test_col_masking_unicode.parquet\")";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("y", "m", "d")
      .baselineValues(2016L,1L,1L)
      .build()
      .run();
  }

  @Test
  public void testMaskDateWithArg3() throws Exception {
    String query = "select extract(year from masked_date) y, extract(month from masked_date) m, extract(day from masked_date) d from\n" +
      " (select mask(cast('2016-04-20' as DATE), -1,-1,-1,-1,-1,-1,-1,-1) as masked_date)";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("y", "m", "d")
      .baselineValues(2016L,4L,20L)
      .build()
      .run();
  }

  @Test
  public void testMaskDateTooManyArgs() throws Exception {
    String query = "select extract(year from masked_date) y, extract(month from masked_date) m, extract(day from masked_date) d from\n" +
      " (select mask(cast('2016-04-20' as DATE), -1,-1,-1,-1,-1,-1,-1,-1,1,2,3,4,5,6,7) as masked_date)";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("y", "m", "d")
      .baselineValues(2016L,4L,20L)
      .build()
      .run();
  }

  @Test
  public void testMaskShowFirstNCharFromDataset() throws Exception {
    String query = "select \n" +
      " mask_show_first_n(v) as c1 from cp.\"parquet/test_col_masking.parquet\"\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("TestXxxxxx-nnnnn")
      .build()
      .run();
  }

  @Test
  public void testMaskShowFirstNCharWithArgs() throws Exception {
    String query = "select \n" +
      " mask_show_first_n(cast('TestString-12345' as varchar(100)), 3, 'Y', 'y', '8', ':') as c1,\n" +
      " mask_show_first_n(cast('TestString-12345' as varchar(100)), 3, 'Y', 'y', '8') as c2,\n" +
      " mask_show_first_n(cast('TestString-12345' as varchar(100)), 3, 'Y', 'y') as c3,\n" +
      " mask_show_first_n(cast('TestString-12345' as varchar(100)), 3, 'Y') as c4,\n" +
      " mask_show_first_n(cast('TestString-12345' as varchar(100)), 3) as c5,\n" +
      " mask_show_first_n(cast('TestString-12345' as varchar(100)), -1) as c6,\n" +
      " mask_show_first_n(cast('TestString-12345' as varchar(100)), -1, -1) as c7,\n" +
      " mask_show_first_n(cast('TestString-12345' as varchar(100)), -1, -1, -1) as c8,\n" +
      " mask_show_first_n(cast('TestString-12345' as varchar(100)), -1, -1, -1, -1) as c9,\n" +
      " mask_show_first_n(cast('TestString-12345' as varchar(100)), -1, -1, -1, -1, -1) as c10,\n" +
      " mask_show_first_n(cast('TestString-12345' as varchar(100)), 4, 'x', 'x', 'x', -1, '-1') as c11,\n" + //Ranger Policy: show first 4
      " mask_show_first_n(cast('TestString-12345' as varchar(100)), 3, 'Y', 'y', '8', ':', 1, 2, 3, 4, 5, 6, 7, 8) as c12\n"; //Too many args

      testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12")
      .baselineValues(
        "TesyYyyyyy:88888",
        "TesyYyyyyy-88888",
        "TesyYyyyyy-nnnnn",
        "TesxYxxxxx-nnnnn",
        "TesxXxxxxx-nnnnn",
        "XxxxXxxxxx-nnnnn",
        "TxxxSxxxxx-nnnnn",
        "TestString-nnnnn",
        "TestString-12345",
        "TestString-12345",
        "Testxxxxxx-xxxxx",
        "TesyYyyyyy:88888"
        )
      .build()
      .run();
  }

  @Test
  public void testMaskShowFirstNInt() throws Exception {
    String query = "select \n" +
      " mask_show_first_n(cast(23456 as int)) as c1\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(23451)
      .build()
      .run();
  }

  @Test
  public void testMaskShowFirstNIntFromDataset() throws Exception {
    String query = "select \n" +
      " mask_show_first_n(i) as c1 from cp.\"parquet/test_col_masking.parquet\"\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(23451)
      .build()
      .run();
  }

  @Test
  public void testMaskShowFirstNBigInt() throws Exception {
    String query = "select \n" +
      " mask_show_first_n(cast(23456 as bigint)) as c1\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(23451L)
      .build()
      .run();
  }
  @Test
  public void testMaskShowFirstNFloat() throws Exception {
    String query = "select \n" +
      " mask_show_first_n(cast(234.56 as float)) as c1\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(null)
      .build()
      .run();
  }
  @Test
  public void testMaskShowFirstNIntWithArgs() throws Exception {
    String query = "select \n" +
      " mask_show_first_n(cast(23456 as int)) as c1,\n" +
      " mask_show_first_n(cast(23456 as int), 3) as c2,\n" +
      " mask_show_first_n(cast(23456 as int), 3, 'Y') as c3,\n" +
      " mask_show_first_n(cast(23456 as int), 3, 'Y', 'y') as c4,\n" +
      " mask_show_first_n(cast(23456 as int), 3, 'Y', 'y', 'd') as c5,\n" +
      " mask_show_first_n(cast(23456 as int), 3, 'Y', 'y', 'd', ':') as c6,\n" +
      " mask_show_first_n(cast(23456 as int), 3, 'Y', 'y', 'd', ':', 8) as c7,\n" +
      " mask_show_first_n(cast(23456 as int), 3, 'Y', 'y', 'd', ':', '8') as c8,\n" +
      " mask_show_first_n(cast(23456 as int), 3, 'Y', 'y', 'd', ':', 88) as c9,\n" +
      " mask_show_first_n(cast(23456 as int), 3, -1, -1, -1, -1, 8) as c10,\n" +
      " mask_show_first_n(cast(23456 as int), 3, -1, -1, -1, -1, -1) as c11,\n" +
      " mask_show_first_n(cast(23456 as int), 4, 'x', 'x', 'x', -1, '1') as c12,\n" + //Ranger Policy: show first 4
      " mask_show_first_n(cast(23456 as int), 3, 'Y', 'y', 'd', ':', 8, 2, 2, 2, 2, 2, 2, 2, 2, 2) as c13"; // Too many args

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13")
      .baselineValues(
        23451,
        23411,
        23411,
        23411,
        23411,
        23411,
        23488,
        23488,
        23411,
        23488,
        23411,
        23451,
        23488
        )
      .build()
      .run();
  }

  @Test
  public void testMaskShowFirstNDate() throws Exception {
    String query = "select extract(year from masked_date) y, extract(month from masked_date) m, extract(day from masked_date) d from\n" +
      " (select mask_show_first_n(cast('2016-04-20' as date)) as masked_date)";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("y", "m", "d")
      .baselineValues(0L,1L,1L)
      .build()
      .run();
  }

  @Test
  public void testMaskShowFirstNDateWithArgs() throws Exception {
    String query = "select extract(year from masked_date) y, extract(month from masked_date) m, extract(day from masked_date) d from\n" +
      " (select mask_show_first_n(cast('2016-04-20' as DATE), 1, 1, 1, 1, 1, 1, 8, 7, 8) as masked_date)";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("y", "m", "d")
      .baselineValues(8L,8L,8L)
      .build()
      .run();
  }

  @Test
  //Ranger Policy: show first 4
  public void testMaskShowFirstNDateWithArgsFromDataset() throws Exception {
    String query = "select extract(year from masked_date) y, extract(month from masked_date) m, extract(day from masked_date) d from\n" +
      " (select mask_show_first_n(a, 4, 'x', 'x', 'x', -1, '1') as masked_date from cp.\"parquet/test_col_masking.parquet\")";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("y", "m", "d")
      .baselineValues(0L,1L,1L)
      .build()
      .run();
  }

  @Test
  public void testMaskShowLastNChar() throws Exception {
    String query = "select \n" +
      " mask_show_last_n('TestString-12345') as c1\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("XxxxXxxxxx-n2345")
      .build()
      .run();
  }

  @Test
  public void testMaskShowLastNCharWithArgs() throws Exception {
    String query = "select \n" +
      " mask_show_last_n(cast('TestString-12345' as varchar(100)), 3, 'Y', 'y', '8', ':') as c1\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("YyyyYyyyyy:88345")
      .build()
      .run();
  }

  @Test
  //Ranger Policy: show last 4
  public void testMaskShowLastNCharWithArgsFromDataset() throws Exception {
    String query = "select \n" +
      " mask_show_last_n(v, 4, 'x', 'x', 'x', -1, '1') as c1 from cp.\"parquet/test_col_masking.parquet\"\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("xxxxxxxxxx-x2345")
      .build()
      .run();
  }

  @Test
  //Ranger Policy: show last 4
  public void testMaskShowLastNIntWithArgsFromDataset() throws Exception {
    String query = "select \n" +
      " mask_show_last_n(i, 4, 'x', 'x', 'x', -1, '1') as c1 from cp.\"parquet/test_col_masking.parquet\"\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(13456)
      .build()
      .run();
  }

  @Test
  //Ranger Policy: show first 4
  public void testMaskShowLastNDateWithArgsFromDataset() throws Exception {
    String query = "select extract(year from masked_date) y, extract(month from masked_date) m, extract(day from masked_date) d from\n" +
      " (select mask_show_last_n(a, 4, 'x', 'x', 'x', -1, '1') as masked_date from cp.\"parquet/test_col_masking.parquet\")";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("y", "m", "d")
      .baselineValues(0L,1L,1L)
      .build()
      .run();
  }

  @Test
  public void testMaskFirstNChar() throws Exception {
    String query = "select \n" +
      " mask_first_n('TestString-12345') as c1\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("XxxxString-12345")
      .build()
      .run();
  }

  @Test
  public void testMaskFirstNCharWithArgsFromDataset() throws Exception {
    String query = "select \n" +
      " mask_first_n(v, 3, 'Y', 'y', '8', ':') as c1 from cp.\"parquet/test_col_masking.parquet\"\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("YyytString-12345")
      .build()
      .run();
  }

  @Test
  public void testMaskFirstNIntWithArgsFromDataset() throws Exception {
    String query = "select \n" +
      " mask_first_n(i, 4, 'x', 'x', 'x', -1, '1') as c1 from cp.\"parquet/test_col_masking.parquet\"\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(11116)
      .build()
      .run();
  }

  @Test
  public void testMaskFirstNBigIntWithArgsFromDataset() throws Exception {
    String query = "select \n" +
      " mask_first_n(cast(i as bigint), 4, 'x', 'x', 'x', -1, '1') as c1 from cp.\"parquet/test_col_masking.parquet\"\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(11116L)
      .build()
      .run();
  }

  @Test
  public void testMaskFirstNDateWithArgsFromDataset() throws Exception {
    String query = "select extract(year from masked_date) y, extract(month from masked_date) m, extract(day from masked_date) d from\n" +
      " (select mask_first_n(a, 4, 'x', 'x', 'x', -1, '1') as masked_date from cp.\"parquet/test_col_masking.parquet\")";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("y", "m", "d")
      .baselineValues(0L,1L,1L)
      .build()
      .run();
  }

  @Test
  public void testMaskLastNChar() throws Exception {
    String query = "select \n" +
      " mask_last_n('TestString-12345') as c1\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("TestString-1nnnn")
      .build()
      .run();
  }

  @Test
  public void testMaskLastNCharWithArgsFromDataset() throws Exception {
    String query = "select \n" +
      " mask_last_n(v, 3, 'Y', 'y', '8', ':') as c1 from cp.\"parquet/test_col_masking.parquet\"\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues("TestString-12888")
      .build()
      .run();
  }

  @Test
  public void testMaskLastNIntWithArgsFromDataset() throws Exception {
    String query = "select \n" +
      " mask_last_n(i, 4, 'x', 'x', 'x', -1, '1') as c1 from cp.\"parquet/test_col_masking.parquet\"\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(21111)
      .build()
      .run();
  }

  @Test
  public void testMaskLastNBigIntWithArgsFromDataset() throws Exception {
    String query = "select \n" +
      " mask_last_n(cast(i as bigint), 4, 'x', 'x', 'x', -1, '1') as c1 from cp.\"parquet/test_col_masking.parquet\"\n";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("c1")
      .baselineValues(21111L)
      .build()
      .run();
  }

  @Test
  public void testMaskLastNDateWithArgsFromDataset() throws Exception {
    String query = "select extract(year from masked_date) y, extract(month from masked_date) m, extract(day from masked_date) d from\n" +
      " (select mask_last_n(a, 4, 'x', 'x', 'x', -1, '1') as masked_date from cp.\"parquet/test_col_masking.parquet\")";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("y", "m", "d")
      .baselineValues(0L,1L,1L)
      .build()
      .run();
  }
}
