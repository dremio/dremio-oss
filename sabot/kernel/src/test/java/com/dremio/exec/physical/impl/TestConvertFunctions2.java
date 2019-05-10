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
package com.dremio.exec.physical.impl;


import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.util.JodaDateUtility;
import com.dremio.sabot.BaseTestFunction;

public class TestConvertFunctions2 extends BaseTestFunction {

  // "1980-01-01 01:23:45.678"
  private static final String DATE_TIME_BE = "\\x00\\x00\\x00\\x49\\x77\\x85\\x1f\\x8e";
  private static final String DATE_TIME_LE = "\\x8e\\x1f\\x85\\x77\\x49\\x00\\x00\\x00";

  private static LocalTime time = LocalTime.parse("01:23:45.678", JodaDateUtility.getTimeFormatter());
  private static LocalDateTime date = LocalDateTime.parse("1980-01-01", JodaDateUtility.getDateTimeFormatter());

  @Ignore("DX-4929")
  @Test
  public void testDateTime1() throws Throwable {
    verifyConversionExpression("(convert_from(binary_string('" + DATE_TIME_BE + "'), 'TIME_EPOCH_BE'))", time);
  }

  @Ignore("DX-4929")
  @Test
  public void testDateTime2() throws Throwable {
    verifyConversionExpression("convert_from(binary_string('" + DATE_TIME_LE + "'), 'TIME_EPOCH')", time);
  }

  @Ignore("DX-4929")
  @Test
  public void testDateTime3() throws Throwable {
    verifyConversionExpression("convert_from(binary_string('" + DATE_TIME_BE + "'), 'DATE_EPOCH_BE')", date );
  }

  @Ignore("DX-4929")
  @Test
  public void testDateTime4() throws Throwable {
    verifyConversionExpression("convert_from(binary_string('" + DATE_TIME_LE + "'), 'DATE_EPOCH')", date);
  }

  @Test
  public void testFixedInts4() throws Throwable {
    verifyConversionExpression("convert_from(binary_string('\\xBE\\xBA\\xFE\\xCA'), 'INT')", 0xCAFEBABE);
  }

  @Test
  public void testFixedInts5() throws Throwable {
    verifyConversionExpression("convert_from(binary_string('\\xCA\\xFE\\xBA\\xBE'), 'INT_BE')", 0xCAFEBABE);
  }

  @Test
  public void testFixedInts6() throws Throwable {
    verifyConversionExpression("convert_from(binary_string('\\xEF\\xBE\\xAD\\xDE\\xBE\\xBA\\xFE\\xCA'), 'BIGINT')", 0xCAFEBABEDEADBEEFL);
  }

  @Test
  public void testFixedInts7() throws Throwable {
    verifyConversionExpression("convert_from(binary_string('\\xCA\\xFE\\xBA\\xBE\\xDE\\xAD\\xBE\\xEF'), 'BIGINT_BE')", 0xCAFEBABEDEADBEEFL);
  }

  @Test
  public void testFixedInts8() throws Throwable {
    verifyConversionExpression("convert_from(convert_to(cast(77 as varchar(2)), 'INT_BE'), 'INT_BE')", 77);
  }

  @Test
  public void testFixedInts9() throws Throwable {
    verifyConversionExpression("convert_to(cast(77 as varchar(2)), 'INT_BE')", new byte[] {0, 0, 0, 77});
  }

  @Test
  public void testFixedInts10() throws Throwable {
    verifyConversionExpression("convert_to(cast(77 as varchar(2)), 'INT')", new byte[] {77, 0, 0, 0});
  }

  @Test
  public void testFixedInts11() throws Throwable {
    verifyConversionExpression("convert_to(77, 'BIGINT_BE')", new byte[] {0, 0, 0, 0, 0, 0, 0, 77});
  }

  @Test
  public void testFixedInts12() throws Throwable {
    verifyConversionExpression("convert_to(9223372036854775807, 'BIGINT')", new byte[] {-1, -1, -1, -1, -1, -1, -1, 0x7f});
  }

  @Test
  public void testFixedInts13() throws Throwable {
    verifyConversionExpression("convert_to(-9223372036854775808, 'BIGINT')", new byte[] {0, 0, 0, 0, 0, 0, 0, (byte)0x80});
  }

  @Test
  public void testVInts1() throws Throwable {
    verifyConversionExpression("convert_to(cast(0 as int), 'INT_HADOOPV')", new byte[] {0});
  }

  @Test
  public void testVInts2() throws Throwable {
    verifyConversionExpression("convert_to(cast(128 as int), 'INT_HADOOPV')", new byte[] {-113, -128});
  }

  @Test
  public void testVInts3() throws Throwable {
    verifyConversionExpression("convert_to(cast(256 as int), 'INT_HADOOPV')", new byte[] {-114, 1, 0});
  }

  @Test
  public void testVInts4() throws Throwable {
    verifyConversionExpression("convert_to(cast(65536 as int), 'INT_HADOOPV')", new byte[] {-115, 1, 0, 0});
  }

  @Test
  public void testVInts5() throws Throwable {
    verifyConversionExpression("convert_to(cast(16777216 as int), 'INT_HADOOPV')", new byte[] {-116, 1, 0, 0, 0});
  }

  @Test
  public void testVInts6() throws Throwable {
    verifyConversionExpression("convert_to(4294967296, 'BIGINT_HADOOPV')", new byte[] {-117, 1, 0, 0, 0, 0});
  }

  @Test
  public void testVInts7() throws Throwable {
    verifyConversionExpression("convert_to(1099511627776, 'BIGINT_HADOOPV')", new byte[] {-118, 1, 0, 0, 0, 0, 0});
  }

  @Test
  public void testVInts8() throws Throwable {
    verifyConversionExpression("convert_to(281474976710656, 'BIGINT_HADOOPV')", new byte[] {-119, 1, 0, 0, 0, 0, 0, 0});
  }

  @Test
  public void testVInts9() throws Throwable {
    verifyConversionExpression("convert_to(72057594037927936, 'BIGINT_HADOOPV')", new byte[] {-120, 1, 0, 0, 0, 0, 0, 0, 0});
  }

  @Test
  public void testVInts10() throws Throwable {
    verifyConversionExpression("convert_to(9223372036854775807, 'BIGINT_HADOOPV')", new byte[] {-120, 127, -1, -1, -1, -1, -1, -1, -1});
  }

  @Test
  public void testVInts11() throws Throwable {
    verifyConversionExpression("convert_from(binary_string('\\x88\\x7f\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF'), 'BIGINT_HADOOPV')", 9223372036854775807L);
  }

  @Test
  public void testVInts12() throws Throwable {
    verifyConversionExpression("convert_to(-9223372036854775808, 'BIGINT_HADOOPV')", new byte[] {-128, 127, -1, -1, -1, -1, -1, -1, -1});
  }

  @Test
  public void testVInts13() throws Throwable {
    verifyConversionExpression("convert_from(binary_string('\\x80\\x7f\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF'), 'BIGINT_HADOOPV')", -9223372036854775808L);
  }

  @Test
  public void testBool1() throws Throwable {
    verifyConversionExpression("convert_from(binary_string('\\x01'), 'BOOLEAN_BYTE')", true);
  }

  @Test
  public void testBool2() throws Throwable {
    verifyConversionExpression("convert_from(binary_string('\\x00'), 'BOOLEAN_BYTE')", false);
  }

  @Test
  public void testBool3() throws Throwable {
    verifyConversionExpression("convert_to(true, 'BOOLEAN_BYTE')", new byte[] {1});
  }

  @Test
  public void testBool4() throws Throwable {
    verifyConversionExpression("convert_to(false, 'BOOLEAN_BYTE')", new byte[] {0});
  }


  @Test
  public void testFloats2() throws Throwable {
    verifyConversionExpression("convert_from(convert_to(cast(77 as float4), 'FLOAT'), 'FLOAT')", new Float(77.0));
  }

  @Test
  public void testFloats2be() throws Throwable {
    verifyConversionExpression("convert_from(convert_to(cast(77 as float4), 'FLOAT_BE'), 'FLOAT_BE')", new Float(77.0));
  }

  @Test
  public void testFloats3() throws Throwable {
    verifyConversionExpression("convert_to(cast(1.4e-45 as float4), 'FLOAT')", new byte[] {1, 0, 0, 0});
  }

  @Test
  public void testFloats4() throws Throwable {
    verifyConversionExpression("convert_to(cast(3.4028235e+38 as float4), 'FLOAT')", new byte[] {-1, -1, 127, 127});
  }

  @Test
  public void testFloats5() throws Exception {
    verifyConversionExpression("convert_from(convert_to(cast(77 as float8), 'DOUBLE'), 'DOUBLE')", 77.0);
  }

  @Test
  public void testFloats5be() throws Exception {
    verifyConversionExpression("convert_from(convert_to(cast(77 as float8), 'DOUBLE_BE'), 'DOUBLE_BE')", 77.0);
  }

  @Test
  public void testFloats6() throws Exception {
    verifyConversionExpression("convert_to(cast(77 as float8), 'DOUBLE')", new byte[] {0, 0, 0, 0, 0, 64, 83, 64});
  }

  @Test
  public void testFloats7() throws Exception {
    verifyConversionExpression("convert_to(4.9e-324d, 'DOUBLE')", new byte[] {1, 0, 0, 0, 0, 0, 0, 0});
  }

  @Test
  public void testFloats8() throws Exception {
    verifyConversionExpression("convert_to(1.7976931348623157e+308d, 'DOUBLE')", new byte[] {-1, -1, -1, -1, -1, -1, -17, 127});
  }

  @Test
  public void testUTF8() throws Throwable {
    verifyConversionExpression("convert_from(binary_string('dremio'), 'UTF8')", "dremio");
    verifyConversionExpression("convert_to('dremio', 'UTF8')", new byte[] {'d', 'r', 'e', 'm', 'i', 'o'});
  }

  protected <T> void verifyConversionExpression(String expression, T expectedResults) throws Exception {
    expression = expression.replace("\\", "\\\\"); // "\\\\" => Java => "\\" => AntlrParser "\"
    testFunction(expression, expectedResults);
  }
}
