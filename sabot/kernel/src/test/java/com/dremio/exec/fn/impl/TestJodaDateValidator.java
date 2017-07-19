/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.fn.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.BaseTestQuery;
import com.dremio.common.expression.fn.JodaDateValidator;

/**
 * Tests for {@link com.dremio.common.expression.fn.JodaDateValidator#toJodaFormat(String)} (String)}
 */
public class TestJodaDateValidator extends BaseTestQuery {
  @Rule
  public final ExpectedException thrownException = ExpectedException.none();

  private boolean patternShouldThrowParseException(String pattern) {
    try {
      JodaDateValidator.toJodaFormat(pattern);
    } catch (ParseException e) {
      return true;
    }

    return false;
  }

  @Test
  public void testToJodaQuoted() throws Exception {
    assertEquals("'anything can live here, AD BC or here PA mayber here?!'", JodaDateValidator.toJodaFormat("\"anything can live here, AD BC or here PA mayber here?!\""));
    assertEquals("G'some text'a'more text'", JodaDateValidator.toJodaFormat("AD\"some text\"PM\"more text\""));

    thrownException.expect(ParseException.class);
    JodaDateValidator.toJodaFormat("P\"a string\"");
  }

  @Test
  public void testToJodaQuotedNonTerminating() throws Exception {
    thrownException.expect(ParseException.class);
    JodaDateValidator.toJodaFormat("\"a string");
  }

  @Test
  public void testToJodaPassthroughChars() throws Exception {
    assertEquals("-G/,.;:G", JodaDateValidator.toJodaFormat("-AD/,.;:AD"));

    thrownException.expect(ParseException.class);
    JodaDateValidator.toJodaFormat("P:M");
  }

  @Test
  public void testToJodaMonth() throws Exception {
    assertEquals("MMMM", JodaDateValidator.toJodaFormat("MONTH"));
    assertEquals("MMM", JodaDateValidator.toJodaFormat("MON"));
    assertEquals("MMM:", JodaDateValidator.toJodaFormat("MON:"));

    thrownException.expect(ParseException.class);
    JodaDateValidator.toJodaFormat("MO:");
  }

  @Test
  public void testToJodaTimezone() throws Exception {
    assertEquals("z", JodaDateValidator.toJodaFormat("TZD"));
    assertEquals("ZZ", JodaDateValidator.toJodaFormat("TZO"));
    assertEquals("ZZ", JodaDateValidator.toJodaFormat("TZH:TZM"));

    thrownException.expect(ParseException.class);
    JodaDateValidator.toJodaFormat("TZH:TZ");
  }

  @Test
  public void testToJodaMilliseconds() throws Exception {
    assertEquals("S", JodaDateValidator.toJodaFormat("F"));
    assertEquals("SS", JodaDateValidator.toJodaFormat("FF"));
    assertEquals("SSS", JodaDateValidator.toJodaFormat("fff"));
  }

  @Test
  public void testToJodaAll() throws Exception {
    assertEquals("yyyy-MM-dd HH:mm:ss.SS z", JodaDateValidator.toJodaFormat("YYYY-MM-DD HH24:MI:SS.FF TZD"));
    assertEquals("G a C.ww yy DDD MMM hh ZZ", JodaDateValidator.toJodaFormat("AD AM CC.WW YY DDD MON HH TZO"));
    assertEquals("G a MMMM hh ZZ", JodaDateValidator.toJodaFormat("BC PM MONTH HH12 TZH:TZM"));

    // bunch of invalid patterns
    assertTrue(patternShouldThrowParseException("MO:"));
    assertTrue(patternShouldThrowParseException("PA"));
    assertTrue(patternShouldThrowParseException("AC"));
    assertTrue(patternShouldThrowParseException("C"));
    assertTrue(patternShouldThrowParseException("H24"));
    assertTrue(patternShouldThrowParseException("HH22"));
    assertTrue(patternShouldThrowParseException("HH2 "));
    assertTrue(patternShouldThrowParseException("HH2:4"));
    assertTrue(patternShouldThrowParseException("TZH:TZMM"));
  }

  @Test
  public void testToJodaLowercase() throws Exception {
    assertEquals("yyyy-MM-dd HH:mm:ss.SS z", JodaDateValidator.toJodaFormat("yyyy-mm-dd hh24:mi:ss.ff tzd"));
    assertEquals("G a C.ww yy DDD MMM hh ZZ", JodaDateValidator.toJodaFormat("ad am cc.ww yy ddd mon hh tzo"));
    assertEquals("G a MMMM hh ZZ", JodaDateValidator.toJodaFormat("bc pm month hh12 tzh:tzm"));

    // bunch of invalid patterns
    assertTrue(patternShouldThrowParseException("mo:"));
    assertTrue(patternShouldThrowParseException("pa"));
    assertTrue(patternShouldThrowParseException("ac"));
    assertTrue(patternShouldThrowParseException("c"));
    assertTrue(patternShouldThrowParseException("h24"));
    assertTrue(patternShouldThrowParseException("hh22"));
    assertTrue(patternShouldThrowParseException("hhH2 "));
    assertTrue(patternShouldThrowParseException("hhH2:4"));
    assertTrue(patternShouldThrowParseException("tzh:tzmm"));
  }
}
