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
package com.dremio.exec.physical.impl;

import static com.dremio.sabot.Fixtures.NULL_VARCHAR;

import com.dremio.sabot.BaseTestFunction;
import org.junit.Test;

public class TestStringFunctions extends BaseTestFunction {
  /** Returns the string 's' repeated 'n' times */
  private String repeat(String s, int n) {
    return new String(new char[n]).replace("\0", s);
  }

  @Test
  public void charLength() {
    testFunctions(
        new Object[][] {
          {"char_length('aababcdf')", 8},
          {"char_length('')", 0},
          {"char_length(c0)", "abc", 3},
          {"character_length('aababcdf')", 8},
          {"character_length('')", 0},
          {"character_length(c0)", "abc", 3},
          {"length('aababcdf')", 8},
          {"length('')", 0},
          {"length(c0)", "abc", 3}
        });
  }

  @Test
  public void md5HashTest() {
    testFunctions(
        new Object[][] {
          {
            "md5(c0)",
            "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃnY [ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ]",
            "a633460644425b44e0e023d6980849cc"
          },
          {
            "hashMD5(c0)",
            "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃnY [ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ]".getBytes(),
            "a633460644425b44e0e023d6980849cc"
          },
        });
  }

  @Test
  public void sha1HashTest() {
    testFunctions(
        new Object[][] {
          {
            "sha1(c0)",
            "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃnY [ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ]",
            "8c6a880ce350769627caa70f3526aeedcc4c959d"
          },
          {
            "hashSHA1(c0)",
            "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃnY [ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ]".getBytes(),
            "8c6a880ce350769627caa70f3526aeedcc4c959d"
          },
        });
  }

  @Test
  public void sha256HashTest() {
    testFunctions(
        new Object[][] {
          {
            "sha256(c0)",
            "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃnY [ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ]",
            "68e676f563660bbebf718d3e062e1a56339b7ba61e48a116c9844298b1d41641"
          },
          {
            "hashSHA256(c0)",
            "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃnY [ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ]".getBytes(),
            "68e676f563660bbebf718d3e062e1a56339b7ba61e48a116c9844298b1d41641"
          },
        });
  }

  @Test
  public void hexConversion() {
    testFunctions(
        new Object[][] {
          {"to_hex(binary_string('\\\\x11\\\\x22'))", "1122"},
          {"string_binary(from_hex('1112'))", "\\x11\\x12"},
          {"string_binary(unhex('1112'))", "\\x11\\x12"},
          {"to_hex(repeatstr(binary_string('\\\\x11\\\\x22'),256))", repeat("1122", 256)},
          {"hex(repeatstr(binary_string('\\\\x11\\\\x22'),256))", repeat("1122", 256)},
          {"to_hex(binary_string(repeatstr('\\\\x11\\\\x22',256)))", repeat("1122", 256)},
          {"hex(binary_string(repeatstr('\\\\x11\\\\x22',256)))", repeat("1122", 256)},
          {"to_hex(c0)", 6713199L, "666F6F"},
          {"hex(c0)", 6713199L, "666F6F"},
        });
  }

  @Test
  public void similar() {
    testFunctions(
        new Object[][] {
          {"similar('abc', 'abc')", true},
          {"similar('abc', 'a')", false},
          {"similar('abc', '%(b|d)%')", true},
          {"similar('abc', '(b|c)%')", false}
        });
  }

  @Test
  public void ltrim() {
    testFunctions(
        new Object[][] {
          {"ltrim('   abcdef  ')", "abcdef  "},
          {"ltrim('abcdef ')", "abcdef "},
          {"ltrim('    ')", ""},
          {"ltrim('abcd')", "abcd"},
          {"ltrim('  çåå†b')", "çåå†b"},
          {"ltrim('')", ""},
          {"ltrim('abcdef', 'abc')", "def"},
          {"ltrim('abcdef', '')", "abcdef"},
          {"ltrim('abcdabc', 'abc')", "dabc"},
          {"ltrim('abc', 'abc')", ""},
          {"ltrim('abcd', 'efg')", "abcd"},
          {"ltrim('ååçåå†eç†Dd', 'çåå†')", "eç†Dd"},
          {"ltrim('ç†ååçåå†', 'çå†')", ""},
          {"ltrim('åéçå', 'åé')", "çå"},
          {"ltrim('', 'abc')", ""},
          {"ltrim('', '')", ""}
        });
  }

  @Test
  public void trim() {
    testFunctions(
        new Object[][] {
          {"btrim('   abcdef  ')", "abcdef"},
          {"btrim('abcdef ')", "abcdef"},
          {"btrim('  abcdef ')", "abcdef"},
          {"btrim('    ')", ""},
          {"btrim('abcd')", "abcd"},
          {"btrim('  çåå†b  ')", "çåå†b"},
          {"btrim('')", ""},
          {"btrim('     efghI e', 'e ')", "fghI"},
          {"btrim('a', 'a')", ""},
          {"btrim('', '')", ""},
          {"btrim('abcd', 'efg')", "abcd"},
          {"btrim('ååçåå†Ddeç†', 'çåå†')", "Dde"},
          {"btrim('ç†ååçåå†', 'çå†')", ""},
          {"btrim('åe†çå', 'çå')", "e†"},
          {"btrim('aAa!aAa', 'aA')", "!"},
          {"btrim(' aaa ', '')", " aaa "}
        });
  }

  @Test
  public void replace() {
    testFunctions(
        new Object[][] {
          {"replace('aababcdf', 'ab', 'AB')", "aABABcdf"},
          {"replace('aababcdf', 'a', 'AB')", "ABABbABbcdf"},
          {"replace('aababcdf', '', 'AB')", "aababcdf"},
          {"replace('aababcdf', 'ab', '')", "acdf"},
          {"replace('abc', 'abc', 'ABCD')", "ABCD"},
          {"replace('abc', 'abcdefg', 'ABCD')", "abc"}
        });
  }

  @Test
  public void rtrim() {
    testFunctions(
        new Object[][] {
          {"rtrim('   abcdef  ')", "   abcdef"},
          {"rtrim('  abcdef')", "  abcdef"},
          {"rtrim('    ')", ""},
          {"rtrim('abcd')", "abcd"},
          {"rtrim('  ')", ""},
          {"rtrim('çåå†b  ')", "çåå†b"},
          {"rtrim('abcdef', 'def')", "abc"},
          {"rtrim('abcdef', '')", "abcdef"},
          {"rtrim('ABdabc', 'abc')", "ABd"},
          {"rtrim('abc', 'abc')", ""},
          {"rtrim('abcd', 'efg')", "abcd"},
          {"rtrim('eDdç†ååçåå†', 'çåå†')", "eDd"},
          {"rtrim('ç†ååçåå†', 'çå†')", ""},
          {"rtrim('åéçå', 'çå')", "åé"},
          {"rtrim('', 'abc')", ""},
          {"rtrim('', '')", ""}
        });
  }

  @Test
  public void concat() {
    testFunctions(
        new Object[][] {
          {"concat('abc', 'ABC')", "abcABC"},
          {"concat('abc', '')", "abc"},
          {"concat('', 'ABC')", "ABC"},
          {"concat('', '')", ""}
        });
  }

  @Test
  public void lower() {
    testFunctions(
        new Object[][] {
          {"lower('ABcEFgh')", "abcefgh"},
          {"lower('aBc')", "abc"},
          {"lower('')", ""},
          {"lower('ÂbĆDËFgh')", "âbćdëfgh"}
        });
  }

  @Test
  public void position() {
    testFunctions(
        new Object[][] {
          {"position('abc', 'AabcabcB')", 2},
          {"position('A', 'AabcabcB')", 1},
          {"position('', 'AabcabcB')", 0},
          {"position('abc', '')", 0},
          {"position('', '')", 0},
          {"position('abc', 'AabcabcB', 3)", 5},
          {"position('A', 'AabcabcB', 1)", 1},
          {"position('', 'AabcabcB', 1)", 0},
          {"position('abc', '', 1)", 0},
          {"position('', '', 5)", 0},
          {"position('foo', 'foofoo', 1)", 1},
          {"position('foo', 'foofoo', 2)", 4},
          {"position('foo', 'foofoo', 3)", 4},
          {"position('foo', 'foofoo', 4)", 4},
          {"position('foo', 'foofoo', 5)", 0},
          {"position('abc', '', 1)", 0},
          {"position('', '', 5)", 0},
          {"locate('abc', 'AabcabcB')", 2},
          {"locate('A', 'AabcabcB')", 1},
          {"locate('', 'AabcabcB')", 0},
          {"locate('abc', '')", 0},
          {"locate('', '')", 0},
          {"locate('abc', 'AabcabcB', 3)", 5},
          {"position('A', 'AabcabcB', 1)", 1},
          {"locate('', 'AabcabcB', 1)", 0},
          {"locate('abc', '', 1)", 0},
          {"locate('', '', 5)", 0},
          {"strpos('AabcabcB', 'abc')", 2},
          {"strpos('', 'AabcabcB')", 0},
          {"strpos('', 'abc')", 0},
          {"strpos('', '')", 0}
        });
  }

  @Test
  public void right() {
    testFunctions(
        new Object[][] {
          {"right('abcdef', 2)", "ef"},
          {"right('abcdef', 6)", "abcdef"},
          {"right('abcdef', 7)", "abcdef"},
          {"right('abcdef', -2)", "cdef"},
          {"right('abcdef', -5)", "f"},
          {"right('abcdef', -6)", ""},
          {"right('abcdef', -7)", ""}
        });
  }

  @Test
  public void substr() {
    testFunctions(
        new Object[][] {
          {"substring('abcdef', 1, 3)", "abc"},
          {"substring('abcdef', 2, 3)", "bcd"},
          {"substring('abcdef', 2, 5)", "bcdef"},
          {"substring('abcdef', 2, 10)", "bcdef"},
          {"substring('abcdef', 0, 3)", "abc"},
          {"substring('abcdef', -1, 3)", "f"},
          {"substring('', 1, 2)", ""},
          {"substring('abcdef', 10, 2)", ""},
          {"substring('भारतवर्ष', 1, 4)", "भारत"},
          {"substring('भारतवर्ष', 5, 4)", "वर्ष"},
          {"substring('भारतवर्ष', 5, 5)", "वर्ष"},
          {"substring('abcdef', 3)", "cdef"},
          {"substring('abcdef', -2)", "ef"},
          {"substring('abcdef', 0)", "abcdef"},
          {"substring('abcdef', 10)", ""},
          {"substring('अपाचे ड्रिल', 7)", "ड्रिल"}
        });
  }

  @Test
  public void left() {
    testFunctions(
        new Object[][] {
          {"left('abcdef', 2)", "ab"},
          {"left('abcdef', 6)", "abcdef"},
          {"left('abcdef', 7)", "abcdef"},
          {"left('abcdef', -2)", "abcd"},
          {"left('abcdef', -5)", "a"},
          {"left('abcdef', -6)", ""},
          {"left('abcdef', -7)", ""}
        });
  }

  @Test
  public void lpad() {
    testFunctions(
        new Object[][] {
          {"lpad('abcdef', 0, 'abc')", ""},
          {"lpad('abcdef', -3, 'abc')", ""},
          {"lpad('abcdef', 6, 'abc')", "abcdef"},
          {"lpad('abcdef', 2, 'abc')", "ab"},
          {"lpad('abcdef', 2, '')", "ab"},
          {"lpad('abcdef', 10, '')", "abcdef"},
          {"lpad('abcdef', 10, 'A')", "AAAAabcdef"},
          {"lpad('abcdef', 10, 'AB')", "ABABabcdef"},
          {"lpad('abcdef', 10, 'ABC')", "ABCAabcdef"},
          {"lpad('abcdef', 10, 'ABCDEFGHIJKLMN')", "ABCDabcdef"}
        });
  }

  @Test
  public void regexreplace() {
    testFunctions(
        new Object[][] {
          {"regexp_replace('Thomas', '.[mN]a.', 'M')", "ThM"},
          {"regexp_replace('Thomas', '.[mN]a.', '')", "Th"},
          {"regexp_replace('Thomas', 'ef', 'AB')", "Thomas"}
        });
  }

  @Test
  public void testRegexpExtract() {
    testFunctions(
        new Object[][] {
          {"regexp_extract(c0, 'foo(.*?)(bar)', 2)", "foothebar", "bar"},
          {"regexp_extract(c0, '@(.*)', 0)", "john@test.com", "@test.com"},
          {"regexp_extract(c0, '(.*) (D.*)', 2)", "John Doe", "Doe"},
          {"regexp_extract(c0,'([a-z]+) ([a-z]+) - ([0-9]+)',c1)", "john doe - 124", 1, "john"},
          {"regexp_extract(c0,'([a-z]+) ([a-z]+) - ([0-9]+)',c1)", "john doe - 124", 2, "doe"},
          {
            "regexp_extract(c0,'([a-z]+) ([a-z]+) - ([0-9]+)',c1)",
            "john doe - 124",
            0,
            "john doe - 124"
          },
          {"regexp_extract(c0,'([a-z]+) ([a-z]+) - ([0-9]+)',c1)", "stringdonotmatch", 0, ""},
        });
  }

  @Test
  public void rpad() {
    testFunctions(
        new Object[][] {
          {"rpad('abcdef', 0, 'abc')", ""},
          {"rpad('abcdef', -3, 'abc')", ""},
          {"rpad('abcdef', 6, 'abc')", "abcdef"},
          {"rpad('abcdef', 2, 'abc')", "ab"},
          {"rpad('abcdef', 2, '')", "ab"},
          {"rpad('abcdef', 10, '')", "abcdef"},
          {"rpad('abcdef', 10, 'A')", "abcdefAAAA"},
          {"rpad('abcdef', 10, 'AB')", "abcdefABAB"},
          {"rpad('abcdef', 10, 'ABC')", "abcdefABCA"},
          {"rpad('abcdef', 10, 'ABCDEFGHIJKLMN')", "abcdefABCD"}
        });
  }

  @Test
  public void upper() {
    testFunctions(
        new Object[][] {
          {"upper('ABcEFgh')", "ABCEFGH"},
          {"upper('aBc')", "ABC"},
          {"upper('')", ""},
          {"upper('âBćDëFGH')", "ÂBĆDËFGH"}
        });
  }

  @Test
  public void stringfuncs() {
    testFunctions(
        new Object[][] {
          {" ascii('apache') ", 97},
          {" ascii('Apache') ", 65},
          {" ascii('अपाचे') ", -32},
          {" chr(65) ", "A"},
          {" btrim('xyxbtrimyyx', 'xy') ", "btrim"},
          {" repeatstr('Peace ', 3) ", "Peace Peace Peace "},
          {" repeatstr('हकुना मताता ', 2) ", "हकुना मताता हकुना मताता "},
          {" reverse('tictak') ", "katcit"},
          {" toascii('âpple','ISO-8859-1') ", "\u00C3\u00A2pple"},
          {" to_utf8('âpple','ISO-8859-1') ", "\u00C3\u00A2pple"},
          {" to_utf8(c0,'ISO-8859-1') ", NULL_VARCHAR, NULL_VARCHAR},
          {" reverse('मदन') ", "नदम"},
          {"substring(c0, 1, 4)", "alpha", "alph"},
          {"byte_substr(c0, -3, 2)", "alpha".getBytes(), "ph".getBytes()}
          // {"substring(c0, -3, 2)", "alphabeta", "ph"} (Invalid since we follow Postgres)
        });
  }

  @Test
  public void concatws() {
    testFunctions(
        new Object[][] {
          {"concat_ws(c0, c1, c2)", "हकुना", "john", "doe", "johnहकुनाdoe"},
          {"concat_ws(c0, c1, c2)", "-", "john", "doe", "john-doe"},
          {"concat_ws(c0, c1, c2)", "<>", "hello", "world", "hello<>world"},
          {"concat_ws(c0, c1, c2)", "jllkjsdhfg", "P18582D", "|", "P18582Djllkjsdhfg|"},
          {"concat_ws(c0, c1, c2)", "uiuikjk", NULL_VARCHAR, "|", "|"},
          {"concat_ws(c0, c1, c2)", NULL_VARCHAR, NULL_VARCHAR, "", NULL_VARCHAR},
          {"concat_ws(c0, c1, c2)", "", NULL_VARCHAR, "", ""},
          {"concat_ws(c0, c1, c2)", "-", NULL_VARCHAR, NULL_VARCHAR, ""},
          {"concat_ws(c0, c1, c2)", "-", "", "hello", "-hello"},
          {"concat_ws(c0, c1, c2)", "-", "hey", "hello", "hey-hello"},
          {"concat_ws(c0, c1, c2)", "-", "", "hello", "-hello"},
          {"concat_ws(c0, c1, c2)", NULL_VARCHAR, "hey", "hello", NULL_VARCHAR},
          {"concat_ws(c0, c1, c2)", "-", NULL_VARCHAR, "hello", "hello"},
          {"concat_ws(c0, c1, c2)", "-", "hey", NULL_VARCHAR, "hey"},
          {"concat_ws(c0, c1, c2, c3)", "#", "hey", "hello", "wow", "hey#hello#wow"},
          {"concat_ws(c0, c1, c2, c3)", "#", "", NULL_VARCHAR, "wow", "#wow"},
          {"concat_ws(c0, c1, c2, c3)", NULL_VARCHAR, "hey", "hello", "wow", NULL_VARCHAR},
          {"concat_ws(c0, c1, c2, c3)", "#", NULL_VARCHAR, "hello", "wow", "hello#wow"},
          {"concat_ws(c0, c1, c2, c3)", "#", "hey", NULL_VARCHAR, "wow", "hey#wow"},
          {"concat_ws(c0, c1, c2, c3)", "#", NULL_VARCHAR, NULL_VARCHAR, "wow", "wow"},
          {
            "concat_ws(c0, c1, c2, c3, c4)",
            "=",
            "hey",
            "hello",
            "wow",
            "awesome",
            "hey=hello=wow=awesome"
          },
          {
            "concat_ws(c0, c1, c2, c3, c4, c5)",
            "&&",
            "hey",
            "hello",
            "wow",
            "awesome",
            "super",
            "hey&&hello&&wow&&awesome&&super"
          },
        });
  }

  @Test
  public void formatNumber() {
    testFunctions(
        new Object[][] {
          {"format_number(c0, c1)", 10123.4444, 2, "10,123.44"},
          {"format_number(c0, c1)", 123456789.1234, 3, "123,456,789.123"},
          {"format_number(c0, c1)", 987654321.987654, 0, "987,654,322"},
          {"format_number(c0, c1)", 987654321.987654, -1, "987,654,322"},
          {"format_number(c0, c1)", -987321654.97853, 1, "-987,321,655.0"},
        });
  }

  @Test
  public void testFindInSet() {
    testFunctions(
        new Object[][] {
          {"find_in_set(c0, c1)", "A", "A,B,C", 1},
          {"find_in_set(c0, c1)", "", "A,B,C,", 4},
          {"find_in_set(c0, c1)", "", ",A,B,C,", 1},
          {"find_in_set(c0, c1)", "", "A,B,,C,", 3}
        });
  }

  @Test
  public void testCrc32() {
    testFunctions(
        new Object[][] {
          {"crc32(c0)", "ABC", 2743272264L},
          {"crc32(c0)", "Hello", 4157704578L},
          {"crc32(c0)", "hive", 3698179064L},
          {"crc32(c0)", "372189372123", 2607335846L},
          {"crc32(c0)", "", 0L},
        });
  }

  @Test
  public void testBinaryRepresentation() {
    // Bin Hive function - returns the binary representation of a specified integer or long
    testFunctions(
        new Object[][] {
          {"bin(c0)", 0, "0"},
          {"bin(c0)", 7, "111"},
          {"bin(c0)", 28550, "110111110000110"},
          {"bin(c0)", -28550, "11111111111111111001000001111010"},
          {
            "bin(c0)",
            Long.MAX_VALUE,
            "111111111111111111111111111111111111111111111111111111111111111"
          },
          {
            "bin(c0)",
            Long.MIN_VALUE,
            "1000000000000000000000000000000000000000000000000000000000000000"
          },
        });
  }

  @Test
  public void parseUrl() {
    testFunctions(
        new Object[][] {
          {"parse_url('http://dremio.com/path/p1.php?query=1', 'PROTOCOL')", "http"},
          {"parse_url('https://dremio.com/path/p1.php?query=1&k1=v1', 'PROTOCOL')", "https"},
          {"parse_url('ftp://dremio.com/path/p1.php?query=1&k1=v1', 'PROTOCOL')", "ftp"},
          {"parse_url('www.dremio.com/path/p1.php?query=1&k1=v1', 'PROTOCOL')", NULL_VARCHAR},
          {"parse_url('http://dremio.com/path/p1.php?query=1', 'HOST')", "dremio.com"},
          {"parse_url('http://127.0.0.0:8080/path/p1.php?query=1', 'HOST')", "127.0.0.0"},
          {"parse_url('www.dremio.com/path/p1.php?query=1&k1=v1', 'HOST')", NULL_VARCHAR},
          {"parse_url('$$$$$$$invalid url#######', 'HOST')", NULL_VARCHAR},
          {"parse_url('http://dremio.com/path/p1.php?query=1', 'AUTHORITY')", "dremio.com"},
          {"parse_url('http://127.0.0.0:8080/path/p1.php?query=1', 'AUTHORITY')", "127.0.0.0:8080"},
          {"parse_url('http://dremio.com/path/p1.php?query=1#ref', 'REF')", "ref"},
          {"parse_url('http://dremio.com/path/p1.php?query=1', 'REF')", NULL_VARCHAR},
          {"parse_url('http://dremio.com/path/p1.php?query=1', 'QUERY')", "query=1"},
          {"parse_url('http://dremio.com/path/p1.php?query=1', 'QUERY', 'query')", "1"},
          {"parse_url('http://dremio.com/path/p1.php', 'QUERY', 'query')", NULL_VARCHAR},
          {"parse_url('http://dremio.com/path/p1.php?query=1&k1=v1', 'QUERY', 'query')", "1"},
          {"parse_url('http://dremio.com/path/p1.php?query=1&k1=v1', 'QUERY', 'k1')", "v1"},
          {"parse_url('http://dremio.com/path/p1.php?query=1&k1=v1', 'QUERY', 'k3')", NULL_VARCHAR},
          {"parse_url('http://dremio.com/path/p1.php?query=1', 'FILE')", "/path/p1.php?query=1"},
          {"parse_url('http://dremio.com', 'FILE')", ""},
          {"parse_url('http://dremio.com/path/p1.php?query=1', 'PATH')", "/path/p1.php"},
          {"parse_url('http://dremio.com', 'PATH')", ""},
          {"parse_url('http://userinfo@dremio.com/path/p1.php?query=1', 'USERINFO')", "userinfo"},
          {"parse_url('http://dremio.com/path/p1.php?query=1', 'USERINFO')", NULL_VARCHAR},
          {"parse_url('http://dremio.com/path/p1.php?query=1', 'ERR')", NULL_VARCHAR},
          {"parse_url(c0, c1)", NULL_VARCHAR, NULL_VARCHAR, NULL_VARCHAR},
          {
            "parse_url(c0, c1)", "http://dremio.com/path/p1.php?query=1", NULL_VARCHAR, NULL_VARCHAR
          },
          {"parse_url(c0, c1)", NULL_VARCHAR, "USERINFO", NULL_VARCHAR},
          {"parse_url(c0, c1, c2)", NULL_VARCHAR, NULL_VARCHAR, NULL_VARCHAR, NULL_VARCHAR},
          {
            "parse_url(c0, c1, c2)",
            "http://dremio.com/path/p1.php?query=1",
            NULL_VARCHAR,
            NULL_VARCHAR,
            NULL_VARCHAR
          },
          {"parse_url(c0, c1, c2)", NULL_VARCHAR, "USERINFO", NULL_VARCHAR, NULL_VARCHAR},
          {"parse_url(c0, c1, c2)", NULL_VARCHAR, NULL_VARCHAR, "v1", NULL_VARCHAR},
        });
  }
}
