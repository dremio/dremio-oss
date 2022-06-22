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
package com.dremio.service.autocomplete.functions;

import java.util.Optional;

import com.google.common.collect.ImmutableMap;

public final class FunctionSyntaxes {
  private static final ImmutableMap<String, String> nameToSyntax = new ImmutableMap.Builder<String, String>()
    .put("ONE_ARG_BOOLEAN_FUNCTION", "<BOOLEAN>")
    .put("ONE_ARG_NUMERIC_FUNCTION", "<NUMERIC>")
    .put("TWO_ARG_NUMERIC_FUNCTION", "<NUMERIC>, <NUMERIC>")
    .put("UNSTABLE_RETURN_TYPE_FUNCTION", "<ANY>")
    .put("VARADIC FUNCTION", "<STRING>, <STRING>, ... <STRING>")
    .put("ZERO_ARG_FUNCTION", "")
    .put("AVG", "[number] {x}")
    .put("COUNT", "DISTINCT [field name] {givenfield}")
    .put("MAX", "[number] {x}")
    .put("MIN", "[number] {x}")
    .put("SUM", "[number] {x}")
    .put("STDDEV", "[number] {x}")
    .put("STDDEV_POP", "[number] {x}")
    .put("VARIANCE", "[number] {x}")
    .put("VAR_POP", "[number] {x}")
    .put("CORR", "[number] {x}, [number] {y}")
    .put("CAST", "[any] {value} AS {<type>}")
    .put("CONVERT_TO", "{expression}, [literal string] {conv_type}")
    .put("CONVERT_FROM", "{expression}, [literal string] {conv_type}")
    .put("STRING_BINARY", "[binary] {data}")
    .put("BINARY_STRING", "[string] {giventext}")
    .put("TO_CHAR", "{expression}, [literal string] {format}")
    .put("TO_DATE", "[string] {giventext}, [literal string] {format}")
    .put("TO_TIME", "[string] {giventext}, [literal string] {format}")
    .put("TO_TIMESTAMP", "[string] {giventext}, [literal string] {format}")
    .put("TIMESTAMPADD", "{<time unit>}, [number] {count}, [timetype] {giventime}")
    .put("TIMESTAMPDIFF", "{<time unit>}, [timetype] {giventime1}, [timetype] {giventime2}")
    .put("TO_NUMBER", "[string] {giventext}, [literal string] {format}")
    .put("EXTRACT", "{<time unit>} FROM [timetype] {giventime}")
    .put("NOW", "")
    .put("TIMEOFDAY", "")
    .put("DATE_ADD", "DATE or TIME or TIMESTAMP [field name] {timetypefield}, [timeinterval] {giveninterval}")
    .put("DATE_SUB", "DATE or TIME or TIMESTAMP [field name] {timetypefield}, [timeinterval] {giveninterval}")
    .put("DATE_PART", "[literal string] {timeunit}, [timetype] {giventime}")
    .put("UNIX_TIMESTAMP", "[string] {timestring}, [string] {format}")
    .put("ISDATE", "[string] {giventext}")
    .put("DATE_TRUNC", "[literal string] {timeunit}, [timetype] {giventime}")
    .put("SIN", "[number] {x}")
    .put("COS", "[number] {x}")
    .put("TAN", "[number] {x}")
    .put("ASIN", "[number] {x}")
    .put("ACOS", "[number] {x}")
    .put("ATAN", "[number] {x}")
    .put("DEGREES", "[number] {x}")
    .put("RADIANS", "[number] {x}")
    .put("FLOOR", "[number] {x}")
    .put("CEILING", "[number] {x}")
    .put("ROUND", "[number] {x}")
    .put("SQRT", "[number] {x}")
    .put("CBRT", "[number] {x}")
    .put("LOG10", "[number] {x}")
    .put("LOG", "[number] {x}, [number] {y}")
    .put("POW", "[number] {x}, [number] {y}")
    .put("EXP", "[number] {x}")
    .put("E", "")
    .put("SINH", "[number] {x}")
    .put("COSH", "[number] {x}")
    .put("TANH", "[number] {x}")
    .put("MOD", "[number] {x}, [number] {y}")
    .put("LSHIFT", "[number] {x}, [number] {y}")
    .put("RSHIFT", "[number] {x}, [number] {y}")
    .put("SIGN", "[number] {x}")
    .put("ABS", "[number] {x}")
    .put("TRUNC", "[number] {x}, [optional number] {y}")
    .put("COT", "[number] {x}")
    .put("RAND", "")
    .put("RANDOM", "")
    .put("FLATTEN", "[JSON array] {givenarray}")
    .put("BIT_LENGTH", "[string] {giventext}")
    .put("TYPEOF", "[field name] {givenfield}")
    .put("ISNone", "{<expression>}")
    .put("ISNOTNone", "{<expression>}")
    .put("TRIM", "LEADING or TRAILING or BOTH [string] {trimtext} FROM [string] {basetext}")
    .put("LTRIM", "[string] {basetext}, [optional string] {trimtext}")
    .put("RTRIM", "[string] {basetext}, [optional string] {trimtext}")
    .put("BTRIM", "[string] {basetext}, [optional string] {trimtext}")
    .put("LPAD", "[string] {basetext}, [number] {x}, [optional string] {padtext}")
    .put("RPAD", "[string] {basetext}, [number] {x}, [optional string] {padtext}")
    .put("LOWER", "[string] {giventext}")
    .put("UPPER", "[string] {giventext}")
    .put("INITCAP", "[string] {giventext}")
    .put("POSITION", "[string] {sometext} IN [string] {giventext}")
    .put("STRPOS", "[string] {giventext}, [string] {sometext}")
    .put("BYTE_SUBSTR", "[string] {giventext}, [number] {x}, [number] {y}")
    .put("SUBSTR", "[string] {giventext}, [number] {x}, [optional number] {y}")
    .put("CHAR_LENGTH", "[string] {giventext}")
    .put("LENGTH", "[string] {giventext}, [optional literal string] {character_encoding")
    .put("ILIKE", "[string] {text1}, [string] {text2}")
    .put("REGEXP_REPLACE", "[string] {basetext}. [string] {matching}, [string] {{newtext}}")
    .put("CONCAT", "[string] {text1}, [optional string] {text2}, [optional string] {text3}, ...")
    .put("SPLIT_PART", "[string] {basetext}, [string] {splitter}, [number] {index}")
    .put("REGEXP_LIKE", "[string] {giventext}, [string literal] {matching}")
    .put("REGEXP_MATCHES", "[string] {giventext}, [string literal] {matching}")
    .put("REVERSE", "[string] {giventext}")
    .put("LEFT", "[string] {giventext}, [number] {x}")
    .put("RIGHT", "[string] {giventext}, [number] {x}")
    .put("REPLACE", "[string] {basetext}, [string] {text1}, [string] {text2}")
    .build();

  private FunctionSyntaxes() {}

  public static Optional<String> tryGetSyntax(String functionName) {
    String syntax = nameToSyntax.get(functionName.toUpperCase());
    return Optional.ofNullable(syntax);
  }
}
