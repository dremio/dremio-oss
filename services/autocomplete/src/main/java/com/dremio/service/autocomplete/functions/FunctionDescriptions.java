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

/**
 * Utility class for getting a description for a function by name.
 */
public final class FunctionDescriptions {
  private static final ImmutableMap<String, String> nameToDescriptions = new ImmutableMap.Builder<String, String>()
    .put("ABS", "Computes the absolute value of a numeric expression.")
    .put("ACOS", "Computes the arcccosine (inverse cosine) of a value in radians")
    .put("APPROX_COUNT_DISTINCT", "Returns the approximate number of rows that contain distinct values in a column. Ignores rows that contain a null value for the column.")
    .put("ASCII", "Returns the ASCII code for the first character of a string. If the string is empty, 0 is returned.")
    .put("ASIN", "Computes the arcsine (inverse sine) of a value in radians")
    .put("ATAN", "Computes the Arctangent (inverse Tangent) of a value")
    .put("AVG", "Computes the average of a set of values.")
    .put("BINARY_STRING", "Converts the input expression to a binary value.")
    .put("BIT_LENGTH", "Gets length of bits of the input expression")
    .put("BOOL_AND", "Computes the boolean AND of two boolean expressions. Returns TRUE if both expressions evaluate to TRUE. Returns FALSE if one or both expression(s) evaluate(s) to FALSE.")
    .put("BOOL_OR", "Computes the boolean OR of two boolean expressions. Returns TRUE if one or both expressions evaluate to TRUE. Returns FALSE if both expressions evaluate to FALSE.")
    .put("BTRIM", "Trims leading and trailing characters from a string.")
    .put("CASE", "Evaluates a list of conditions and returns the first resulting true expression. If a true expression is not found, will return the `ELSE` statement, if present, or else will return `NULL`.")
    .put("CAST", "Converts a value of one data type to another data type. This function behaves similarly to the TO_<data_type> (i.e. TO_TIMESTAMP) functions.")
    .put("CBRT", "Computes the cube root of a numeric expression")
    .put("CEILING", "Returns the nearest equal or larger value of the input expression.  Can also be called using CEIL().")
    .put("CHAR_LENGTH", "Returns the character length of the input string")
    .put("CHARACTER_LENGTH", "Returns the length of an input string.")
    .put("CHR", "Converts a Unicode code point into the character that matches the input Unicode character. If an invalid code point is specified, an empty string is returned.")
    .put("COALESCE", "Evaluates the arguments in order and returns the value of the first expression that does not contain `NULL`.")
    .put("CONCAT", "Concatenates two or more strings")
    .put("CONCAT_WS", "Concatenate with separator. Returns a string resulting from the joining of two or more string values in an end-to-end manner. Uses the first argument as the separator between each string.")
    .put("CONTAINS", "Returns TRUE if the first expression contains the second expression.")
    .put("CONVERT_TIMEZONE", "Convert timestamp to the specified timezone")
    .put("CORR", "Calculates the covariance of the values expression1 and expression2. The function name must be enclosed in double quotes (\"CORR\").")
    .put("COS", "Computes the cosine of a value in radians")
    .put("COSH", "Computes the hyperbolic cosine of a value in radians.")
    .put("COT", "Computes the cotangent of a value in radians.")
    .put("COUNT", "Returns the total number of records for the specified expression.")
    .put("CURRENT_DATE", "Returns the current date of the system.")
    .put("CURRENT_DATE_UTC", "Returns the current date of the system based on the UTC timezone.")
    .put("CURRENT_SCHEMA", "Returns the path/schema in use by the current session.")
    .put("CURRENT_TIME", "Returns the current time for the system.")
    .put("CURRENT_TIMESTAMP", "Returns the current timestamp for the system.")
    .put("DATE_ADD", "Add (or subract) days from a date/timestamp value or column")
    .put("DATE_PART", "Return subfields such as year or hour from date or timestamp values")
    .put("DATE_SUB", "Subtracts the number of days from the specified date or timestamp.")
    .put("DATE_TRUNC", "Truncates the date or timestamp to the indicated precision.")
    .put("DEGREES", "Converts radians to degrees.")
    .put("E", "Returns Euler's number, a constant approximately equal to 2.718281828459045.")
    .put("EXP", "Calculates Euler's number, e, raised to the power of the specified value.")
    .put("EXTRACT", "Extracts the specified date or time part from the date or timestamp.")
    .put("FLOOR", "Returns the value from the specifed expression rounded to the nearest equal or smaller integer.")
    .put("FROM_HEX", "Returns a binary value for the given hexadecimal string")
    .put("HLL", "Uses HyperLogLog to return an approximation of the distint cardinality of the input.")
    .put("ILIKE", "Compares two strings and returns true if they match.")
    .put("INITCAP", "Returns the input string with the first letter of each word in uppercase and the subsequent letters in the word are in lowercase).")
    .put("IS NULL", "Determines if an expression is NULL. Returns true if <expression> is NULL, and false otherwise.  Alias for the operator IS NULL.")
    .put("IS_BIGINT", "Returns TRUE if the input expression is an big integer value.")
    .put("IS_DATE", "Returns TRUE if the input expression can be cast to a date.")
    .put("IS_INT", "Returns TRUE if the input expression is an integer value.")
    .put("IS_UTF8", "Returns whether an expression is valid UTF-8")
    .put("IS_VARCHAR", "Returns TRUE if the input expression is a varchar value.")
    .put("ISFALSE", "Returns TRUE if the input expression is FALSE.")
    .put("ISNULL", "Determines if an expression is NULL. Returns true if <expression> is NULL, and false otherwise.  Alias for the operator IS NULL.")
    .put("ISTRUE", "Returns TRUE if the input expression evaluates to TRUE.")
    .put("LAST_QUERY_ID", "Returns the ID for the most recently executed query in the current session.")
    .put("LEFT", "Returns the left-most substring. The function name must be enclosed in double quotes (\"LEFT\").")
    .put("LENGTH", "Returns the length of an input string. If the character encoding isn't specified, it assumes to UTF8.")
    .put("LOCALTIME", "Returns the current time for the system.")
    .put("LOCALTIMESTAMP", "Returns the current timestamp for the system.")
    .put("LOCATE", "Searches for the first occurrence of the first argument in the second argument and if found, returns the position the of the first argument in the second argument. The first character in a string is position 1. Returns 0 if the substring isn't found in the expression.")
    .put("LOG", "Returns the logarithm of the numeric input expression. If no base is specified, the natural log (ln) will be calculated.")
    .put("LOG10", "Returns the log base 10 of the numeric input expression.")
    .put("LOWER", "Returns the input expression with all the characters converted to lowercase.")
    .put("LPAD", "Left pads a string with spaces or specified characters to reach the number of chracters specified as a parameter.")
    .put("LSHIFT", "Shifts the bits of the numeric expression to the left.")
    .put("LTRIM", "Removes leading spaces or characters from a string.")
    .put("MAX", "Returns the maximum value among the non-NULL input expressions.")
    .put("MEDIAN", "Computes a percentile based on a continuous distribution of the column value")
    .put("MIN", "Returns the minimum value among the non-NULL input expressions.")
    .put("MOD", "Returns the remainder of the input expression divided by the second input expression.")
    .put("NDV", "Returns an approximate distinct value number, similar to `COUNT(DISTINCT col)`. NDV can return results faster than using the combination of COUNT and DISTINCT while using a constant amount of memory, resulting in less memory usage for columns with high cardinality.")
    .put("NOW", "Returns the current timestamp (date and time) in UTC timezone.")
    .put("NULLIF", "Compares two expressions. If the values in each expression are equal, returns `NULL` and, if they are not equal, returns the value of the first expression.")
    .put("OCTET_LENGTH", "Returns the length of the string in bytes.")
    .put("PERCENTILE_CONT", "Computes a percentile based on a continuous distribution of the column value")
    .put("PERCENTILE_DISC", "Computes a specific percentile for sorted values in a column")
    .put("PI", "Returns the value of pi, which is approximately 3.14592654.")
    .put("PIVOT", "Converts a set of data from rows into columns")
    .put("POSITION", "Returns the position of the first occurrence of a substring within another string")
    .put("POWER", "Returns the result of raising the input value to the specified power.")
    .put("QUERY_USER", "Returns the username of the user that is currently logged in to the system.")
    .put("RADIANS", "Convert a value in degrees to radians")
    .put("RANDOM", "Each call returns a random generated number between 0 and 1 for each row.")
    .put("REGEXP_SPLIT", "Splits an input string by using a regular expression according to a keyword and an integer value.")
    .put("REPEAT", "Builds a string by repeating the input for the specified number of times")
    .put("REPEATSTR", "Repeats the given string n times.")
    .put("REPLACE", "Removes all occurrences of a specified substring and replaces them with another string.")
    .put("REVERSE", "Reverses the order of characters in a string.")
    .put("RIGHT", "Returns the right-most substring. The function name must be enclosed in double quotes (\"RIGHT\").")
    .put("ROUND", "Returns the rounded value for the inputted value. If no scale is specified, the closest whole number is returned.")
    .put("RPAD", "Right pads a string with spaces or specified characters to reach the number of chracters specified as a parameter.")
    .put("RSHIFT", "Shifts the bits of the numeric expression to he right.")
    .put("RTRIM", "Removes trailing spaces or characters from a string.")
    .put("SESSION_USER", "Returns the username of the user that created the current session.")
    .put("SIGN", "Returns the sign of the input expression.")
    .put("SIN", "Computes the sine of a value.")
    .put("SINH", "Computes the hyperbolic sine of the input expression.")
    .put("SPLIT_PART", "Splits a given string at a specified character and returns the requested part.")
    .put("SQRT", "Returns the square root of the non-negative numeric expression.")
    .put("STRPOS", "Searches for the first occurence of the substring in the given expression and returns the position of where the substring begins. Searching binary values is also supported.")
    .put("SUBSTRING", "Returns the portion of the string from the specified base expression starting at the specified chracters.")
    .put("SUM", "Returns the sum of non-NULL input expressions.")
    .put("TAN", "Computes the tangent of a value in radians.")
    .put("TANH", "Computes the hyperbolic tangent of the input expression.")
    .put("TIMESTAMPADD", "Add (or subtract) an interval of time from a date/timestamp value or column")
    .put("TIMESTAMPDIFF", "Return the amount of time between two date or timestamp values")
    .put("TO_CHAR", "Converts the input expression to a character/string using the specified format.")
    .put("TO_HEX", "Returns a hexadecimal string for the given binary value.")
    .put("TO_NUMBER", "Converts a string into a number (double) in the specified format.")
    .put("TO_TIMESTAMP", "Converts the input expressions to the corresponding timestamp.")
    .put("TRANSACTION_TIMESTAMP", "Returns the timestamp in UTC of the current transaction.")
    .put("TRANSLATE", "Translates the base expression from the source characters/expression to the target characters/expression.")
    .put("TRIM", "Removes leading, trailing, or both spaces or characters from a string")
    .put("TRUNCATE", "Rounds the input expression down the nearest of equal integer depending on the specified number of places before or after the decimal point.")
    .put("TYPEOF", "Reports the type (in string format) of the input expression.")
    .put("UNIX_TIMESTAMP", "Returns the Unix timestamp for the timestamp parameter.")
    .put("UNPIVOT", "Converts a set of data from columns into rows")
    .put("UPPER", "Returns the input expression with all the characters converted to uppercase.")
    .put("USER", "Returns the user that is currently logged into the system.")
    .put("VAR_POP", "Returns the population variance of non-NULL records.")
    .put("VAR_SAMP", "Returns the sample variance of non-NULL records.")
    .put("XOR", "Returns the bitwise XOR of two integers.")
    .build();

  private FunctionDescriptions() {}

  public static Optional<String> tryGetDescription(String functionName) {
    String description = nameToDescriptions.get(functionName.toUpperCase());
    return Optional.ofNullable(description);
  }
}
