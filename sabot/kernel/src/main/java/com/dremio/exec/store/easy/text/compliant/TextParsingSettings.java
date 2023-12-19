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
package com.dremio.exec.store.easy.text.compliant;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;

import com.dremio.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import com.google.common.base.Preconditions;
import com.univocity.parsers.common.TextParsingException;

public class TextParsingSettings {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TextParsingSettings.class);

  public static final TextParsingSettings DEFAULT = new TextParsingSettings();

  private String emptyValue = null;
  private boolean parseUnescapedQuotes = true;
  private byte[] quote = {b('"')};
  private byte[] quoteEscape = {b('"')};
  private byte[] delimiter = {b(',')};
  private byte[] comment = {b('#')};

  private long maxCharsPerColumn = Character.MAX_VALUE;
  private byte normalizedNewLine = b('\n');
  private byte[] newLineDelimiter = {normalizedNewLine};
  private boolean ignoreLeadingWhitespaces = false;
  private boolean ignoreTrailingWhitespaces = false;
  private boolean skipEmptyLine = false;
  private boolean skipFirstLine = false;
  private boolean autoGenerateColumnNames = false;
  private boolean trimHeader = false;

  private boolean headerExtractionEnabled = false;
  private int skipLines = 0;
  private boolean useRepeatedVarChar = true;
  private int numberOfRecordsToRead = -1;


  public void set(TextFormatConfig config){
    this.quote = config.getQuote().getBytes(UTF_8);
    this.quoteEscape = config.getEscape().getBytes(UTF_8);
    this.newLineDelimiter = config.getLineDelimiter().getBytes(UTF_8);
    this.delimiter = config.getFieldDelimiter().getBytes(UTF_8);
    this.comment = config.getComment().getBytes(UTF_8);
    this.skipFirstLine = config.isSkipFirstLine();
    this.headerExtractionEnabled = config.isHeaderExtractionEnabled();
    this.skipLines = config.getSkipLines();
    this.autoGenerateColumnNames = config.isAutoGenerateColumnNames();
    this.trimHeader = config.isTrimHeaderEnabled();

    if (this.headerExtractionEnabled || this.autoGenerateColumnNames) {
      // In case of header TextRecordReader will use set of VarChar vectors vs RepeatedVarChar
      this.useRepeatedVarChar = false;
    }
  }

  public byte[] getComment(){
    return comment;
  }

  public boolean isSkipFirstLine() {
    return skipFirstLine;
  }

  public void setSkipFirstLine(boolean skipFirstLine) {
    this.skipFirstLine = skipFirstLine;
  }

  public boolean isSkipEmptyLine() {
    return skipEmptyLine;
  }

  public void setSkipEmptyLine(boolean skipEmptyLine) {
    this.skipEmptyLine = skipEmptyLine;
  }

  public boolean isUseRepeatedVarChar() {
    return useRepeatedVarChar;
  }

  public boolean isAutoGenerateColumnNames() {
    return autoGenerateColumnNames;
  }

  public void setUseRepeatedVarChar(boolean useRepeatedVarChar) {
    this.useRepeatedVarChar = useRepeatedVarChar;
  }

  private static byte b(char c){
    return (byte) c;
  }

  public byte[] getNewLineDelimiter() {
    return newLineDelimiter;
  }

  /**
   * Returns the string used for escaping values where the field delimiter is part of the value. Defaults to '"'
   * @return the quote string
   */
  public byte[] getQuote() {
    return quote;
  }

  /**
   * Defines the string used for escaping values where the field delimiter is part of the value. Defaults to '"'
   * @param quote the quote string
   */
  public void setQuote(byte[] quote) {
    this.quote = quote;
  }

  /**
   * Identifies whether or not a given String is used for escaping values where the field delimiter is part of the value
   * @param str the string to be verified
   * @return true if the given string is the string used for escaping values, false otherwise
   */
  public boolean isQuote(byte[] str) {
    return Arrays.equals(this.quote, str);
  }

  /**
   * Returns the string used for escaping quotes inside an already quoted value. Defaults to '"'
   * @return the quote escape string
   */
  public byte[] getQuoteEscape() {
    return quoteEscape;
  }

  /**
   * Defines the string used for escaping quotes inside an already quoted value. Defaults to '"'
   * @param quoteEscape the quote escape string
   */
  public void setQuoteEscape(byte[] quoteEscape) {
    this.quoteEscape = quoteEscape;
  }

  /**
   * Identifies whether or not a given String is used for escaping quotes inside an already quoted value.
   * @param str the String to be verified
   * @return true if the given String is the quote escape String, false otherwise
   */
  public boolean isQuoteEscape(byte[] str) {
    return Arrays.equals(this.quoteEscape,  str);
  }

  /**
   * Returns the field delimiter string. Defaults to ','
   * @return the field delimiter string
   */
  public byte[] getDelimiter() {
    return delimiter;
  }

  /**
   * Defines the field delimiter string. Defaults to ','
   * @param delimiter the field delimiter string
   */
  public void setDelimiter(byte[] delimiter) {
    this.delimiter = delimiter;
  }

  /**
   * Identifies whether or not a given string represents a field delimiter
   * @param str the string to be verified
   * @return true if the given string is the field delimiter string, false otherwise
   */
  public boolean isDelimiter(byte[] str) {
    return Arrays.equals(this.delimiter, str);
  }

  /**
   * Returns the String representation of an empty value (defaults to null)
   *
   * <p>When reading, if the parser does not read any character from the input, and the input is within quotes, the empty is used instead of an empty string
   *
   * @return the String representation of an empty value
   */
  public String getEmptyValue() {
    return emptyValue;
  }

  /**
   * Sets the String representation of an empty value (defaults to null)
   *
   * <p>When reading, if the parser does not read any character from the input, and the input is within quotes, the empty is used instead of an empty string
   *
   * @param emptyValue the String representation of an empty value
   */
  public void setEmptyValue(String emptyValue) {
    this.emptyValue = emptyValue;
  }


  /**
   * Indicates whether the CSV parser should accept unescaped quotes inside quoted values and parse them normally. Defaults to {@code true}.
   * @return a flag indicating whether or not the CSV parser should accept unescaped quotes inside quoted values.
   */
  public boolean isParseUnescapedQuotes() {
    return parseUnescapedQuotes;
  }

  /**
   * Configures how to handle unescaped quotes inside quoted values. If set to {@code true}, the parser will parse the quote normally as part of the value.
   * If set the {@code false}, a {@link TextParsingException} will be thrown. Defaults to {@code true}.
   * @param parseUnescapedQuotes indicates whether or not the CSV parser should accept unescaped quotes inside quoted values.
   */
  public void setParseUnescapedQuotes(boolean parseUnescapedQuotes) {
    this.parseUnescapedQuotes = parseUnescapedQuotes;
  }

  /**
   * Indicates whether or not the first valid record parsed from the input should be considered as the row containing the names of each column
   * @return true if the first valid record parsed from the input should be considered as the row containing the names of each column, false otherwise
   */
  public boolean isHeaderExtractionEnabled() {
    return headerExtractionEnabled;
  }

  /**
   * Defines whether or not the first valid record parsed from the input should be considered as the row containing the names of each column
   * @param headerExtractionEnabled a flag indicating whether the first valid record parsed from the input should be considered as the row containing the names of each column
   */
  public void setHeaderExtractionEnabled(boolean headerExtractionEnabled) {
    this.headerExtractionEnabled = headerExtractionEnabled;
  }

  /**
   * Number of lines to skip or ignore from the beginning of the file.
   * @return number of lines, a non-negative value
   */
  public int getSkipLines() {
    return skipLines;
  }

  /**
   * Defines the number of lines to be skipped at the beginning of a scanned file.
   * @param skipLines number of lines to skip, must be >= 0
   */
  public void setSkipLines(int skipLines) {
    Preconditions.checkArgument(skipLines >= 0,
      "number of lines to skip must be equal to or greater than 0");
    this.skipLines = skipLines;
  }

  /**
   * The number of valid records to be parsed before the process is stopped. A negative value indicates there's no limit (defaults to -1).
   * @return the number of records to read before stopping the parsing process.
   */
  public int getNumberOfRecordsToRead() {
    return numberOfRecordsToRead;
  }

  /**
   * Defines the number of valid records to be parsed before the process is stopped. A negative value indicates there's no limit (defaults to -1).
   * @param numberOfRecordsToRead the number of records to read before stopping the parsing process.
   */
  public void setNumberOfRecordsToRead(int numberOfRecordsToRead) {
    this.numberOfRecordsToRead = numberOfRecordsToRead;
  }

  public long getMaxCharsPerColumn() {
    return maxCharsPerColumn;
  }

  public void setMaxCharsPerColumn(long maxCharsPerColumn) {
    this.maxCharsPerColumn = maxCharsPerColumn;
  }

  public void setComment(byte[] comment) {
    this.comment = comment;
  }

  public byte getNormalizedNewLine() {
    return normalizedNewLine;
  }

  public void setNormalizedNewLine(byte normalizedNewLine) {
    this.normalizedNewLine = normalizedNewLine;
  }

  public boolean isIgnoreLeadingWhitespaces() {
    return ignoreLeadingWhitespaces;
  }

  public void setIgnoreLeadingWhitespaces(boolean ignoreLeadingWhitespaces) {
    this.ignoreLeadingWhitespaces = ignoreLeadingWhitespaces;
  }

  public boolean isIgnoreTrailingWhitespaces() {
    return ignoreTrailingWhitespaces;
  }

  public void setIgnoreTrailingWhitespaces(boolean ignoreTrailingWhitespaces) {
    this.ignoreTrailingWhitespaces = ignoreTrailingWhitespaces;
  }

  public boolean isTrimHeader() {
    return trimHeader;
  }

  public void setTrimHeader(boolean trimHeaders) {
    this.trimHeader = trimHeaders;
  }

}
