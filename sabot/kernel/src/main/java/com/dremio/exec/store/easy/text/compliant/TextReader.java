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

import java.io.IOException;
import java.util.Arrays;
import java.util.OptionalInt;

import org.apache.arrow.memory.ArrowBuf;

import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.config.SimpleQueryContext;
import com.dremio.exec.physical.config.copyinto.CopyIntoErrorInfo;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties;
import com.dremio.exec.store.dfs.ErrorInfo;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.service.namespace.file.proto.FileType;
import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.csv.CsvParserSettings;

import io.netty.buffer.NettyArrowBuf;

/*******************************************************************************
 * Portions Copyright 2014 uniVocity Software Pty Ltd
 ******************************************************************************/

/**
 * A byte-based Text parser implementation. Builds heavily upon the uniVocity parsers. Customized for UTF8 parsing and
 * ArrowBuf support.
 */
final class TextReader implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TextReader.class);

  private final TextParsingContext context;

  private final long recordsToRead;
  private final TextParsingSettings settings;

  private final TextInput input;

  private final ArrowBuf workBuf;

  private boolean skipEmptyLine;

  // Records count i.e, comments are excluded and empty rows are excluded depending upon @skipEmptyLine
  private long recordCount = 0;

  private byte ch;

  /**
  * 0 -> ch is general byte
  * 1 -> line delimiter or normalized newLine is detected starting with ch
  * 2 -> field delimiter is detected starting with ch
  * */
  private byte chType;

  private boolean chIsLineDelimiter() {
    return (chType == 1);
  }

  private boolean chIsFieldDelimiter() {
    return (chType == 2);
  }

  private boolean chIsDelimiter() {
    return (chType == 1 || chType == 2);
  }

  /**
   * Wrapper class to encapsulate the TextOutput to improve readability and
   *    simplify the testing needed by the calling code
   *    (i.e. eliminate repeated testing canAppend )
   */
  static class OutputWrapper {
    /** 'canAppend' controls appending parsed content to output */
    private boolean canAppend = true;
    private final TextOutput output;

    public OutputWrapper(TextOutput output) { this.output = output; }
    public TextOutput output() { return this.output; }

    public boolean canAppend() { return canAppend; }
    public void setCanAppend(boolean append) { canAppend = append; }

    public void startBatch() { output.startBatch(); }
    public void finishBatch() { output.finishBatch(); }

    public void startField(int n) {
      if (canAppend) { output.startField(n); }
    }
    public void endField() {
      if(canAppend) { canAppend = output.endField(); }
    }
    public void endEmptyField() {
      if(canAppend) { canAppend = output.endEmptyField(); }
    }

    public boolean rowHasData(){ return output.rowHasData(); }
    public void finishRecord() { output.finishRecord(); }

    public void setFieldCurrentDataPointer(int cur) {
      if(canAppend) { output.setFieldCurrentDataPointer(cur);}
    }
    public int getFieldCurrentDataPointer() { return output.getFieldCurrentDataPointer() ; }

    public void append(byte parameter) {
      if(canAppend){ output.append(parameter); }
    }
    public void append(byte[] parameter) {
      if(canAppend){
        for (byte pByte : parameter) {
          output.append(pByte);
        }
      }
    }
    public void appendIgnoringWhitespace(byte cur) {
      if(canAppend) { output.appendIgnoringWhitespace(cur); }
    }
  }

  private final OutputWrapper output;

  /** Behavior settings **/
  private final boolean ignoreTrailingWhitespace;
  private final boolean ignoreLeadingWhitespace;
  private final boolean parseUnescapedQuotes;

  /** Temp buffer to save white spaces conditionally while parsing */
  private NettyArrowBuf tempWhiteSpaceBuff;

  /** savedWhitespaces controls whether tempWhiteSpaceBuff should be used to store the saved white spaces */
  private boolean savedWhitespaces = false;

  /** Key Parameters **/
  private final byte[] comment;
  private final byte[] fieldDelimiter;
  private final byte[] quote;
  private final byte[] quoteEscape;
  final byte[] lineDelimiter;
  private String filePath;
  private boolean schemaImposedMode;
  private CopyIntoQueryProperties copyIntoQueryProperties;
  private SimpleQueryContext queryContext;
  private int recordsRejectedCount;
  private final boolean isValidationMode;

  /**
   * The CsvParser supports all settings provided by {@link CsvParserSettings}, and requires this configuration to be
   * properly initialized.
   *
   * @param settings         the parser configuration
   * @param input            input stream
   * @param output           interface to produce output record batch
   * @param workBuf          working buffer to handle whitespaces
   * @param isValidationMode true if this is a copy_errors use case
   */
  public TextReader(TextParsingSettings settings, TextInput input, TextOutput output, ArrowBuf workBuf, boolean isValidationMode) {
    this.context = new TextParsingContext(input, output);
    this.workBuf = workBuf;
    this.settings = settings;

    this.recordsToRead = settings.getNumberOfRecordsToRead() == -1 ? Long.MAX_VALUE : settings.getNumberOfRecordsToRead();

    this.ignoreTrailingWhitespace = settings.isIgnoreTrailingWhitespaces();
    this.ignoreLeadingWhitespace = settings.isIgnoreLeadingWhitespaces();
    this.parseUnescapedQuotes = settings.isParseUnescapedQuotes();
    this.fieldDelimiter = settings.getDelimiter();
    this.quote = settings.getQuote();
    this.quoteEscape = settings.getQuoteEscape();
    this.comment = settings.getComment();
    this.schemaImposedMode = false;
    this.lineDelimiter = settings.getNewLineDelimiter();
    this.skipEmptyLine = settings.isSkipEmptyLine();
    this.input = input;
    this.output = new OutputWrapper(output);
    this.isValidationMode = isValidationMode;
  }

  public TextReader(TextParsingSettings settings, TextInput input, TextOutput output, ArrowBuf workBuf, String filePath,
                    boolean schemaImposedMode, CopyIntoQueryProperties copyIntoQueryProperties, SimpleQueryContext queryContext, boolean isValidationMode) {
    this(settings, input, output, workBuf, isValidationMode);
    this.filePath = filePath;
    this.schemaImposedMode = schemaImposedMode;
    this.copyIntoQueryProperties = copyIntoQueryProperties;
    this.queryContext = queryContext;
  }

  public TextOutput getOutput(){
    return this.output.output();
  }

  /* Check if the given byte is a white space. As per the univocity text reader
   * any ASCII <= ' ' is considered a white space. However, since byte in JAVA is signed
   * we have an additional check to make sure it's not negative
   */
  static boolean isWhite(byte b){
    return b <= ' ' && b > -1;
  }

  // Inform the output interface to indicate we are starting a new record batch
  public void resetForNextBatch(){
    output.startBatch();
  }

  public long getPos(){
    return input.getPos();
  }

  /**
   * Function encapsulates parsing an entire record, delegates parsing of the
   * fields to parseField() function.
   * We mark the start of the record and if there are any failures encountered (OOM for eg)
   * then we reset the input stream to the marked position
   * @throws IOException if End of Input stream is reached
   */
  private void parseRecord() throws IOException {
    // index of the field within this record
    int fieldIndex = 0;

    try{
      while ( !chIsLineDelimiter() ) {
        parseField(fieldIndex);
        fieldIndex++;
        if ( !chIsLineDelimiter() ) {
          parseNextChar();
          if ( chIsLineDelimiter() ) {
            output.startField(fieldIndex);
            output.endEmptyField();
          }
        }
      }
      // re-enable the output for the next record
      output.setCanAppend(true);
      recordCount++;

    } catch(StreamFinishedPseudoException e){
      // if we've written part of a field or all of a field, we should send this row.
      if(fieldIndex == 0 && !output.rowHasData()){
        throw e;
      }
    }

    output.finishRecord();
  }

  private void parseNextChar() throws IOException {
    byte[] byteNtype = input.nextChar();
    ch = byteNtype[1];
    chType = byteNtype[0];
  }

  /**
   * Function parses an individual field and skips Whitespaces if @ignoreTrailingWhitespace is true
   * by not appending it to the output vector
   * @throws IOException if End of Input stream is reached
   */
  private void parseValue() throws IOException {
    int continuousSpace = 0;
    try {
      while (!chIsDelimiter()) {
        if (ignoreTrailingWhitespace) {
          if (schemaImposedMode) {
            if (isWhite(ch)) {
              continuousSpace++;
            } else {
              continuousSpace = 0;
            }
            output.append(ch);
          } else {
            output.appendIgnoringWhitespace(ch);
          }
        } else {
          output.append(ch);
        }
        parseNextChar();
      }
    } finally {
      // in case parseNextChar fails with some exception or even StreamFinishedPseudoException
      //    we still want currentDataPointer to be set properly before exit.
      if(continuousSpace > 0){
        output.setFieldCurrentDataPointer(output.getFieldCurrentDataPointer() - continuousSpace);
      }
    }
  }

  /**
   * Function invoked when a quote is encountered. Function also
   * handles the unescaped quotes conditionally.
   * @throws IOException if End of Input stream is reached
   */
  private void parseQuotedValue() throws IOException {
    boolean isPrevQuoteEscape = false;
    boolean isPrevQuote = false;
    boolean quoteNescapeSame = Arrays.equals(quote, quoteEscape);
    boolean isQuoteMatched;
    while (true) {
      if (isPrevQuote) { // encountered quote previously
        if ( chIsDelimiter() ) { // encountered delimiter (line or field)
          break;
        }
        isQuoteMatched = input.match(ch, quote);
        if (quoteNescapeSame) { // quote and escape are same
          if (!isQuoteMatched) {
            if (isEndOfQuotedField()) {
              break;
            }
          } else {
            output.append(quote);
            parseNextChar();
          }
        } else {
          if (isQuoteMatched){
            // previous was a quote, ch is a quote
            //    and since "" is equivalent to \" in SQL, treat previous as escaped quote
            isPrevQuoteEscape = true;
          } else if (isEndOfQuotedField()) {
            break;
          }
        }
        isPrevQuote = false;
      }
      if ( chIsLineDelimiter() ) {
        if (isPrevQuoteEscape) {
          output.append(quoteEscape);
        }
        if (ch==-1) {
          output.append(lineDelimiter);
        } else {
          output.append(ch);
        }
        isPrevQuoteEscape = false;
        parseNextChar();
        continue;
      } else if ( chIsFieldDelimiter() ) {
        if (isPrevQuoteEscape) {
          output.append(quoteEscape);
        }
        output.append(fieldDelimiter);
        isPrevQuoteEscape = false;
        parseNextChar();
        continue;
      }
      isQuoteMatched = input.match(ch, quote);
      if (!isQuoteMatched) {
        if (!quoteNescapeSame) {
          if (isPrevQuoteEscape) {
            output.append(quoteEscape);
          }
          if (input.match(ch, quoteEscape)) {
            isPrevQuoteEscape = true;
          } else {
            isPrevQuoteEscape = false;
            output.append(ch);
          }
        } else {
          output.append(ch);
        }
      } else {
        if (!quoteNescapeSame) {
          if (!isPrevQuoteEscape) {
            isPrevQuote = true;
          } else {
            output.append(quote);
          }
          isPrevQuoteEscape = false;
        } else {
          isPrevQuote = true;
        }
      }
      try {
        parseNextChar();
      } catch (StreamFinishedPseudoException e) {
        if (isPrevQuote) {
          // (unescaped i.e. field terminating) quote symbol is the last char of the file
          return;
        } else {
          throw e;
        }
      }
    }
  }

  private boolean isEndOfQuotedField() throws IOException {
    if (isWhite(ch)) {
      // Handles whitespaces after quoted value:
      // Whitespaces are ignored (i.e., ch <= ' ') if they are not used as delimiters (i.e., ch != ' ')
      // For example, in tab-separated files (TSV files), '\t' is used as delimiter and should not be ignored
      parseWhiteSpaces(false);
      if ( chIsDelimiter() ) {
        savedWhitespaces = false;
        return true;
      }
    }
    if (!parseUnescapedQuotes) {
      throw new TextParsingException(
        context,
        String.format("Unescaped quote '%s' inside quoted value of CSV field. To allow unescaped quotes, set 'parseUnescapedQuotes' to 'true' in the CSV parser settings. Cannot parse CSV input.", Arrays.toString(quote)));
    }
    output.append(quote);
    if (savedWhitespaces) {
      for (int i = 0; i < tempWhiteSpaceBuff.writerIndex(); i++) {
        output.append(tempWhiteSpaceBuff.getByte(i));
      }
      savedWhitespaces = false;
    }
    return false;
  }

  /**
   * Captures the entirety of parsing a single field
   * @throws IOException if End of Input stream is reached
   */
  private void parseField(int fieldIndex) throws IOException {

    output.startField(fieldIndex);

    if (isWhite(ch)) {
      parseWhiteSpaces(ignoreLeadingWhitespace);
    }

    if ( chIsFieldDelimiter() ) {
      if (savedWhitespaces) {
        for (int i = 0; i < tempWhiteSpaceBuff.writerIndex(); i++) {
          output.append(tempWhiteSpaceBuff.getByte(i));
        }
        savedWhitespaces = false;
        output.endField();
      } else {
        output.endEmptyField();
      }
    } else {
      if (input.match(ch, quote)) {
        savedWhitespaces = false;
        long quoteStartLine = context.currentLine() + 1;
        parseNextChar();
        try {
          parseQuotedValue();
        } catch (Exception e) {
          // handling cases where a processing limit was reached: EOF or size limit exception
          if (e instanceof StreamFinishedPseudoException) {
            throw new UnmatchedQuoteException(quoteStartLine, context.currentLine());
          } else if (e.getCause() instanceof FieldSizeLimitExceptionHelper.FieldSizeLimitException) {
            throw new UnmatchedQuoteException(quoteStartLine, context.currentLine(), e);
          } else {
            throw e;
          }
        }
      } else {
        if (savedWhitespaces) {
          for (int i = 0; i < tempWhiteSpaceBuff.writerIndex(); i++) {
            output.append(tempWhiteSpaceBuff.getByte(i));
          }
          savedWhitespaces = false;
        }
        parseValue();
      }
      output.endField();
    }
  }

  /**
   * Helper function to skip white spaces occurring at the current input stream and save them to buffer conditionally.
   * @throws IOException if End of Input stream is reached
   */
  private void parseWhiteSpaces(boolean ignoreWhitespaces) throws IOException {

    // don't create buffers if code will not be able to output the cached bytes
    boolean bufferOn = output.canAppend();

    if (!chIsDelimiter())  {
      if(bufferOn) {
        tempWhiteSpaceBuff = NettyArrowBuf.unwrapBuffer(this.workBuf);
        tempWhiteSpaceBuff.resetWriterIndex();
        if (!ignoreWhitespaces){
          savedWhitespaces = true;
        }
      }
      while (!chIsDelimiter() && isWhite(ch)) {
        if (savedWhitespaces && bufferOn) {
          tempWhiteSpaceBuff.writeByte(ch);
        }
        parseNextChar();
      }
    }
  }

  /**
   * Starting point for the reader. Sets up the input interface.
   * @throws IOException if the record count is zero
   */
  public void start() throws IOException {
    context.stopped = false;
    if (input.start() || settings.isSkipFirstLine()) {
      if (settings.isHeaderExtractionEnabled()) {
        // *also* ignore up empty Lines when isSkipFirstLine is set
        skipEmptyLine = true;
      }
      // block output
      output.setCanAppend(false);
      parseNext();
      skipEmptyLine = settings.isSkipEmptyLine();  // reset in case modified above
      if (recordCount == 0) {
        // end of file most likely
        throw new IllegalArgumentException("Only one data line detected. Please consider changing line delimiter.");
      }
    } else if (settings.getSkipLines() > 0) {
      // skipLines can only be set for COPY INTO CSV, and is exclusive with skipFirstLine
      input.skipLines(settings.getSkipLines());
    }
  }


  /**
   * Parses the next record from the input. Will skip the line if it is a comment,
   * this is required when the file contains headers
   * @return the status of the reading
   * @throws IOException will rethrow some exceptions
   */
  public RecordReaderStatus parseNext() throws IOException {
    try {
      while (!context.stopped) {
        savedWhitespaces = false;
        parseNextChar();
        if (isWhite(ch)) {
          parseWhiteSpaces(ignoreLeadingWhitespace);
        }
        if (chIsLineDelimiter()) { // empty line
          if (skipEmptyLine) { // don't exit 'while' if allowed to skip
            continue;
          }
          break;
        } else if (chIsFieldDelimiter()) {
          break;
        } else if (input.match(ch, comment)) {
          // Comment lines (ex. those with leading '#') are not allowed to contain newlines
          //   ... so skipLines() is adequate to index past comment line
          // TODO[tp]: MARKER - we could attempt to hijack onto this when skipping header lines
          //   it's a bit tricky because parseNext() would only have to do this once, the first
          //   for each file
          //   we definitely need a test with multiple files to catch any errors in that logic
          input.skipLines(1);
          continue;
        }
        break;
      }
      parseRecord();

      if (recordsToRead > 0 && context.currentRecord() >= recordsToRead) {
        context.stop();
      }
      return RecordReaderStatus.SUCCESS;

    } catch (StreamFinishedPseudoException ex) {
      stopParsing();
      return writeError();
    } catch (Exception ex) {
      try {
        return handleOrRaiseException(ex);
      } finally {
        stopParsing();
      }
    }
  }

  private void stopParsing(){

  }

  private String displayLineSeparators(String str, boolean addNewLine) {
    if (addNewLine) {
      if (str.contains("\r\n")) {
        str = str.replaceAll("\\r\\n", "[\\\\r\\\\n]\r\n\t");
      } else if (str.contains("\n")) {
        str = str.replaceAll("\\n", "[\\\\n]\n\t");
      } else {
        str = str.replaceAll("\\r", "[\\\\r]\r\t");
      }
    } else {
      str = str.replaceAll("\\n", "\\\\n");
      str = str.replaceAll("\\r", "\\\\r");
    }
    return str;
  }

  /**
   * Helper method to handle exceptions caught while processing text files and generate better error messages associated with
   * the exception. In case the {@link TextReader#copyIntoQueryProperties} is set to {@link CopyIntoQueryProperties.OnErrorOption#CONTINUE}
   * the exception will be ignored.
   * @param ex  Exception raised
   * @return Exception replacement
   * @throws IOException Selectively augments exception error messages and rethrows
   */
  private RecordReaderStatus handleOrRaiseException(Exception ex) throws IOException {
    CopyIntoQueryProperties.OnErrorOption onErrorOption = null;
    if (copyIntoQueryProperties != null) {
      onErrorOption = copyIntoQueryProperties.getOnErrorOption();
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("Encountered error while reading text file. TextReader is running in '%s' mode.",
          copyIntoQueryProperties.getOnErrorOption()), ex);
      }
    }
    if (CopyIntoQueryProperties.OnErrorOption.CONTINUE == onErrorOption || isValidationMode) {
      if (isValidationMode) {
        // if error was seen during a copy_errors use case we write the details to the output
        SchemaImposedOutput schemaImposedOutput = (SchemaImposedOutput) output.output();

        // if error happened mid (unfinished with parsing) line we need to increment currentLine()
        long lineOfError = chIsLineDelimiter() ? context.currentLine() : context.currentLine() + 1;

        if (ex instanceof UnmatchedQuoteException) {
          lineOfError = ((UnmatchedQuoteException) ex).quoteStartLine;
        }

        schemaImposedOutput.writeValidationError(recordCount + recordsRejectedCount, lineOfError, collapseExceptionMessages(ex));
      }
      recordsRejectedCount++;
      //try to skip to the next line, if we are not at the end of the line or file
      if (chIsFieldDelimiter()) {
        try {
          input.skipLines(1);
        } catch (StreamFinishedPseudoException e) {
          // we are at the end of the file
          return RecordReaderStatus.SKIP;
        }
      }

      if (ex instanceof UnmatchedQuoteException) {
        // UnmatchedQuoteException is a structural error in CSV, need to stop processing this file
        input.close();
      }

      if (isValidationMode) {
        return RecordReaderStatus.VALIDATION_ERROR;
      } else {
        return RecordReaderStatus.SKIP;
      }
    }

    if (ex instanceof TextParsingException) {
      throw (TextParsingException) ex;
    }

    if (ex instanceof ArrayIndexOutOfBoundsException) {
      ex = UserException
          .dataReadError(ex)
          .message(
              "Dremio failed to read your text file.  Dremio supports up to %d columns in a text file.  Your file appears to have more than that.",
              RepeatedVarCharOutput.MAXIMUM_NUMBER_COLUMNS)
          .build(logger);
    }

    String message = ex.getMessage() + ", File :" + filePath;
    String tmp = input.getStringSinceMarkForError();
    char[] chars = tmp.toCharArray();
    if (chars != null) {
      int length = chars.length;
      if (length > settings.getMaxCharsPerColumn()) {
        message = "Length of parsed input (" + length
            + ") exceeds the maximum number of characters defined in your parser settings ("
            + settings.getMaxCharsPerColumn() + "). ";
      }

      if (tmp.contains("\n") || tmp.contains("\r")) {
        tmp = displayLineSeparators(tmp, true);
        String lineSeparator = displayLineSeparators(Arrays.toString(settings.getNewLineDelimiter()), false);
        message += "\nIdentified line separator characters in the parsed content. This may be the cause of the error. The line separator in your parser settings is set to '"
            + lineSeparator + "'. Parsed content:\n\t" + tmp;
      }

      int nullCharacterCount = 0;
      // ensuring the StringBuilder won't grow over Integer.MAX_VALUE to avoid OutOfMemoryError
      int maxLength = length > Integer.MAX_VALUE / 2 ? Integer.MAX_VALUE / 2 - 1 : length;
      StringBuilder s = new StringBuilder(maxLength);
      for (int i = 0; i < maxLength; i++) {
        if (chars[i] == '\0') {
          s.append('\\');
          s.append('0');
          nullCharacterCount++;
        } else {
          s.append(chars[i]);
        }
      }
      tmp = s.toString();

      if (nullCharacterCount > 0) {
        message += "\nIdentified "
            + nullCharacterCount
            + " null characters ('\0') on parsed content. This may indicate the data is corrupt or its encoding is invalid. Parsed content:\n\t"
            + tmp;
      }

    }

    throw new TextParsingException(context, message, ex);
  }

  /**
   * Prepare and write error metadata to the target table {@link ColumnUtils#COPY_INTO_ERROR_COLUMN_NAME} column.
   * The metadata definition is declared by {@link CopyIntoErrorInfo} and it is serialized to json format.
   */
  private RecordReaderStatus writeError() {
    if (schemaImposedMode && output.output() instanceof SchemaImposedOutput && recordsRejectedCount > 0) {
      SchemaImposedOutput schemaImposedOutput = (SchemaImposedOutput) output.output();
      OptionalInt errorColIndex = schemaImposedOutput.getErrorColIndex();
      if (errorColIndex.isPresent()) {
        output.startField(errorColIndex.getAsInt());
        long recordsLoadedCount = settings.isHeaderExtractionEnabled() ? recordCount - 1 : recordCount;
        String infoJson = ErrorInfo.Util.getJson(new CopyIntoErrorInfo.Builder(queryContext.getQueryId(),
          queryContext.getUserName(), queryContext.getTableNamespace(), copyIntoQueryProperties.getStorageLocation(), filePath,
          schemaImposedOutput.getExtendedFormatOptions(), FileType.CSV.name(),
          recordsLoadedCount == 0 ? CopyIntoErrorInfo.CopyIntoFileState.SKIPPED : CopyIntoErrorInfo.CopyIntoFileState.PARTIALLY_LOADED)
          .setRecordsLoadedCount(recordsLoadedCount)
          .setRecordsRejectedCount(recordsRejectedCount)
          .setRecordDelimiter(new String(lineDelimiter)).setFieldDelimiter(new String(fieldDelimiter))
          .setQuoteChar(new String(quote)).setEscapeChar(new String(quoteEscape)).build());
        output.append(infoJson.getBytes());
        schemaImposedOutput.endErrorField();
        output.setCanAppend(true);
        recordCount++;
        output.finishRecord();
        recordsRejectedCount = 0;
        return RecordReaderStatus.ERROR;
      }
    }
    return RecordReaderStatus.END;
  }

  /**
   * Finish the processing of a batch, indicates to the output
   * interface to wrap up the batch
   */
  public void finishBatch(){
    output.finishBatch();
    // System.out.println(String.format("line %d, cnt %d", input.getLineCount(), output.getRecordCount()));
  }

  /**
   * Invoked once there are no more records, and we are done with the
   * current record reader to clean up state.
   * @throws IOException nested exception
   */
  @Override
  public void close() throws IOException{
    input.close();
  }

  /**
   * Get the input
   * @return input
   */
  public TextInput getInput() {
    return input;
  }

  private String collapseExceptionMessages(Throwable throwable) {
    StringBuilder sb = new StringBuilder();

    while (throwable != null) {
      String message = throwable.getMessage();
      if (message != null) {
        sb.append(message).append(' ');
      }
      throwable = throwable.getCause();
    }

    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  public enum RecordReaderStatus {
    SUCCESS, SKIP, END, ERROR, VALIDATION_ERROR
  }

  class UnmatchedQuoteException extends IOException {

    private static final String ERROR_MESSAGE =
      "Malformed CSV file: expected closing quote symbol for a quoted value, started in line %d, but encountered %s" +
        " in line %d.%s";

    final long quoteStartLine;

    // EOF case
    UnmatchedQuoteException(long quoteStartLine, long lineOfException) {
      super(String.format(ERROR_MESSAGE, quoteStartLine, "EOF", lineOfException, ""));
      this.quoteStartLine = quoteStartLine;
    }

    // Size limit case
    UnmatchedQuoteException(long quoteStartLine, long lineOfException, Throwable cause) {
      super(String.format(ERROR_MESSAGE, quoteStartLine, "a size limit exception", lineOfException, " No further lines " +
            "will be processed from this file."), cause);
      this.quoteStartLine = quoteStartLine;
    }

  }
}
