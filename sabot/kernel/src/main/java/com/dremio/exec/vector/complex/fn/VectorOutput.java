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
package com.dremio.exec.vector.complex.fn;

import java.io.IOException;

import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.DateMilliWriter;
import org.apache.arrow.vector.complex.writer.TimeMilliWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliWriter;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.DateMilliHolder;
import org.apache.arrow.vector.holders.TimeMilliHolder;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.expr.fn.impl.StringFunctionHelpers;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

abstract class VectorOutput {

  private static final Logger LOG = LoggerFactory.getLogger(VectorOutput.class);
  final VarBinaryHolder binary = new VarBinaryHolder();
  final TimeMilliHolder time = new TimeMilliHolder();
  final DateMilliHolder date = new DateMilliHolder();
  final TimeStampMilliHolder timestamp = new TimeStampMilliHolder();
  final BigIntHolder bigint = new BigIntHolder();
  final VarCharHolder varchar = new VarCharHolder();

  protected final WorkingBuffer work;
  protected JsonParser parser;

  private final boolean enforceValidJsonDateFormat;

  public VectorOutput(WorkingBuffer work, boolean enforceValidJsonDateFormat){
    this.work = work;
    this.enforceValidJsonDateFormat = enforceValidJsonDateFormat;
  }

  public void setParser(JsonParser parser){
    this.parser = parser;
  }

  protected boolean innerRun() throws IOException{
    JsonToken t = parser.nextToken();
    if(t != JsonToken.FIELD_NAME){
      return false;
    }

    String possibleTypeName = parser.getText();
    if(!possibleTypeName.isEmpty() && possibleTypeName.charAt(0) == '$'){
      switch(possibleTypeName){
      case ExtendedTypeName.BINARY:
        writeBinary(checkNextToken(JsonToken.VALUE_STRING));
        checkCurrentToken(JsonToken.END_OBJECT);
        return true;
      case ExtendedTypeName.TYPE:
        if(checkNextToken(JsonToken.VALUE_NUMBER_INT) || !hasBinary()) {
          throw UserException.parseError()
          .message("Either $type is not an integer or has no $binary")
          .build(LOG);
        }
        writeBinary(checkNextToken(JsonToken.VALUE_STRING));
        checkCurrentToken(JsonToken.END_OBJECT);
        return true;
      case ExtendedTypeName.DATE:
        writeDate(checkNextToken(JsonToken.VALUE_STRING));
        checkNextToken(JsonToken.END_OBJECT);
        return true;
      case ExtendedTypeName.TIME:
        writeTime(checkNextToken(JsonToken.VALUE_STRING));
        checkNextToken(JsonToken.END_OBJECT);
        return true;
      case ExtendedTypeName.TIMESTAMP:
        parser.nextToken();
        if (enforceValidJsonDateFormat) {
          parseExtendedTimestamp(parser);
        } else {
          writeTimestamp(checkCurrentToken(JsonToken.VALUE_STRING, JsonToken.VALUE_NUMBER_INT));
        }
        checkNextToken(JsonToken.END_OBJECT);
        return true;
      case ExtendedTypeName.INTERVAL:
        throw new UnsupportedOperationException("Interval type not supported");
      case ExtendedTypeName.INTEGER:
        writeInteger(checkNextToken(JsonToken.VALUE_STRING, JsonToken.VALUE_NUMBER_INT));
        checkNextToken(JsonToken.END_OBJECT);
        return true;
      case ExtendedTypeName.DECIMAL:
        throw new UnsupportedOperationException("Decimal type not supported");
      }
    }
    return false;
  }

  private void parseExtendedTimestamp(JsonParser parser) throws IOException {
    // We may have $date:{$numberLong: xxx}, in which case we still expect
    // VALUE_STRING token for the xxx, but first we need to skip over the
    // $numberLong token.
    if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
      parser.nextToken();
      String parserValue = parser.getValueAsString();
      if (!ExtendedTypeName.INTEGER.equals(parserValue)) {
        throw UserException.validationError()
          .message(String.format("Invalid token %s", parserValue))
          .build(LOG);
      }
      writeTimestamp(checkNextToken(JsonToken.VALUE_STRING));
      // If we had a nested token, there is an extra end object (that of the outer token) that we need to skip over.
      checkNextToken(JsonToken.END_OBJECT);
    } else if (StringUtils.isNumeric(parser.getValueAsString())) {
      // The fact that we have entered this method means ENFORCE_VALID_JSON_DATE_FORMAT_ENABLED flag is enabled.
      // In this case, we don't support the invalid format of a numerical value for $date which we supported before.
      throw UserException.unsupportedError()
        .message("Invalid format for " + ExtendedTypeName.TIMESTAMP + ". Must be wrapped by $numberLong or "
          + "follow any other valid ISO-8601 format.")
        .build(LOG);
    } else {
      // Handle non-nested format
      writeTimestamp(checkCurrentToken(JsonToken.VALUE_STRING));
    }
  }

  public boolean checkNextToken(final JsonToken expected) throws IOException{
    return checkNextToken(expected, expected);
  }

  public boolean checkCurrentToken(final JsonToken expected) throws IOException{
    return checkCurrentToken(expected, expected);
  }

  public boolean checkNextToken(final JsonToken expected1, final JsonToken expected2) throws IOException{
    return checkToken(parser.nextToken(), expected1, expected2);
  }

  public boolean checkCurrentToken(final JsonToken expected1, final JsonToken expected2) throws IOException{
    return checkToken(parser.getCurrentToken(), expected1, expected2);
  }

  boolean hasType() throws JsonParseException, IOException {
    JsonToken token = parser.nextToken();
    return token == JsonToken.FIELD_NAME && parser.getText().equals(ExtendedTypeName.TYPE);
  }

  boolean hasBinary() throws JsonParseException, IOException {
    JsonToken token = parser.nextToken();
    return token == JsonToken.FIELD_NAME && parser.getText().equals(ExtendedTypeName.BINARY);
  }

  long getType() throws JsonParseException, IOException {
    if (!checkNextToken(JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_STRING)) {
      long type = parser.getValueAsLong();
      //Advancing the token, as checking current token in binary
      parser.nextToken();
      return type;
    }
    throw new JsonParseException("Failure while reading $type value. Expected a NUMBER or STRING",
        parser.getCurrentLocation());
  }

  public boolean checkToken(final JsonToken t, final JsonToken expected1, final JsonToken expected2) throws IOException{
    if(t == JsonToken.VALUE_NULL){
      return true;
    }else if(t == expected1){
      return false;
    }else if(t == expected2){
      return false;
    }else{
      throw new JsonParseException(String.format("Failure while reading ExtendedJSON typed value. Expected a %s but "
          + "received a token of type %s", expected1, t), parser.getCurrentLocation());
    }
  }

  public abstract void writeBinary(boolean isNull) throws IOException;
  public abstract void writeDate(boolean isNull) throws IOException;
  public abstract void writeTime(boolean isNull) throws IOException;
  public abstract void writeTimestamp(boolean isNull) throws IOException;
  public abstract void writeInteger(boolean isNull) throws IOException;

  static class ListVectorOutput extends VectorOutput{
    private ListWriter writer;
    private boolean enforceValidJsonDateFormat;

    public ListVectorOutput(WorkingBuffer work, boolean enforceValidJsonDateFormat) {
      super(work, enforceValidJsonDateFormat);
      this.enforceValidJsonDateFormat = enforceValidJsonDateFormat;
    }

    public boolean run(ListWriter writer) throws IOException{
      this.writer = writer;
      return innerRun();
    }

    @Override
    public void writeBinary(boolean isNull) throws IOException {
      VarBinaryWriter bin = writer.varBinary();
      if(!isNull){
        byte[] binaryData = parser.getBinaryValue();
        if (hasType()) {
          //Ignoring type info as of now.
          long type = getType();
          if (type < 0 || type > 255) {
            throw UserException.validationError()
            .message("$type should be between 0 to 255")
            .build(LOG);
          }
        }
        work.prepareBinary(binaryData, binary);
        bin.write(binary);
      }
    }

    @Override
    public void writeDate(boolean isNull) throws IOException {
      DateMilliWriter dt = writer.dateMilli();
      if(!isNull){
        work.prepareVarCharHolder(parser.getValueAsString(), varchar);
        dt.writeDateMilli(StringFunctionHelpers.getDate(varchar.buffer, varchar.start, varchar.end));
      }
    }

    @Override
    public void writeTime(boolean isNull) throws IOException {
      TimeMilliWriter t = writer.timeMilli();
      if(!isNull){
        DateTimeFormatter f = ISODateTimeFormat.time();
        t.writeTimeMilli((int) com.dremio.common.util.DateTimes.toMillis(f.parseLocalDateTime(parser.getValueAsString())));
      }
    }

    @Override
    public void writeTimestamp(boolean isNull) throws IOException {
      TimeStampMilliWriter ts = writer.timeStampMilli();
      if(!isNull){
        switch (parser.getCurrentToken()) {
        case VALUE_NUMBER_INT:
          if (enforceValidJsonDateFormat) {
            throw UserException.unsupportedError()
              .message("Invalid format for " + ExtendedTypeName.TIMESTAMP + ". Must be wrapped by $numberLong or "
                + "follow any other valid ISO-8601 format.")
              .build(LOG);
          }
          LocalDateTime dt = new LocalDateTime(parser.getLongValue(), org.joda.time.DateTimeZone.UTC);
          ts.writeTimeStampMilli(com.dremio.common.util.DateTimes.toMillis(dt));
          break;
        case VALUE_STRING:
          DateTimeFormatter f = ISODateTimeFormat.dateTime();
          ts.writeTimeStampMilli(com.dremio.common.util.DateTimes.toMillis(LocalDateTime.parse(parser.getValueAsString(), f)));
          break;
        default:
          throw UserException.unsupportedError()
              .message(parser.getCurrentToken().toString())
              .build(LOG);
        }
      }
    }

    @Override
    public void writeInteger(boolean isNull) throws IOException {
      BigIntWriter intWriter = writer.bigInt();
      if(!isNull){
        intWriter.writeBigInt(Long.parseLong(parser.getValueAsString()));
      }
    }

  }

  static class MapVectorOutput extends VectorOutput {

    private StructWriter writer;
    private String fieldName;
    boolean enforceValidJsonDateFormat;

    public MapVectorOutput(WorkingBuffer work, boolean enforceValidJsonDateFormat) {
      super(work, enforceValidJsonDateFormat);
      this.enforceValidJsonDateFormat = enforceValidJsonDateFormat;
    }

    public boolean run(StructWriter writer, String fieldName) throws IOException{
      this.fieldName = fieldName;
      this.writer = writer;
      return innerRun();
    }

    @Override
    public void writeBinary(boolean isNull) throws IOException {
      VarBinaryWriter bin = writer.varBinary(fieldName);
      if(!isNull){
        byte[] binaryData = parser.getBinaryValue();
        if (hasType()) {
          //Ignoring type info as of now.
          long type = getType();
          if (type < 0 || type > 255) {
            throw UserException.validationError()
            .message("$type should be between 0 to 255")
            .build(LOG);
          }
        }
        work.prepareBinary(binaryData, binary);
        bin.write(binary);
      }
    }

    @Override
    public void writeDate(boolean isNull) throws IOException {
      DateMilliWriter dt = writer.dateMilli(fieldName);
      if(!isNull){
        DateTimeFormatter f = ISODateTimeFormat.date();
        LocalDateTime date = f.parseLocalDateTime(parser.getValueAsString());
        dt.writeDateMilli(com.dremio.common.util.DateTimes.toMillis(date));
      }
    }

    @Override
    public void writeTime(boolean isNull) throws IOException {
      TimeMilliWriter t = writer.timeMilli(fieldName);
      if(!isNull){
        DateTimeFormatter f = ISODateTimeFormat.time();
        t.writeTimeMilli((int) com.dremio.common.util.DateTimes.toMillis(f.parseLocalDateTime(parser.getValueAsString())));
      }
    }

    @Override
    public void writeTimestamp(boolean isNull) throws IOException {
      TimeStampMilliWriter ts = writer.timeStampMilli(fieldName);
      if(!isNull){
        switch (parser.getCurrentToken()) {
        case VALUE_NUMBER_INT:
          if (enforceValidJsonDateFormat) {
            throw UserException.unsupportedError()
              .message("Invalid format for " + ExtendedTypeName.TIMESTAMP + ". Must be wrapped by $numberLong or "
                + "follow any other valid ISO-8601 format.")
              .build(LOG);
          }
          LocalDateTime dt = new LocalDateTime(parser.getLongValue(), org.joda.time.DateTimeZone.UTC);
          ts.writeTimeStampMilli(com.dremio.common.util.DateTimes.toMillis(dt));
          break;
        case VALUE_STRING:
          String timestampLiteral = parser.getValueAsString();
          if (timestampLiteral.contains("W")) { // Joda Time does not natively support week number in short format (eg 2020W065), so we need to add hyphens ('-') around 'W'
            int weekIndex = timestampLiteral.indexOf("W"); // Index of the W char that marks week num
            if (timestampLiteral.charAt(weekIndex - 1) != '-') {
              timestampLiteral = timestampLiteral.substring(0, weekIndex) + '-' + timestampLiteral.substring(weekIndex, weekIndex + 3);
              weekIndex++; // Increment the index as we added a '-' character
              if (timestampLiteral.length() > weekIndex + 3) { // Check if the timestamp has a day component following week
                timestampLiteral += '-' + timestampLiteral.substring(weekIndex + 3); // Add a '-' between week num and day
              }
            }
          }
          timestampLiteral = timestampLiteral.replace(' ', 'T'); // Support formats like 2020-039 09 that have a space between date and time
          long millis = StringUtils.isNumeric(timestampLiteral) ? Long.parseLong(timestampLiteral) : new DateTime(timestampLiteral).getMillis();
          ts.writeTimeStampMilli(millis);
          break;
        default:
          throw UserException.unsupportedError()
          .message(parser.getCurrentToken().toString())
          .build(LOG);
        }
      }
    }

    @Override
    public void writeInteger(boolean isNull) throws IOException {
      BigIntWriter intWriter = writer.bigInt(fieldName);
      if(!isNull){
        intWriter.writeBigInt(Long.parseLong(parser.getValueAsString()));
      }
    }

  }

}
