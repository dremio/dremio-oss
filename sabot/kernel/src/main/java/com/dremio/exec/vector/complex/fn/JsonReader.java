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
import java.io.InputStream;
import java.util.BitSet;
import java.util.List;

import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;

import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.store.easy.json.reader.BaseJsonProcessor;
import com.dremio.exec.vector.complex.fn.VectorOutput.ListVectorOutput;
import com.dremio.exec.vector.complex.fn.VectorOutput.MapVectorOutput;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.netty.buffer.ArrowBuf;


public class JsonReader extends BaseJsonProcessor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonReader.class);

  private final WorkingBuffer workingBuffer;
  private final List<SchemaPath> columns;
  private final boolean allTextMode;
  private final MapVectorOutput mapOutput;
  private final ListVectorOutput listOutput;
  private final boolean extended = true;
  private final boolean readNumbersAsDouble;
  private final int maxFieldSize;
  private final int maxLeafLimit;
  private int currentLeafCount;

  private long dataSizeReadSoFar;

  /**
   * Describes whether or not this reader can unwrap a single root array record and treat it like a set of distinct records.
   */
  private final boolean skipOuterList;

  /**
   * Whether the reader is currently in a situation where we are unwrapping an outer list.
   */
  private boolean inOuterList;
  /**
   * The name of the current field being parsed. For Error messages.
   */
  private String currentFieldName;

  private FieldSelection selection;

  public JsonReader(ArrowBuf managedBuf, int maxFieldSize, int maxLeafLimit, boolean allTextMode, boolean skipOuterList, boolean readNumbersAsDouble) {
    this(managedBuf, GroupScan.ALL_COLUMNS, maxFieldSize, maxLeafLimit, allTextMode, skipOuterList, readNumbersAsDouble);
  }

  public JsonReader(ArrowBuf managedBuf, List<SchemaPath> columns, int maxFieldSize, int maxLeafLimit, boolean allTextMode,
                    boolean skipOuterList, boolean readNumbersAsDouble) {
    assert Preconditions.checkNotNull(columns).size() > 0 : "JSON record reader requires at least one column";
    this.selection = FieldSelection.getFieldSelection(columns);
    this.workingBuffer = new WorkingBuffer(managedBuf);
    this.skipOuterList = skipOuterList;
    this.allTextMode = allTextMode;
    this.columns = columns;
    this.mapOutput = new MapVectorOutput(workingBuffer);
    this.listOutput = new ListVectorOutput(workingBuffer);
    this.currentFieldName="<none>";
    this.readNumbersAsDouble = readNumbersAsDouble;
    this.maxFieldSize = maxFieldSize;
    this.maxLeafLimit = maxLeafLimit;
    this.dataSizeReadSoFar = 0;
    this.currentLeafCount = 0;
  }

  @Override
  public void resetDataSizeCounter() {
    dataSizeReadSoFar = 0;
  }

  @Override
  public long getDataSizeCounter() {
    return dataSizeReadSoFar;
  }

  @Override
  public void ensureAtLeastOneField(ComplexWriter writer) {
    List<BaseWriter.StructWriter> writerList = Lists.newArrayList();
    List<PathSegment> fieldPathList = Lists.newArrayList();
    BitSet emptyStatus = new BitSet(columns.size());

    if (writer.rootAsStruct().getField() != null && !writer.rootAsStruct().isEmptyStruct()) {
      return;
    }

    // first pass: collect which fields are empty
    for (int i = 0; i < columns.size(); i++) {
      SchemaPath sp = columns.get(i);
      PathSegment fieldPath = sp.getRootSegment();
      BaseWriter.StructWriter fieldWriter = writer.rootAsStruct();
      while (fieldPath.getChild() != null && ! fieldPath.getChild().isArray()) {
        fieldWriter = fieldWriter.struct(fieldPath.getNameSegment().getPath());
        fieldPath = fieldPath.getChild();
      }
      writerList.add(fieldWriter);
      fieldPathList.add(fieldPath);
      if (fieldWriter.isEmptyStruct()) {
        emptyStatus.set(i, true);
      }
      if (i == 0 && !allTextMode) {
        // when allTextMode is false, there is not much benefit to producing all the empty
        // fields; just produce 1 field.  The reason is that the type of the fields is
        // unknown, so if we produce multiple Integer fields by default, a subsequent batch
        // that contains non-integer fields will error out in any case.  Whereas, with
        // allTextMode true, we are sure that all fields are going to be treated as varchar,
        // so it makes sense to produce all the fields, and in fact is necessary in order to
        // avoid schema change exceptions by downstream operators.
        break;
      }

    }

    // second pass: create default typed vectors corresponding to empty fields
    // Note: this is not easily do-able in 1 pass because the same fieldWriter may be
    // shared by multiple fields whereas we want to keep track of all fields independently,
    // so we rely on the emptyStatus.
    for (int j = 0; j < fieldPathList.size(); j++) {
      BaseWriter.StructWriter fieldWriter = writerList.get(j);
      PathSegment fieldPath = fieldPathList.get(j);
      if (emptyStatus.get(j)) {
        if (allTextMode) {
          fieldWriter.varChar(fieldPath.getNameSegment().getPath());
        } else {
          fieldWriter.integer(fieldPath.getNameSegment().getPath());
        }
      }
    }
  }

  public void setSource(int start, int end, ArrowBuf buf) throws IOException {
    setSource(ArrowBufInputStream.getStream(start, end, buf));
  }


  @Override
  public void setSource(InputStream is) throws IOException {
    super.setSource(is);
    mapOutput.setParser(parser);
    listOutput.setParser(parser);
  }

  @Override
  public void setSource(JsonNode node) {
    super.setSource(node);
    mapOutput.setParser(parser);
    listOutput.setParser(parser);
  }

  public void setSource(String data) throws IOException {
    setSource(data.getBytes(Charsets.UTF_8));
  }

  public void setSource(byte[] bytes) throws IOException {
    setSource(new SeekableBAIS(bytes));
  }

  @Override
  public ReadState write(ComplexWriter writer) throws IOException {
    JsonToken t = parser.nextToken();

    while (!parser.hasCurrentToken() && !parser.isClosed()) {
      t = parser.nextToken();
    }

    if (parser.isClosed()) {
      return ReadState.END_OF_STREAM;
    }

    this.currentLeafCount = 0;
    ReadState readState = writeToVector(writer, t);

    switch (readState) {
    case END_OF_STREAM:
      break;
    case WRITE_SUCCEED:
      break;
    default:
      throw
        getExceptionWithContext(
          UserException.dataReadError(), currentFieldName, null)
          .message("Failure while reading JSON. (Got an invalid read state %s )", readState.toString())
          .build(logger);
    }

    return readState;
  }

  private void confirmLast() throws IOException{
    parser.nextToken();
    if(!parser.isClosed()){
      throw
        getExceptionWithContext(
          UserException.dataReadError(), currentFieldName, null)
        .message("Dremio attempted to unwrap a toplevel list "
          + "in your document.  However, it appears that there is trailing content after this top level list.  Dremio only "
          + "supports querying a set of distinct maps or a single json array with multiple inner maps.")
        .build(logger);
    }
  }

  private ReadState writeToVector(ComplexWriter writer, JsonToken t) throws IOException {
    switch (t) {
    case START_OBJECT:
      writeDataSwitch(writer.rootAsStruct());
      break;
    case START_ARRAY:
      if(inOuterList){
        throw
          getExceptionWithContext(
            UserException.dataReadError(), currentFieldName, null)
          .message("The top level of your document must either be a single array of maps or a set "
            + "of white space delimited maps.")
          .build(logger);
      }

      if(skipOuterList){
        t = parser.nextToken();
        if(t == JsonToken.START_OBJECT){
          inOuterList = true;
          writeDataSwitch(writer.rootAsStruct());
        }else{
          throw
            getExceptionWithContext(
              UserException.dataReadError(), currentFieldName, null)
            .message("The top level of your document must either be a single array of maps or a set "
              + "of white space delimited maps.")
            .build(logger);
        }

      }else{
        writeDataSwitch(writer.rootAsList());
      }
      break;
    case END_ARRAY:

      if(inOuterList){
        confirmLast();
        return ReadState.END_OF_STREAM;
      }else{
        throw
          getExceptionWithContext(
            UserException.dataReadError(), currentFieldName, null)
          .message("Failure while parsing JSON.  Ran across unexpected %s.", JsonToken.END_ARRAY)
          .build(logger);
      }

    case NOT_AVAILABLE:
      return ReadState.END_OF_STREAM;
    default:
      throw
        getExceptionWithContext(
          UserException.dataReadError(), currentFieldName, null)
          .message("Failure while parsing JSON.  Found token of [%s].  Dremio currently only supports parsing "
              + "json strings that contain either lists or maps.  The root object cannot be a scalar.", t)
          .build(logger);
    }

    return ReadState.WRITE_SUCCEED;

  }

  private void writeDataSwitch(BaseWriter.StructWriter w) throws IOException {
    if (this.allTextMode) {
      writeStructDataAllText(w, this.selection, true);
    } else {
      writeStructData(w, this.selection, true);
    }
  }

  private void writeDataSwitch(ListWriter w) throws IOException {
    if (this.allTextMode) {
      writeListDataAllText(w, this.selection);
    } else {
      writeListData(w, this.selection);
    }
  }

  private void consumeEntireNextValue() throws IOException {
    switch (parser.nextToken()) {
    case START_ARRAY:
    case START_OBJECT:
      parser.skipChildren();
      return;
    default:
      // hit a single value, do nothing as the token was already read
      // in the switch statement
      return;
    }
  }

  /**
   *
   * @param map
   * @param selection
   * @param moveForward
   *          Whether or not we should start with using the current token or the next token. If moveForward = true, we
   *          should start with the next token and ignore the current one.
   * @throws IOException
   */
  private void writeStructData(BaseWriter.StructWriter map, FieldSelection selection, boolean moveForward) throws IOException {
    //
    map.start();
    try {
      outside:
      while (true) {

        JsonToken t;
        if (moveForward) {
          t = parser.nextToken();
        } else {
          t = parser.getCurrentToken();
          moveForward = true;
        }

        if (t == JsonToken.NOT_AVAILABLE || t == JsonToken.END_OBJECT) {
          return;
        }

        assert t == JsonToken.FIELD_NAME : String.format("Expected FIELD_NAME but got %s.", t.name());

        final String fieldName = parser.getText();
        this.currentFieldName = fieldName;
        FieldSelection childSelection = selection.getChild(fieldName);
        if (childSelection.isNeverValid()) {
          consumeEntireNextValue();
          continue outside;
        }

        switch (parser.nextToken()) {
        case START_ARRAY:
          writeListData(map.list(fieldName), childSelection);
          break;
        case START_OBJECT:
          if (!writeMapDataIfTyped(map, fieldName)) {
            writeStructData(map.struct(fieldName), childSelection, false);
          }
          break;
        case END_OBJECT:
          break outside;

        case VALUE_FALSE: {
          incrementLeafCount();
          map.bit(fieldName).writeBit(0);
          break;
        }
        case VALUE_TRUE: {
          incrementLeafCount();
          map.bit(fieldName).writeBit(1);
          break;
        }
        case VALUE_NULL:
          // do nothing as we don't have a type.
          break;
        case VALUE_NUMBER_FLOAT: {
          incrementLeafCount();
          map.float8(fieldName).writeFloat8(parser.getDoubleValue());
          break;
        }
        case VALUE_NUMBER_INT: {
          incrementLeafCount();
          if (this.readNumbersAsDouble) {
            map.float8(fieldName).writeFloat8(parser.getDoubleValue());
          } else {
            map.bigInt(fieldName).writeBigInt(parser.getLongValue());
          }
          break;
        }
        case VALUE_STRING: {
          handleString(parser, map, fieldName);
          break;
        }

        default:
          throw
                  getExceptionWithContext(
                          UserException.dataReadError(), currentFieldName, null)
                          .message("Unexpected token %s", parser.getCurrentToken())
                          .build(logger);
        }
      }
    } finally {
      map.end();
    }
  }

  private void writeStructDataAllText(BaseWriter.StructWriter map, FieldSelection selection, boolean moveForward) throws IOException {
    //
    map.start();
    try {
      outside:
      while (true) {
        JsonToken t;

        if (moveForward) {
          t = parser.nextToken();
        } else {
          t = parser.getCurrentToken();
          moveForward = true;
        }

        if (t == JsonToken.NOT_AVAILABLE || t == JsonToken.END_OBJECT) {
          return;
        }

        assert t == JsonToken.FIELD_NAME : String.format("Expected FIELD_NAME but got %s.", t.name());

        final String fieldName = parser.getText();
        this.currentFieldName = fieldName;
        FieldSelection childSelection = selection.getChild(fieldName);
        if (childSelection.isNeverValid()) {
          consumeEntireNextValue();
          continue outside;
        }

        switch (parser.nextToken()) {
        case START_ARRAY:
          writeListDataAllText(map.list(fieldName), childSelection);
          break;
        case START_OBJECT:
          if (!writeMapDataIfTyped(map, fieldName)) {
            writeStructDataAllText(map.struct(fieldName), childSelection, false);
          }
          break;
        case END_OBJECT:
          break outside;

        case VALUE_EMBEDDED_OBJECT:
        case VALUE_FALSE:
        case VALUE_TRUE:
        case VALUE_NUMBER_FLOAT:
        case VALUE_NUMBER_INT:
        case VALUE_STRING:
          handleString(parser, map, fieldName);
          break;
        case VALUE_NULL:
          // do nothing as we don't have a type.
          break;

        default:
          throw
            getExceptionWithContext(
              UserException.dataReadError(), currentFieldName, null)
              .message("Unexpected token %s", parser.getCurrentToken())
              .build(logger);
        }
      }
    } finally {
      map.end();
    }
  }

  /**
   * Will attempt to take the current value and consume it as an extended value (if extended mode is enabled).  Whether extended is enable or disabled, will consume the next token in the stream.
   * @param writer
   * @param fieldName
   * @return
   * @throws IOException
   */
  private boolean writeMapDataIfTyped(BaseWriter.StructWriter writer, String fieldName) throws IOException {
    if (extended) {
      return mapOutput.run(writer, fieldName);
    } else {
      parser.nextToken();
      return false;
    }
  }

  /**
   * Will attempt to take the current value and consume it as an extended value (if extended mode is enabled).  Whether extended is enable or disabled, will consume the next token in the stream.
   * @param writer
   * @return
   * @throws IOException
   */
  private boolean writeListDataIfTyped(ListWriter writer) throws IOException {
    if (extended) {
      return listOutput.run(writer);
    } else {
      parser.nextToken();
      return false;
    }
  }

  private void handleString(JsonParser parser, BaseWriter.StructWriter writer, String fieldName) throws IOException {
    incrementLeafCount();
    final int size = workingBuffer.prepareVarCharHolder(parser.getText());
    FieldSizeLimitExceptionHelper.checkSizeLimit(size, maxFieldSize, currentFieldName, logger);
    writer.varChar(fieldName).writeVarChar(0, size, workingBuffer.getBuf());
    dataSizeReadSoFar += size;
  }

  private void handleString(JsonParser parser, ListWriter writer) throws IOException {
    incrementLeafCount();
    final int size = workingBuffer.prepareVarCharHolder(parser.getText());
    FieldSizeLimitExceptionHelper.checkSizeLimit(size, maxFieldSize, currentFieldName, logger);
    writer.varChar().writeVarChar(0, size, workingBuffer.getBuf());
    dataSizeReadSoFar += size;
  }

  private void writeListData(ListWriter list, FieldSelection selection) throws IOException {
    list.startList();
    final int originalLeafCount = currentLeafCount;
    int maxArrayLeafCount = 0;
    outside: while (true) {
      currentLeafCount = originalLeafCount;
      try {
        switch (parser.nextToken()) {
        case START_ARRAY:
          writeListData(list.list(), selection);
          break;
        case START_OBJECT:
          if (!writeListDataIfTyped(list)) {
            writeStructData(list.struct(), selection, false);
          }
          break;
        case END_ARRAY:
        case END_OBJECT:
          break outside;

        case VALUE_EMBEDDED_OBJECT:
        case VALUE_FALSE: {
          incrementLeafCount();
          list.bit().writeBit(0);
          // dataSizeReadSoFar += 1; - not counting as it takes 1-bit per value
          break;
        }
        case VALUE_TRUE: {
          incrementLeafCount();
          list.bit().writeBit(1);
          // dataSizeReadSoFar += 1; - not counting as it takes 1-bit per value
          break;
        }
        case VALUE_NULL:
          throw UserException.unsupportedError()
            .message("Null values are not supported in lists by default. " +
              "Please set `store.json.all_text_mode` to true to read lists containing nulls. " +
              "Be advised that this will treat JSON null values as a string containing the word 'null'.")
            .build(logger);
        case VALUE_NUMBER_FLOAT: {
          incrementLeafCount();
          list.float8().writeFloat8(parser.getDoubleValue());
          dataSizeReadSoFar += 8;
          break;
        }
        case VALUE_NUMBER_INT: {
          incrementLeafCount();
          dataSizeReadSoFar += 8;
          if (this.readNumbersAsDouble) {
            list.float8().writeFloat8(parser.getDoubleValue());
          } else {
            list.bigInt().writeBigInt(parser.getLongValue());
          }
          break;
        }
        case VALUE_STRING:
          handleString(parser, list);
          break;
        default:
          throw UserException.dataReadError()
            .message("Unexpected token %s", parser.getCurrentToken())
            .build(logger);
        }

        // Take the maximum number of leaves from the current and the calculated for this array entry.
        maxArrayLeafCount = Math.max(maxArrayLeafCount, currentLeafCount);
      } catch (Exception e) {
        throw getExceptionWithContext(e, this.currentFieldName, null).build(logger);
      }
    }

    // Take the maximum calculated leaf count from the array.
    currentLeafCount = maxArrayLeafCount;
    list.endList();
  }

  private void writeListDataAllText(ListWriter list, FieldSelection selection) throws IOException {
    list.startList();
    final int originalLeafCount = currentLeafCount;
    int maxArrayLeafCount = 0;
    outside: while (true) {
      currentLeafCount = originalLeafCount;

      switch (parser.nextToken()) {
      case START_ARRAY:
        writeListDataAllText(list.list(), selection);
        break;
      case START_OBJECT:
        if (!writeListDataIfTyped(list)) {
          writeStructDataAllText(list.struct(), selection, false);
        }
        break;
      case END_ARRAY:
      case END_OBJECT:
        break outside;

      case VALUE_EMBEDDED_OBJECT:
      case VALUE_FALSE:
      case VALUE_TRUE:
      case VALUE_NULL:
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
      case VALUE_STRING:
        handleString(parser, list);
        break;
      default:
        throw
          getExceptionWithContext(
            UserException.dataReadError(), currentFieldName, null)
          .message("Unexpected token %s", parser.getCurrentToken())
          .build(logger);
      }

      // Take the maximum number of leaves from the current and the calculated for this array entry.
      maxArrayLeafCount = Math.max(maxArrayLeafCount, currentLeafCount);
    }

    // Take the maximum calculated leaf count from the array.
    currentLeafCount = maxArrayLeafCount;
    list.endList();
  }

  /**
   * Increment the current leaf count and throw ColumnCountTooLargeException if the max limit is exceeded.
   */
  private void incrementLeafCount() {
    if (++currentLeafCount > maxLeafLimit) {
      throw new ColumnCountTooLargeException(maxLeafLimit);
    }
  }
}
