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

import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.impl.PromotableWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.easy.EasyFormatUtils;
import com.dremio.exec.store.easy.json.reader.BaseJsonProcessor;
import com.dremio.exec.vector.complex.fn.VectorOutput.ListVectorOutput;
import com.dremio.exec.vector.complex.fn.VectorOutput.MapVectorOutput;
import com.dremio.sabot.exec.context.OperatorContext;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


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
  private OperatorContext context;

  private boolean schemaImposedMode;
  private ExtendedFormatOptions extendedFormatOptions;

  private BatchSchema targetSchema;

  public boolean validCopyIntoFile = false;
  private boolean trimSpace = false;

  private Map<BaseWriter.StructWriter,Map<String, Field>> structWriterToFieldMap = new HashMap<>();

  public JsonReader(ArrowBuf managedBuf, int maxFieldSize, int maxLeafLimit, boolean allTextMode, boolean skipOuterList, boolean readNumbersAsDouble) {
    this(managedBuf, GroupScan.ALL_COLUMNS, maxFieldSize, maxLeafLimit, allTextMode, skipOuterList, readNumbersAsDouble, false, null, null, null);
  }

  public JsonReader(ArrowBuf managedBuf, List<SchemaPath> columns, int maxFieldSize, int maxLeafLimit, boolean allTextMode,
                    boolean skipOuterList, boolean readNumbersAsDouble, boolean schemaImposedMode, ExtendedFormatOptions extendedFormatOptions, OperatorContext context, BatchSchema targetSchema) {
    assert Preconditions.checkNotNull(columns).size() > 0 : "JSON record reader requires at least one column";
    this.targetSchema = targetSchema;
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
    this.schemaImposedMode = schemaImposedMode;
    this.extendedFormatOptions = extendedFormatOptions;
    this.context = context;
    this.trimSpace = getTrimSpaceValue();
  }

  private Field getField(BaseWriter.StructWriter map, String fieldName) {
    Map<String, Field> stringFieldMap = structWriterToFieldMap.get(map);
    if (stringFieldMap == null) {
      stringFieldMap = new HashMap<>();
      List<Field> children = getChildren(map);
      for (Field field : children) {
        stringFieldMap.put(field.getName().toLowerCase(), field);
      }
      structWriterToFieldMap.put(map, stringFieldMap);
    }

    Field field = stringFieldMap.get(fieldName);
    if (field == null && !stringFieldMap.containsKey(fieldName)) {
      field = stringFieldMap.get(fieldName.toLowerCase());
      stringFieldMap.put(fieldName, field);
    }

    return field;
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
    setSource(data.getBytes(UTF_8));
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
      if (schemaImposedMode) {
        if (!validCopyIntoFile) {
          throw new TransformationException(String.format("No column name matches target %s", targetSchema), parser.getCurrentLocation().getLineNr());
        }
        validCopyIntoFile = false;
      }
      break;
    default:
      throw
        getExceptionWithContext(
          UserException.dataReadError(), currentFieldName)
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
          UserException.dataReadError(), currentFieldName)
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
            UserException.dataReadError(), currentFieldName)
          .message("The top level of your document must either be a single array of maps or a set "
            + "of white space delimited maps.")
          .build(logger);
      }

      if(skipOuterList){
        t = parser.nextToken();
        if(t == JsonToken.START_OBJECT){
          inOuterList = true;
          writeDataSwitch(writer.rootAsStruct());
        } else if(t == VALUE_NULL && schemaImposedMode) {
            inOuterList = true;
            addNullValueForStruct(writer);
            break;
          } else {
          throw
            getExceptionWithContext(
              UserException.dataReadError(), currentFieldName)
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
            UserException.dataReadError(), currentFieldName)
          .message("Failure while parsing JSON.  Ran across unexpected %s.", JsonToken.END_ARRAY)
          .build(logger);
      }
    case NOT_AVAILABLE:
      return ReadState.END_OF_STREAM;
    case VALUE_NULL:
      if(schemaImposedMode && inOuterList) {
        addNullValueForStruct(writer);
        break;
      }
    default:
      throw
        getExceptionWithContext(
          UserException.dataReadError(), currentFieldName)
          .message("Failure while parsing JSON.  Found token of [%s]. Root object cannot be a scalar.", t)
          .build(logger);
    }

    return ReadState.WRITE_SUCCEED;

  }

  private void addNullValueForStruct(ComplexWriter writer) {
    BaseWriter.StructWriter structWriter = writer.rootAsStruct();
    validCopyIntoFile = true;
    structWriter.start();
    structWriter.end();
  }

  private void writeDataSwitch(BaseWriter.StructWriter w) throws IOException {
    if (this.allTextMode || this.schemaImposedMode) {
      writeStructDataAllText(w, this.selection, true, true);
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
                          UserException.dataReadError(), currentFieldName)
                          .message("Unexpected token %s", parser.getCurrentToken())
                          .build(logger);
        }
      }
    } finally {
      map.end();
    }
  }

  private void writeStructDataAllText(BaseWriter.StructWriter map, FieldSelection selection, boolean moveForward, boolean rootStruct) throws IOException {
    //
    map.start();
    try {
      Set<String> fieldNamesSet = schemaImposedMode? new HashSet<>() : null;
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
        Boolean skipObject = skipObject(fieldName, selection, map);
        FieldSelection childSelection = selection.getChild(fieldName);

        if (skipObject) {
          consumeEntireNextValue();
          continue outside;
        }

        JsonToken token = parser.nextToken();

        if(schemaImposedMode) {
          if (rootStruct) {
              validCopyIntoFile = true;
          }
          Field originalField = getField(map, fieldName);
          String originalName = originalField.getName();

          if(!fieldNamesSet.contains(originalName)) {
            fieldNamesSet.add(originalName);
          } else {
            throw new TransformationException(String.format("Duplicate column name %s.", fieldName), parser.getCurrentLocation().getLineNr());
          }

          if(token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT){
            ArrowType type = originalField.getType();
            checkForComplexCoercions(type, token, fieldName);
          }
        }

        switch (token) {
        case START_ARRAY:
          writeListDataAllText(map.list(fieldName), childSelection);
          break;
        case START_OBJECT:
          if (!writeMapDataIfTyped(map, fieldName)) {
            writeStructDataAllText(map.struct(fieldName), childSelection, false, false);
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
          if (schemaImposedMode) {
            handleStringToType(parser, map, fieldName);
          } else {
            handleString(parser, map, fieldName);
          }
          break;
        case VALUE_NULL:
          // do nothing as we don't have a type.
          break;

        default:
          throw
            getExceptionWithContext(
              UserException.dataReadError(), currentFieldName)
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

  private void handleStringToType(JsonParser parser, BaseWriter.StructWriter writer, String fieldName) throws IOException {
    try {
      incrementLeafCount();
      String varcharValue = parser.getText();
      Field leafChild = getField(writer, fieldName);
      ArrowType leafChildType = leafChild.getType();
      writeValue(fieldName, leafChildType, writer, varcharValue);
    } catch (Exception e) {
      throw new TransformationException(e.getMessage(), parser.getCurrentLocation().getLineNr());
    }
  }

  private void writeValue(String fieldName, ArrowType type, BaseWriter.StructWriter writer, String varcharValue) throws IOException {
    varcharValue = EasyFormatUtils.applyStringTransformations(varcharValue, extendedFormatOptions, trimSpace);
    if(varcharValue == null) {
      writeNullToStruct(fieldName, type, writer);
    } else {
      try {
        if (CompleteType.BIT.getType().equals(type)) {
          Integer val = EasyFormatUtils.JsonBooleanFunction.apply(varcharValue);
          writer.bit(fieldName).writeBit(val);
        } else if (CompleteType.INT.getType().equals(type)) {
          Integer val = Integer.valueOf(varcharValue);
          writer.integer(fieldName).writeInt(val);
        } else if (CompleteType.BIGINT.getType().equals(type)) {
          Long val = Long.valueOf(varcharValue);
          writer.bigInt(fieldName).writeBigInt(val);
        } else if (CompleteType.FLOAT.getType().equals(type)) {
          Float val = Float.valueOf(varcharValue);
          writer.float4(fieldName).writeFloat4(val);
        } else if (CompleteType.DOUBLE.getType().equals(type)) {
          Double val = Double.valueOf(varcharValue);
          writer.float8(fieldName).writeFloat8(val);
        } else if (ArrowType.ArrowTypeID.Decimal.equals(type.getTypeID())) {
          BigDecimal val = EasyFormatUtils.getBigDecimalValue(type, varcharValue);
          writer.decimal(fieldName, ((ArrowType.Decimal) type).getScale(), ((ArrowType.Decimal) type).getPrecision())
            .writeDecimal(val);
        } else if (CompleteType.VARCHAR.getType().equals(type)) {
          writeString(varcharValue, writer, fieldName);
        } else if (CompleteType.DATE.getType().equals(type)) {
          final Long dateMilliValue = EasyFormatUtils.getDateTimeValueAsUnixTimestamp(varcharValue, type, extendedFormatOptions);
          writer.dateMilli(fieldName).writeDateMilli(dateMilliValue);
        } else if (CompleteType.TIME.getType().equals(type)) {
          final Long timeMilliValue = EasyFormatUtils.getDateTimeValueAsUnixTimestamp(varcharValue, type, extendedFormatOptions);
          writer.timeMilli(fieldName).writeTimeMilli(timeMilliValue.intValue());
        } else if (CompleteType.TIMESTAMP.getType().equals(type)) {
          Long timeStampMilliValue = EasyFormatUtils.getDateTimeValueAsUnixTimestamp(varcharValue, type, extendedFormatOptions);
          writer.timeStampMilli(fieldName).writeTimeStampMilli(timeStampMilliValue);
        } else {
          throw new RuntimeException("Unsupported data type : " + type);
        }
      } catch (IllegalArgumentException e) {
        throw EasyFormatUtils.handleExceptionDuringCoercion(varcharValue, type, fieldName, e);
      }
    }
  }

  private void handleStringToType(JsonParser parser, BaseWriter.ListWriter writer) throws IOException {
    try {
      incrementLeafCount();
      ArrowType type = ((PromotableWriter) writer.list()).getField().getType();
      if (parser.currentToken() == VALUE_NULL) {
        writeNullToList(type, writer);
      } else {
        String varcharValue = parser.getText();
        writeValue(type, writer, varcharValue);
      }
    } catch (Exception e) {
      throw new TransformationException(e.getMessage(), parser.getCurrentLocation().getLineNr());
    }
  }

  private void writeValue(ArrowType type, BaseWriter.ListWriter writer, String varcharValue) throws IOException {
    varcharValue = EasyFormatUtils.applyStringTransformations(varcharValue, extendedFormatOptions, trimSpace);
    if(varcharValue == null) {
      writeNullToList(type, writer);
    } else {
      try {
        if (CompleteType.BIT.getType().equals(type)) {
          Integer val = EasyFormatUtils.JsonBooleanFunction.apply(varcharValue);
          writer.bit().writeBit(val);
        } else if (CompleteType.INT.getType().equals(type)) {
          Integer val = Integer.valueOf(varcharValue);
          writer.integer().writeInt(val);
        } else if (CompleteType.BIGINT.getType().equals(type)) {
          Long val = Long.valueOf(varcharValue);
          writer.bigInt().writeBigInt(val);
        } else if (CompleteType.FLOAT.getType().equals(type)) {
          Float val = Float.valueOf(varcharValue);
          writer.float4().writeFloat4(val);
        } else if (CompleteType.DOUBLE.getType().equals(type)) {
          Double val = Double.valueOf(varcharValue);
          writer.float8().writeFloat8(val);
        } else if (ArrowType.ArrowTypeID.Decimal.equals(type.getTypeID())) {
          BigDecimal val = EasyFormatUtils.getBigDecimalValue(type, varcharValue);
          writer.decimal().writeDecimal(val);
        } else if (CompleteType.VARCHAR.getType().equals(type)) {
          writeString(varcharValue, writer);
        } else if (CompleteType.DATE.getType().equals(type)) {
          final Long dateMilliValue = EasyFormatUtils.getDateTimeValueAsUnixTimestamp(varcharValue, type, extendedFormatOptions);
          writer.dateMilli().writeDateMilli(dateMilliValue);
        } else if (CompleteType.TIME.getType().equals(type)) {
          final Long timeMilliValue = EasyFormatUtils.getDateTimeValueAsUnixTimestamp(varcharValue, type, extendedFormatOptions);
          writer.timeMilli().writeTimeMilli(timeMilliValue.intValue());
        } else if (CompleteType.TIMESTAMP.getType().equals(type)) {
          Long timestampMilliValue = EasyFormatUtils.getDateTimeValueAsUnixTimestamp(varcharValue, type, extendedFormatOptions);
          writer.timeStampMilli().writeTimeStampMilli(timestampMilliValue);
        } else {
          throw new RuntimeException("Unsupported data type : " + type);
        }
      } catch (IllegalArgumentException e) {
        // Pass 'fieldName' as empty here since we don't have that information while dealing with lists.
        throw EasyFormatUtils.handleExceptionDuringCoercion(varcharValue, type, "", e);
      }
    }
  }

  private boolean getTrimSpaceValue() {
    return (extendedFormatOptions != null && extendedFormatOptions.getTrimSpace() != null) ? extendedFormatOptions.getTrimSpace() : false;
  }

  private void writeNullToStruct(String fieldName, ArrowType type, BaseWriter.StructWriter writer) {
    if (CompleteType.BIT.getType().equals(type)) {
      writer.bit(fieldName).writeNull();
    } else if (CompleteType.INT.getType().equals(type)) {
      writer.integer(fieldName).writeNull();
    } else if (CompleteType.BIGINT.getType().equals(type)) {
      writer.bigInt(fieldName).writeNull();
    } else if (CompleteType.FLOAT.getType().equals(type)) {
      writer.float4(fieldName).writeNull();
    } else if (CompleteType.DOUBLE.getType().equals(type)) {
      writer.float8(fieldName).writeNull();
    } else if (ArrowType.ArrowTypeID.Decimal.equals(type.getTypeID())) {
      writer.decimal(fieldName, ((ArrowType.Decimal)type).getScale(), ((ArrowType.Decimal)type).getPrecision())
        .writeNull();
    } else if (CompleteType.VARCHAR.getType().equals(type)) {
//      writeString(varcharValue, writer, fieldName);
      writer.varChar(fieldName).writeNull();
    } else if (CompleteType.DATE.getType().equals(type)) {
      writer.dateMilli(fieldName).writeNull();
    } else if (CompleteType.TIME.getType().equals(type)) {
      writer.timeMilli(fieldName).writeNull();
    } else if (CompleteType.TIMESTAMP.getType().equals(type)) {
      writer.timeStampMilli(fieldName).writeNull();
    } else {
      throw new RuntimeException("Unsupported data type : " + type);
    }
  }

  private void writeNullToList(ArrowType type, BaseWriter.ListWriter writer) {
    if (CompleteType.BIT.getType().equals(type)) {
      writer.bit().writeNull();
    } else if (CompleteType.INT.getType().equals(type)) {
      writer.integer().writeNull();
    } else if (CompleteType.BIGINT.getType().equals(type)) {
      writer.bigInt().writeNull();
    } else if (CompleteType.FLOAT.getType().equals(type)) {
      writer.float4().writeNull();
    } else if (CompleteType.DOUBLE.getType().equals(type)) {
      writer.float8().writeNull();
    } else if (ArrowType.ArrowTypeID.Decimal.equals(type.getTypeID())) {
      writer.decimal().writeNull();
    } else if (CompleteType.VARCHAR.getType().equals(type)) {
      writer.varChar().writeNull();
    } else if (CompleteType.DATE.getType().equals(type)) {
      writer.dateMilli().writeNull();
    } else if (CompleteType.TIME.getType().equals(type)) {
      writer.timeMilli().writeNull();
    } else if (CompleteType.TIMESTAMP.getType().equals(type)) {
      writer.timeStampMilli().writeNull();
    } else if(CompleteType.STRUCT.getType().equals(type)) {
      writer.struct().writeNull();
    } else if(CompleteType.LIST.getType().equals(type)) {
      writer.list().writeNull();
    } else {
      throw new RuntimeException("Unsupported data type : " + type);
    }
  }


  private void handleString(JsonParser parser, BaseWriter.StructWriter writer, String fieldName) throws IOException {
    incrementLeafCount();
    writeString(parser.getText(), writer, fieldName);
  }
  private void writeString(String val, BaseWriter.StructWriter writer, String fieldName) throws IOException {
    final int size = workingBuffer.prepareVarCharHolder(val);
    FieldSizeLimitExceptionHelper.checkSizeLimit(size, maxFieldSize, currentFieldName, logger);
    writer.varChar(fieldName).writeVarChar(0, size, workingBuffer.getBuf());
  }

  private void handleString(JsonParser parser, ListWriter writer) throws IOException {
    incrementLeafCount();
    writeString(parser.getText(), writer);
  }

  private void writeString(String val, BaseWriter.ListWriter writer) throws IOException {
    final int size = workingBuffer.prepareVarCharHolder(val);
    FieldSizeLimitExceptionHelper.checkSizeLimit(size, maxFieldSize, currentFieldName, logger);
    writer.varChar().writeVarChar(0, size, workingBuffer.getBuf());
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
        throw getExceptionWithContext(e, this.currentFieldName).build(logger);
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

      JsonToken token = parser.nextToken();
      if(schemaImposedMode && (token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT)) {
        ArrowType type = getFieldType(list);
        checkForComplexCoercions(type, token, "");
      }

      switch (token) {
      case START_ARRAY:
        writeListDataAllText(list.list(), selection);
        break;
      case START_OBJECT:
        if (!writeListDataIfTyped(list)) {
          writeStructDataAllText(list.struct(), selection, false, false);
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
        if (schemaImposedMode) {
          handleStringToType(parser, list);
        } else {
          handleString(parser, list);
        }
        break;
      default:
        throw
          getExceptionWithContext(
            UserException.dataReadError(), currentFieldName)
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

  private boolean skipObject(String fieldName, FieldSelection selection, BaseWriter.StructWriter map) throws TransformationException {
    boolean neverValid = selection.getChild(fieldName).isNeverValid();

    if (schemaImposedMode) {
      Field currentField = getField(map, fieldName);
      if(!Objects.isNull(currentField) && CompleteType.MAP.getType().equals(currentField.getType())) {
        throw new TransformationException(String.format("'COPY INTO Command' does not support MAP Type. Found Map type field : '%s'. ", fieldName), parser.getCurrentLocation().getLineNr());
      }
      return currentField == null || neverValid;
    } else {
      return neverValid;
    }
  }

  private List<Field> getChildren(BaseWriter.StructWriter map) {
    if(CompleteType.LIST.getType().equals(map.getField().getType())) {
      return map.getField().getChildren().get(0).getChildren();
    } else {
      return map.getField().getChildren();
    }
  }

  private ArrowType getFieldType(BaseWriter.ListWriter writer){
    try {
      return ((PromotableWriter) writer).getField().getChildren().get(0).getType();
    }catch(Exception e){
      throw new RuntimeException("Unable to get field type.",e);
    }
  }

  private void checkForComplexCoercions(ArrowType type, JsonToken token, String fieldName) throws TransformationException{
    switch(token){
    case START_ARRAY:
      if(!ArrowType.ArrowTypeID.List.equals(type.getTypeID())){
        throw new TransformationException(String.format("Field %s having List datatype in the file cannot be coerced into %s datatype of the target table.", fieldName, type), parser.getCurrentLocation().getLineNr());
      }
      break;
    case START_OBJECT:
      if(!ArrowType.ArrowTypeID.Struct.equals(type.getTypeID())){
        throw new TransformationException(String.format("Field %s having Struct datatype in the file cannot be coerced into %s datatype of the target table.", fieldName, type), parser.getCurrentLocation().getLineNr());
      }
      break;
      default:
    }
  }
}
