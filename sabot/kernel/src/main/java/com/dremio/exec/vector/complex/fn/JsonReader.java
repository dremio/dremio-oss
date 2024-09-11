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

import static com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties.OnErrorOption.CONTINUE;
import static com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties.OnErrorOption.SKIP_FILE;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.PathSegment.PathSegmentType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.SimpleQueryContext;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo.Builder;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo.CopyIntoFileState;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties;
import com.dremio.exec.physical.config.copyinto.IngestionProperties;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.FileLoadInfo;
import com.dremio.exec.store.dfs.copyinto.CopyIntoExceptionUtils;
import com.dremio.exec.store.easy.EasyFormatUtils;
import com.dremio.exec.store.easy.json.reader.BaseJsonProcessor;
import com.dremio.exec.tablefunctions.copyerrors.ValidationErrorRowWriter;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.vector.complex.fn.VectorOutput.ListVectorOutput;
import com.dremio.exec.vector.complex.fn.VectorOutput.MapVectorOutput;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.file.proto.FileType;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.impl.PromotableWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public class JsonReader extends BaseJsonProcessor {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(JsonReader.class);

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
   * Describes whether or not this reader can unwrap a single root array record and treat it like a
   * set of distinct records.
   */
  private final boolean skipOuterList;

  /** Whether the reader is currently in a situation where we are unwrapping an outer list. */
  private boolean inOuterList;

  /** The name of the current field being parsed. For Error messages. */
  private String currentFieldName;

  private final FieldSelection selection;
  private final OperatorContext context;
  private final boolean schemaImposedMode;
  private final ExtendedFormatOptions extendedFormatOptions;
  private final CopyIntoQueryProperties copyIntoQueryProperties;
  private final SimpleQueryContext queryContext;
  private final IngestionProperties ingestionProperties;
  private final BatchSchema targetSchema;
  private boolean validCopyIntoFile = false;
  private final boolean trimSpace;
  private final String filePath;
  private long recordsLoadedCount;
  private boolean resetWriterPosition = true;
  private final boolean isValidationMode;
  private final ValidationErrorRowWriter validationErrorWriter;
  private boolean hasErrors = false;
  private final long processingStartTime;
  private final long fileSize;
  private FieldWrapper rootFieldWrapper;

  public JsonReader(
      ArrowBuf managedBuf,
      int maxFieldSize,
      int maxLeafLimit,
      boolean allTextMode,
      boolean skipOuterList,
      boolean readNumbersAsDouble,
      boolean enforceValidJsonDateFormat) {
    this(
        managedBuf,
        GroupScan.ALL_COLUMNS,
        maxFieldSize,
        maxLeafLimit,
        allTextMode,
        skipOuterList,
        readNumbersAsDouble,
        false,
        null,
        null,
        null,
        null,
        null,
        null,
        enforceValidJsonDateFormat,
        null,
        0L,
        false,
        null,
        0L);
  }

  public JsonReader(
      ArrowBuf managedBuf,
      List<SchemaPath> columns,
      int maxFieldSize,
      int maxLeafLimit,
      boolean allTextMode,
      boolean skipOuterList,
      boolean readNumbersAsDouble,
      boolean schemaImposedMode,
      ExtendedFormatOptions extendedFormatOptions,
      CopyIntoQueryProperties copyIntoQueryProperties,
      SimpleQueryContext queryContext,
      IngestionProperties ingestionProperties,
      OperatorContext context,
      BatchSchema targetSchema,
      boolean enforceValidJsonDateFormat,
      String filePath,
      long fileSize,
      boolean isValidationMode,
      ValidationErrorRowWriter validationErrorWriter,
      long processingStartTime) {
    assert Preconditions.checkNotNull(columns).size() > 0
        : "JSON record reader requires at least one column";
    this.targetSchema = targetSchema;
    if (isValidationMode) {
      columns =
          targetSchema.getFields().stream()
              .map(Field::getName)
              .map(SchemaPath::getSimplePath)
              .collect(Collectors.toList());
      this.rootFieldWrapper = new FieldWrapper(targetSchema.getFields());
    } else {
      this.rootFieldWrapper = FieldWrapper.EMPTY;
    }
    this.selection = FieldSelection.getFieldSelection(columns);
    this.workingBuffer = new WorkingBuffer(managedBuf);
    this.skipOuterList = skipOuterList;
    this.allTextMode = allTextMode;
    this.columns = columns;
    this.mapOutput = new MapVectorOutput(workingBuffer, enforceValidJsonDateFormat);
    this.listOutput = new ListVectorOutput(workingBuffer, enforceValidJsonDateFormat);
    this.currentFieldName = "<none>";
    this.readNumbersAsDouble = readNumbersAsDouble;
    this.maxFieldSize = maxFieldSize;
    this.maxLeafLimit = maxLeafLimit;
    this.dataSizeReadSoFar = 0;
    this.currentLeafCount = 0;
    this.schemaImposedMode = schemaImposedMode;
    this.extendedFormatOptions = extendedFormatOptions;
    this.copyIntoQueryProperties = copyIntoQueryProperties;
    this.queryContext = queryContext;
    this.ingestionProperties = ingestionProperties;
    this.context = context;
    this.trimSpace = getTrimSpaceValue();
    this.filePath = filePath;
    this.fileSize = fileSize;
    this.isValidationMode = isValidationMode;
    this.validationErrorWriter = validationErrorWriter;
    this.processingStartTime = processingStartTime;
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
      while (fieldPath.getChild() != null
          && !fieldPath.getChild().getType().equals(PathSegmentType.ARRAY_INDEX)) {
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

  @Override
  public void setSource(byte[] bytes) throws IOException {
    setSource(new SeekableBAIS(bytes));
  }

  @Override
  public ReadState write(ComplexWriter writer) throws IOException {
    if (parser.isClosed()) {
      return ReadState.END_OF_STREAM;
    }

    JsonToken t = null;
    try {
      t = parser.nextToken();
    } catch (Exception e) {
      resetWriterPosition = false;
      return handleOrRaiseException(writer, e);
    }

    while (!parser.hasCurrentToken() && !parser.isClosed()) {
      t = parser.nextToken();
    }

    if (parser.isClosed()) {
      return ReadState.END_OF_STREAM;
    }

    this.currentLeafCount = 0;
    ReadState readState;
    try {
      readState = writeToVector(writer, t);
      switch (readState) {
        case END_OF_STREAM:
          break;
        case WRITE_SUCCEED:
          if (schemaImposedMode) {
            if (!validCopyIntoFile) {
              throw new TransformationException(
                  String.format("No column name matches target %s", targetSchema),
                  parser.getCurrentLocation().getLineNr());
            }
            recordsLoadedCount++;
            validCopyIntoFile = false;
          }
          break;
        default:
          throw getExceptionWithContext(UserException.dataReadError(), currentFieldName)
              .message(
                  "Failure while reading JSON. (Got an invalid read state %s )",
                  readState.toString())
              .build(logger);
      }

      return readState;
    } catch (Exception e) {
      return handleOrRaiseException(writer, e);
    }
  }

  private void confirmLast() throws IOException {
    parser.nextToken();
    if (!parser.isClosed()) {
      resetWriterPosition = false;
      throw getExceptionWithContext(UserException.dataReadError(), currentFieldName)
          .message(
              "Dremio attempted to unwrap a toplevel list "
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
        if (inOuterList) {
          throw getExceptionWithContext(UserException.dataReadError(), currentFieldName)
              .message(
                  "The top level of your document must either be a single array of maps or a set "
                      + "of white space delimited maps.")
              .build(logger);
        }

        if (skipOuterList) {
          t = parser.nextToken();
          if (t == JsonToken.START_OBJECT) {
            inOuterList = true;
            writeDataSwitch(writer.rootAsStruct());
          } else if (t == VALUE_NULL && schemaImposedMode) {
            inOuterList = true;
            addNullValueForStruct(writer);
            break;
          } else {
            throw getExceptionWithContext(UserException.dataReadError(), currentFieldName)
                .message(
                    "The top level of your document must either be a single array of maps or a set "
                        + "of white space delimited maps.")
                .build(logger);
          }

        } else {
          writeDataSwitch(writer.rootAsList());
        }
        break;
      case END_ARRAY:
        if (inOuterList) {
          confirmLast();
          return ReadState.END_OF_STREAM;
        } else {
          throw getExceptionWithContext(UserException.dataReadError(), currentFieldName)
              .message(
                  "Failure while parsing JSON.  Ran across unexpected %s.", JsonToken.END_ARRAY)
              .build(logger);
        }
      case NOT_AVAILABLE:
        return ReadState.END_OF_STREAM;
      case VALUE_NULL:
        if (schemaImposedMode && inOuterList) {
          addNullValueForStruct(writer);
          break;
        }
        // fall through
      default:
        resetWriterPosition = false;
        throw getExceptionWithContext(UserException.dataReadError(), currentFieldName)
            .message(
                "Failure while parsing JSON.  Found token of [%s]. Root object cannot be a scalar.",
                t)
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
    w.start();
    try {
      if (this.allTextMode || this.schemaImposedMode) {
        if (schemaImposedMode && !isValidationMode) {
          Field rootField = w.getField();
          if (rootFieldWrapper == FieldWrapper.EMPTY || rootFieldWrapper.getField() != rootField) {
            rootFieldWrapper = new FieldWrapper(rootField);
          }
        }
        writeStructDataAllText(rootFieldWrapper, w, this.selection, true, true);
      } else {
        writeStructData(w, this.selection, true);
      }
    } finally {
      w.end();
    }
  }

  private void writeDataSwitch(ListWriter w) throws IOException {
    w.startList();
    try {
      if (this.allTextMode || this.schemaImposedMode) {
        if (schemaImposedMode && !isValidationMode) {
          Field rootField = ((PromotableWriter) w).getField();
          if (rootFieldWrapper == FieldWrapper.EMPTY || rootFieldWrapper.getField() != rootField) {
            rootFieldWrapper = new FieldWrapper(rootField);
          }
        }
        writeListDataAllText(rootFieldWrapper, w, this.selection);
      } else {
        writeListData(w, this.selection);
      }
    } finally {
      w.endList();
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
   * @param map
   * @param selection
   * @param moveForward Whether or not we should start with using the current token or the next
   *     token. If moveForward = true, we should start with the next token and ignore the current
   *     one.
   * @throws IOException
   */
  private void writeStructData(
      BaseWriter.StructWriter map, FieldSelection selection, boolean moveForward)
      throws IOException {
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

        assert t == JsonToken.FIELD_NAME
            : String.format("Expected FIELD_NAME but got %s.", t.name());

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

          case VALUE_FALSE:
            {
              incrementLeafCount();
              map.bit(fieldName).writeBit(0);
              break;
            }
          case VALUE_TRUE:
            {
              incrementLeafCount();
              map.bit(fieldName).writeBit(1);
              break;
            }
          case VALUE_NULL:
            // do nothing as we don't have a type.
            break;
          case VALUE_NUMBER_FLOAT:
            {
              incrementLeafCount();
              map.float8(fieldName).writeFloat8(parser.getDoubleValue());
              break;
            }
          case VALUE_NUMBER_INT:
            {
              incrementLeafCount();
              if (this.readNumbersAsDouble) {
                map.float8(fieldName).writeFloat8(parser.getDoubleValue());
              } else {
                map.bigInt(fieldName).writeBigInt(parser.getLongValue());
              }
              break;
            }
          case VALUE_STRING:
            {
              handleString(parser, map, fieldName);
              break;
            }

          default:
            throw getExceptionWithContext(UserException.dataReadError(), currentFieldName)
                .message("Unexpected token %s", parser.getCurrentToken())
                .build(logger);
        }
      }
    } finally {
      map.end();
    }
  }

  private void writeStructDataAllText(
      FieldWrapper structField,
      BaseWriter.StructWriter map,
      FieldSelection selection,
      boolean moveForward,
      boolean rootStruct)
      throws IOException {
    Set<String> fieldNamesSet = schemaImposedMode ? new HashSet<>() : null;
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
      Boolean skipObject = skipObject(structField, fieldName, selection);
      FieldSelection childSelection = selection.getChild(fieldName);

      if (skipObject) {
        consumeEntireNextValue();
        continue outside;
      }

      JsonToken token = parser.nextToken();

      FieldWrapper originalField = structField.getChild(fieldName);
      if (schemaImposedMode) {
        if (rootStruct) {
          validCopyIntoFile = true;
        }

        String originalName = originalField.getField().getName();

        if (!fieldNamesSet.contains(originalName)) {
          fieldNamesSet.add(originalName);
        } else {
          throw new TransformationException(
              String.format("Duplicate column name %s.", fieldName),
              parser.getCurrentLocation().getLineNr(),
              fieldName);
        }

        if (token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT) {
          ArrowType type = originalField.getField().getType();
          checkForComplexCoercions(type, token, fieldName);
        }
      }

      switch (token) {
        case START_ARRAY:
          startList(originalField.getListElement(), map, fieldName, childSelection);
          break;
        case START_OBJECT:
          if (!writeMapDataIfTyped(map, fieldName)) {
            startStruct(originalField, map, fieldName, childSelection, false, false);
            //            writeStructDataAllText(map.struct(fieldName), childSelection, false,
            // false);
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
            handleStringToType(parser, map, fieldName, originalField.getField().getType());
          } else {
            handleString(parser, map, fieldName);
          }
          break;
        case VALUE_NULL:
          // do nothing as we don't have a type.
          break;

        default:
          throw getExceptionWithContext(UserException.dataReadError(), currentFieldName)
              .message("Unexpected token %s", parser.getCurrentToken())
              .build(logger);
      }
    }
  }

  private void startStruct(
      FieldWrapper field,
      BaseWriter.StructWriter writer,
      String fieldName,
      FieldSelection selection,
      boolean moveForward,
      boolean rootStruct)
      throws IOException {
    BaseWriter.StructWriter structWriter = null;
    try {
      if (!isValidationMode) {
        structWriter = writer.struct(fieldName);
        structWriter.start();
        writeStructDataAllText(field, structWriter, selection, moveForward, rootStruct);
      } else {
        writeStructDataAllText(field, writer, selection, moveForward, rootStruct);
      }
    } finally {
      if (!isValidationMode) {
        structWriter.end();
      }
    }
  }

  private void startStruct(
      FieldWrapper field,
      BaseWriter.ListWriter writer,
      FieldSelection selection,
      boolean moveForward,
      boolean rootStruct)
      throws IOException {
    BaseWriter.StructWriter structWriter = null;
    try {
      if (!isValidationMode) {
        structWriter = writer.struct();
        structWriter.start();
        writeStructDataAllText(field, structWriter, selection, moveForward, rootStruct);
      } else {
        writeStructDataAllText(field, (FieldWriter) writer, selection, moveForward, rootStruct);
      }
    } finally {
      if (!isValidationMode) {
        structWriter.end();
      }
    }
  }

  private void startList(
      FieldWrapper field,
      BaseWriter.StructWriter writer,
      String fieldName,
      FieldSelection selection)
      throws IOException {
    BaseWriter.ListWriter listWriter = null;
    try {
      if (!isValidationMode) {
        listWriter = writer.list(fieldName);
        listWriter.startList();
        writeListDataAllText(field, listWriter, selection);
      } else {
        writeListDataAllText(field, (FieldWriter) writer, selection);
      }
    } finally {
      if (!isValidationMode) {
        listWriter.endList();
      }
    }
  }

  private void startList(FieldWrapper field, BaseWriter.ListWriter writer, FieldSelection selection)
      throws IOException {
    BaseWriter.ListWriter listWriter = null;
    try {
      if (!isValidationMode) {
        listWriter = writer.list();
        listWriter.startList();
        writeListDataAllText(field, listWriter, selection);
      } else {
        writeListDataAllText(field, writer, selection);
      }
    } finally {
      if (!isValidationMode) {
        listWriter.endList();
      }
    }
  }

  /**
   * Will attempt to take the current value and consume it as an extended value (if extended mode is
   * enabled). Whether extended is enable or disabled, will consume the next token in the stream.
   *
   * @param writer
   * @param fieldName
   * @return
   * @throws IOException
   */
  private boolean writeMapDataIfTyped(BaseWriter.StructWriter writer, String fieldName)
      throws IOException {
    if (extended) {
      return mapOutput.run(writer, fieldName);
    } else {
      parser.nextToken();
      return false;
    }
  }

  /**
   * Will attempt to take the current value and consume it as an extended value (if extended mode is
   * enabled). Whether extended is enable or disabled, will consume the next token in the stream.
   *
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

  private void handleStringToType(
      JsonParser parser, BaseWriter.StructWriter writer, String fieldName, ArrowType type)
      throws IOException {
    try {
      incrementLeafCount();
      writeValue(fieldName, type, writer, parser);
    } catch (Exception e) {
      throw new TransformationException(
          e.getMessage(), parser.getCurrentLocation().getLineNr(), fieldName);
    }
  }

  private void handleStringToType(JsonParser parser, BaseWriter.ListWriter writer, ArrowType type)
      throws IOException {
    try {
      incrementLeafCount();
      writeValue(type, writer, parser);
    } catch (Exception e) {
      throw new TransformationException(e.getMessage(), parser.getCurrentLocation().getLineNr());
    }
  }

  /**
   * Converts a json element value from string format to an arbitrary java type.
   *
   * @param fieldName the name of the json element/target table column
   * @param type the arrow type that tells us what should be the output type
   * @param parser json parser instance, must be not null
   * @return the type converted object
   */
  private Object convertValue(String fieldName, ArrowType type, JsonParser parser)
      throws IOException {
    if (parser.getCurrentToken() == VALUE_NULL) {
      return null;
    }
    String value =
        EasyFormatUtils.applyStringTransformations(
            parser.getText(), extendedFormatOptions, trimSpace);
    if (value == null) {
      return null;
    }
    try {
      if (CompleteType.BIT.getType().equals(type)) {
        return EasyFormatUtils.JsonBooleanFunction.apply(value);
      } else if (CompleteType.INT.getType().equals(type)) {
        return Integer.valueOf(value);
      } else if (CompleteType.BIGINT.getType().equals(type)) {
        return Long.valueOf(value);
      } else if (CompleteType.FLOAT.getType().equals(type)) {
        return Float.valueOf(value);
      } else if (CompleteType.DOUBLE.getType().equals(type)) {
        return Double.valueOf(value);
      } else if (ArrowType.ArrowTypeID.Decimal.equals(type.getTypeID())) {
        return EasyFormatUtils.getBigDecimalValue(type, value);
      } else if (CompleteType.VARCHAR.getType().equals(type)) {
        return value;
      } else if (CompleteType.DATE.getType().equals(type)
          || CompleteType.TIME.getType().equals(type)
          || CompleteType.TIMESTAMP.getType().equals(type)) {
        return EasyFormatUtils.getDateTimeValueAsUnixTimestamp(value, type, extendedFormatOptions);
      } else {
        throw new RuntimeException("Unsupported data type :" + type);
      }
    } catch (IllegalArgumentException e) {
      throw EasyFormatUtils.handleExceptionDuringCoercion(value, type, fieldName, e);
    }
  }

  private void writeValue(
      String fieldName, ArrowType type, BaseWriter.StructWriter writer, JsonParser parser)
      throws IOException {
    Object value = convertValue(fieldName, type, parser);
    if (isValidationMode) {
      return;
    }
    if (value == null) {
      writeNull(fieldName, type, writer);
      return;
    }

    if (CompleteType.BIT.getType().equals(type)) {
      writer.bit(fieldName).writeBit((Integer) value);
    } else if (CompleteType.INT.getType().equals(type)) {
      writer.integer(fieldName).writeInt((Integer) value);
    } else if (CompleteType.BIGINT.getType().equals(type)) {
      writer.bigInt(fieldName).writeBigInt((Long) value);
    } else if (CompleteType.FLOAT.getType().equals(type)) {
      writer.float4(fieldName).writeFloat4((Float) value);
    } else if (CompleteType.DOUBLE.getType().equals(type)) {
      writer.float8(fieldName).writeFloat8((Double) value);
    } else if (ArrowType.ArrowTypeID.Decimal.equals(type.getTypeID())) {
      writer
          .decimal(
              fieldName,
              ((ArrowType.Decimal) type).getScale(),
              ((ArrowType.Decimal) type).getPrecision())
          .writeDecimal((BigDecimal) value);
    } else if (CompleteType.VARCHAR.getType().equals(type)) {
      writeString((String) value, writer, fieldName);
    } else if (CompleteType.DATE.getType().equals(type)) {
      writer.dateMilli(fieldName).writeDateMilli((Long) value);
    } else if (CompleteType.TIME.getType().equals(type)) {
      writer.timeMilli(fieldName).writeTimeMilli(((Long) value).intValue());
    } else if (CompleteType.TIMESTAMP.getType().equals(type)) {
      writer.timeStampMilli(fieldName).writeTimeStampMilli((Long) value);
    }
  }

  private void writeValue(ArrowType type, BaseWriter.ListWriter writer, JsonParser parser)
      throws IOException {
    Object value = convertValue(null, type, parser);
    if (isValidationMode) {
      return;
    }
    if (value == null) {
      writeNull(type, writer);
      return;
    }

    if (CompleteType.BIT.getType().equals(type)) {
      writer.bit().writeBit((Integer) value);
    } else if (CompleteType.INT.getType().equals(type)) {
      writer.integer().writeInt((Integer) value);
    } else if (CompleteType.BIGINT.getType().equals(type)) {
      writer.bigInt().writeBigInt((Long) value);
    } else if (CompleteType.FLOAT.getType().equals(type)) {
      writer.float4().writeFloat4((Float) value);
    } else if (CompleteType.DOUBLE.getType().equals(type)) {
      writer.float8().writeFloat8((Double) value);
    } else if (ArrowType.ArrowTypeID.Decimal.equals(type.getTypeID())) {
      writer.decimal().writeDecimal((BigDecimal) value);
    } else if (CompleteType.VARCHAR.getType().equals(type)) {
      writeString((String) value, writer);
    } else if (CompleteType.DATE.getType().equals(type)) {
      writer.dateMilli().writeDateMilli((Long) value);
    } else if (CompleteType.TIME.getType().equals(type)) {
      writer.timeMilli().writeTimeMilli(((Long) value).intValue());
    } else if (CompleteType.TIMESTAMP.getType().equals(type)) {
      writer.timeStampMilli().writeTimeStampMilli((Long) value);
    }
  }

  private boolean getTrimSpaceValue() {
    return (extendedFormatOptions != null && extendedFormatOptions.getTrimSpace() != null)
        ? extendedFormatOptions.getTrimSpace()
        : false;
  }

  private void writeNull(String fieldName, ArrowType type, BaseWriter.StructWriter writer) {
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
      writer
          .decimal(
              fieldName,
              ((ArrowType.Decimal) type).getScale(),
              ((ArrowType.Decimal) type).getPrecision())
          .writeNull();
    } else if (CompleteType.VARCHAR.getType().equals(type)) {
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

  private void writeNull(ArrowType type, BaseWriter.ListWriter writer) {
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
    } else if (CompleteType.STRUCT.getType().equals(type)) {
      writer.struct().writeNull();
    } else if (CompleteType.LIST.getType().equals(type)) {
      writer.list().writeNull();
    } else {
      throw new RuntimeException("Unsupported data type : " + type);
    }
  }

  /**
   * This utility method handles errors related to transformations during the reading of a JSON
   * file. Based on the reading mode configured by {@link JsonReader#copyIntoQueryProperties}, the
   * provided exception is either rethrown or the error metadata is stored in JSON format in an
   * "error" column on the target table.
   *
   * <p>Reader modes:
   *
   * <p>ABORT -> the exception is rethrown. CONTINUE, SKIP_FILE -> a new record is stored in the
   * error column.
   *
   * @param writer output writer object
   * @param exception exception object raised during parsing
   * @return if the exception is not rethrown the {@link
   *     com.dremio.exec.store.easy.json.JsonProcessor.ReadState} is returned
   */
  private ReadState handleOrRaiseException(ComplexWriter writer, Exception exception)
      throws IOException {
    this.hasErrors = true;
    if (processError(writer, exception)) {
      return copyIntoQueryProperties.getOnErrorOption() == SKIP_FILE
          ? ReadState.VALIDATION_ERROR
          : ReadState.WRITE_SUCCEED;
    }
    if (processErrorDuringValidation(writer, exception)) {
      return ReadState.VALIDATION_ERROR;
    }
    throw new JsonReaderIOException(exception);
  }

  /**
   * Persist error record based on the configured {@link JsonReader#copyIntoQueryProperties}.
   *
   * @param writer output writer object
   * @param exception exception object raised during parsing
   * @return true, if the error record is persisted
   */
  private boolean processError(ComplexWriter writer, Exception exception) throws IOException {
    if (copyIntoQueryProperties != null) {
      CopyIntoQueryProperties.OnErrorOption option = copyIntoQueryProperties.getOnErrorOption();
      logger.debug(
          String.format(
              "Encountered error while reading json file. JsonReader is running in '%s' mode.",
              option),
          exception);
      if (schemaImposedMode && (CONTINUE == option || SKIP_FILE == option)) {
        writeErrorEvent(
            writer.rootAsStruct(), CopyIntoExceptionUtils.redactException(exception).getMessage());
        parser.close();
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("ReturnValueIgnored")
  private <T extends Exception> boolean processErrorDuringValidation(
      ComplexWriter writer, T exception) throws IOException {
    if (isValidationMode) {
      String fieldName = null;
      Long lineNumber = null;
      if (exception instanceof TransformationException) {
        fieldName = ((TransformationException) exception).getFieldName();
        lineNumber = (long) ((TransformationException) exception).getLineNumber();
      } else if (exception instanceof JsonParseException) {
        lineNumber = (long) ((JsonParseException) exception).getLocation().getLineNr();
      }
      validationErrorWriter.write(
          fieldName,
          recordsLoadedCount + 1,
          lineNumber,
          CopyIntoExceptionUtils.redactException(exception).getMessage());

      parser.close();
      return true;
    }
    return false;
  }

  /**
   * Prepare and write error metadata to the target table {@link
   * ColumnUtils#COPY_HISTORY_COLUMN_NAME} column. The metadata definition is declared by {@link
   * CopyIntoFileLoadInfo} and it is serialized to json format.
   *
   * @param structWriter root struct writer, must be not null
   * @param errorMessage message describing the error
   * @throws IOException if the writing fails for some reason
   */
  private void writeErrorEvent(StructWriter structWriter, String errorMessage) throws IOException {
    // when the error was raised we were in the middle of processing a json record, which was
    // already submitted to the
    // root structWriter. Therefore, we have an unfinished record which should be dropped.
    if (structWriter.getPosition() > 0 && resetWriterPosition) {
      structWriter.setPosition(structWriter.getPosition() - 1);
    }
    long recordsCount =
        copyIntoQueryProperties.getOnErrorOption() == SKIP_FILE ? 0L : this.recordsLoadedCount;

    String infoJson =
        getFileLoadInfoJson(
            recordsCount == 0 ? CopyIntoFileState.SKIPPED : CopyIntoFileState.PARTIALLY_LOADED,
            recordsCount,
            1L,
            errorMessage);
    writeString(infoJson, structWriter, ColumnUtils.COPY_HISTORY_COLUMN_NAME);
  }

  /**
   * Generates a JSON representation of file load information.
   *
   * @param fileState The state of the file load.
   * @param recordsLoadedCount The number of records loaded.
   * @param recordsRejectedCount The number of records rejected.
   * @param firstErrorMessage The first error message encountered during the file load.
   * @return A JSON string representing the file load information.
   */
  private String getFileLoadInfoJson(
      CopyIntoFileState fileState,
      long recordsLoadedCount,
      long recordsRejectedCount,
      String firstErrorMessage) {
    Builder builder =
        new Builder(
                queryContext.getQueryId(),
                queryContext.getUserName(),
                queryContext.getTableNamespace(),
                copyIntoQueryProperties.getStorageLocation(),
                filePath,
                extendedFormatOptions,
                FileType.JSON.name(),
                fileState)
            .setRecordsLoadedCount(recordsLoadedCount)
            .setRecordsRejectedCount(recordsRejectedCount)
            .setBranch(copyIntoQueryProperties.getBranch())
            .setProcessingStartTime(processingStartTime)
            .setFileSize(fileSize)
            .setFirstErrorMessage(firstErrorMessage);

    if (ingestionProperties != null) {
      builder
          .setPipeName(ingestionProperties.getPipeName())
          .setPipeId(ingestionProperties.getPipeId())
          .setFileNotificationTimestamp(ingestionProperties.getNotificationTimestamp())
          .setIngestionSourceType(ingestionProperties.getIngestionSourceType())
          .setRequestId(ingestionProperties.getRequestId());
    }

    return FileLoadInfo.Util.getJson(builder.build());
  }

  @Override
  public int writeSuccessfulParseEvent(ComplexWriter writer) throws IOException {
    if (!hasErrors
        && copyIntoQueryProperties != null
        && copyIntoQueryProperties.shouldRecord(
            CopyIntoFileLoadInfo.CopyIntoFileState.FULLY_LOADED)) {
      String infoJson =
          getFileLoadInfoJson(CopyIntoFileState.FULLY_LOADED, recordsLoadedCount, 0L, null);
      writeString(infoJson, writer.rootAsStruct(), ColumnUtils.COPY_HISTORY_COLUMN_NAME);
      recordsLoadedCount++;
      logger.debug("Recording successful parsing event for {}", filePath);
      return 1;
    }

    return 0;
  }

  private void handleString(JsonParser parser, BaseWriter.StructWriter writer, String fieldName)
      throws IOException {
    incrementLeafCount();
    writeString(parser.getText(), writer, fieldName);
  }

  private void writeString(String val, BaseWriter.StructWriter writer, String fieldName)
      throws IOException {
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
    outside:
    while (true) {
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
          case VALUE_FALSE:
            {
              incrementLeafCount();
              list.bit().writeBit(0);
              // dataSizeReadSoFar += 1; - not counting as it takes 1-bit per value
              break;
            }
          case VALUE_TRUE:
            {
              incrementLeafCount();
              list.bit().writeBit(1);
              // dataSizeReadSoFar += 1; - not counting as it takes 1-bit per value
              break;
            }
          case VALUE_NULL:
            throw UserException.unsupportedError()
                .message(
                    "Null values are not supported in lists by default. "
                        + "Please set `store.json.all_text_mode` to true to read lists containing nulls. "
                        + "Be advised that this will treat JSON null values as a string containing the word 'null'.")
                .build(logger);
          case VALUE_NUMBER_FLOAT:
            {
              incrementLeafCount();
              list.float8().writeFloat8(parser.getDoubleValue());
              dataSizeReadSoFar += 8;
              break;
            }
          case VALUE_NUMBER_INT:
            {
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

        // Take the maximum number of leaves from the current and the calculated for this array
        // entry.
        maxArrayLeafCount = Math.max(maxArrayLeafCount, currentLeafCount);
      } catch (Exception e) {
        throw getExceptionWithContext(e, this.currentFieldName).build(logger);
      }
    }

    // Take the maximum calculated leaf count from the array.
    currentLeafCount = maxArrayLeafCount;
    list.endList();
  }

  private void writeListDataAllText(
      FieldWrapper listField, BaseWriter.ListWriter list, FieldSelection selection)
      throws IOException {
    final int originalLeafCount = currentLeafCount;
    int maxArrayLeafCount = 0;
    outside:
    while (true) {
      currentLeafCount = originalLeafCount;

      JsonToken token = parser.nextToken();
      if (schemaImposedMode
          && (token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT)) {
        ArrowType type = listField.getField().getType();
        checkForComplexCoercions(type, token, "");
      }

      switch (token) {
        case START_ARRAY:
          startList(listField.getListElement(), list, selection);
          break;
        case START_OBJECT:
          if (!writeListDataIfTyped(list)) {
            startStruct(listField, list, selection, false, false);
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
            handleStringToType(parser, list, listField.getField().getType());
          } else {
            handleString(parser, list);
          }
          break;
        default:
          throw getExceptionWithContext(UserException.dataReadError(), currentFieldName)
              .message("Unexpected token %s", parser.getCurrentToken())
              .build(logger);
      }

      // Take the maximum number of leaves from the current and the calculated for this array entry.
      maxArrayLeafCount = Math.max(maxArrayLeafCount, currentLeafCount);
    }

    // Take the maximum calculated leaf count from the array.
    currentLeafCount = maxArrayLeafCount;
  }

  /**
   * Increment the current leaf count and throw ColumnCountTooLargeException if the max limit is
   * exceeded.
   */
  private void incrementLeafCount() {
    if (++currentLeafCount > maxLeafLimit) {
      throw new ColumnCountTooLargeException(maxLeafLimit);
    }
  }

  private boolean skipObject(FieldWrapper field, String fieldName, FieldSelection selection)
      throws TransformationException {
    boolean neverValid = selection.getChild(fieldName).isNeverValid();

    if (schemaImposedMode) {
      FieldWrapper currentField = field.getChild(fieldName);
      if (!Objects.isNull(currentField)
          && CompleteType.MAP.getType().equals(currentField.getField().getType())) {
        throw new TransformationException(
            String.format(
                "'COPY INTO Command' does not support MAP Type. Found Map type field : '%s'. ",
                fieldName),
            parser.getCurrentLocation().getLineNr(),
            fieldName);
      }
      return currentField == null || neverValid;
    } else {
      return neverValid;
    }
  }

  /**
   * Validate if the current json token matches the type of the column in the target table.
   *
   * @param type type of the column in the target table, must be not-null
   * @param token current json token, must be not-null
   * @param fieldName name of the column, must be not-null
   */
  private void checkForComplexCoercions(ArrowType type, JsonToken token, String fieldName)
      throws TransformationException {
    String errorMessage = null;
    switch (token) {
      case START_ARRAY:
        if (!ArrowType.ArrowTypeID.List.equals(type.getTypeID())) {
          errorMessage =
              String.format(
                  "Field %s having %s datatype in the file cannot be coerced into %s datatype "
                      + "of the target table.",
                  fieldName, "List", type);
        }
        break;
      case START_OBJECT:
        if (!ArrowType.ArrowTypeID.Struct.equals(type.getTypeID())) {
          errorMessage =
              String.format(
                  "Field %s having %s datatype in the file cannot be coerced into %s datatype "
                      + "of the target table.",
                  fieldName, "Struct", type);
        }
        break;
      default:
        errorMessage = "Unexpected token found. It should be either start of an array or struct.";
        break;
    }

    if (errorMessage != null) {
      throw new TransformationException(
          errorMessage, parser.getCurrentLocation().getLineNr(), fieldName);
    }
  }
}
