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
package com.dremio.plugins.elastic.execution;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.calcite.util.Pair;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.easy.json.reader.BaseJsonProcessor;
import com.dremio.exec.vector.complex.fn.FieldSelection;
import com.dremio.exec.vector.complex.fn.WorkingBuffer;
import com.dremio.plugins.elastic.ElasticsearchConstants;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Preconditions;
import io.netty.buffer.ArrowBuf;


public class ElasticsearchJsonReader extends BaseJsonProcessor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticsearchJsonReader.class);

  private static final boolean EXTRA_STREAM_DEBUG = false;
  private final WorkingBuffer workingBuffer;
  private final List<SchemaPath> columns;
  private final FieldReadDefinition rootDefinition;
  private final String resourceName;
  private final boolean fieldsProjected;

  private final boolean metaUIDSelected;
  private final boolean metaIDSelected;
  private final boolean metaIndexSelected;
  private final boolean metaTypeSelected;

  /**
   * The name of the current field being parsed. For Error messages.
   */
  private String currentFieldName = "<none>";

  private FieldSelection selection;
  private String index;
  private String type;

  public ElasticsearchJsonReader(ArrowBuf managedBuf,
                                 List<SchemaPath> columns,
                                 String resourceName,
                                 FieldReadDefinition rootDefinition,
                                 boolean fieldsProjected,
                                 boolean metaUIDSelected,
                                 boolean metaIDSelected,
                                 boolean metaTypeSelected,
                                 boolean metaIndexSelected) {

    this.workingBuffer = new WorkingBuffer(managedBuf);
    this.resourceName = resourceName;
    this.columns = columns;
    this.rootDefinition = rootDefinition;
    this.fieldsProjected = fieldsProjected;

    this.selection = FieldSelection.getFieldSelection(columns);

    this.metaUIDSelected = metaUIDSelected;
    this.metaIDSelected = metaIDSelected;
    this.metaIndexSelected = metaIndexSelected;
    this.metaTypeSelected = metaTypeSelected;
  }

  public Pair<String, Long> getScrollAndTotalSizeThenSeekToHits() throws IOException {
    final JsonToken token = seekForward(ElasticsearchConstants.SCROLL_ID);
    Preconditions.checkState(token == JsonToken.VALUE_STRING, "Invalid response");

    final String scroll_id = parser.getValueAsString();

    seekForward(ElasticsearchConstants.HITS);
    final JsonToken totalSizeToken = seekForward(ElasticsearchConstants.TOTAL_HITS);
    Preconditions.checkState(totalSizeToken == JsonToken.VALUE_NUMBER_INT, "Invalid response");
    final long totalSize = parser.getValueAsLong();

    final JsonToken hitsToken = seekForward(ElasticsearchConstants.HITS);
    Preconditions.checkState(hitsToken == JsonToken.START_ARRAY, "Invalid response");
    return new Pair<>(scroll_id, totalSize);
  }

  @Override
  public void ensureAtLeastOneField(ComplexWriter writer) {
    // if we had no columns, create one empty one so we can return some data for count purposes.
    SchemaPath sp = columns.get(0);
    PathSegment fieldPath = sp.getRootSegment();
    BaseWriter.StructWriter fieldWriter = writer.rootAsStruct();
    while (fieldPath.getChild() != null && !fieldPath.getChild().isArray()) {
      fieldWriter = fieldWriter.struct(fieldPath.getNameSegment().getPath());
      fieldPath = fieldPath.getChild();
    }
    if (fieldWriter.isEmptyStruct()) {
      fieldWriter.integer(fieldPath.getNameSegment().getPath());
    }
  }

  @Override
  public ReadState write(ComplexWriter writer) throws IOException {

    JsonToken t = parser.nextToken();

    while ( !parser.hasCurrentToken() && !parser.isClosed()) {
      t = parser.nextToken();
    }

    if (parser.isClosed()) {
      return ReadState.END_OF_STREAM;
    }

    ReadState readState;

    if (t != JsonToken.END_ARRAY) {
      t = seekForward(ElasticsearchConstants.INDEX);
      if (t == null) {
        return ReadState.END_OF_STREAM;
      }

      StructWriter structWriter = writer.rootAsStruct();
      structWriter.start();

      assert t == JsonToken.VALUE_STRING;
      index = parser.getText();
      if (metaIndexSelected) {
        structWriter.varChar(ElasticsearchConstants.INDEX).writeVarChar(0, workingBuffer.prepareVarCharHolder(index), workingBuffer.getBuf());
      }

      t = seekForward(ElasticsearchConstants.TYPE);
      assert t == JsonToken.VALUE_STRING;
      type = parser.getText();
      if (metaTypeSelected) {
        structWriter.varChar(ElasticsearchConstants.TYPE).writeVarChar(0, workingBuffer.prepareVarCharHolder(type), workingBuffer.getBuf());
      }

      // We use _uid, since _uid can be used in aggregation pipelines, while _id cannot.
      // Per elasticsearch doc, _uid = _type # _id.
      if (metaUIDSelected || metaIDSelected) {
        t = seekForward(ElasticsearchConstants.ID);
        assert t == JsonToken.VALUE_STRING;

        final String thisID = parser.getText();
        if (metaUIDSelected) {
          final String uid = this.type + "#" + thisID;
          structWriter.varChar(ElasticsearchConstants.UID).writeVarChar(0, workingBuffer.prepareVarCharHolder(uid), workingBuffer.getBuf());
        }
        if (metaIDSelected) {
          structWriter.varChar(ElasticsearchConstants.ID).writeVarChar(0, workingBuffer.prepareVarCharHolder(thisID), workingBuffer.getBuf());
        }
      }

      if (fieldsProjected) {
        t = seekForward(ElasticsearchConstants.FIELDS);
        if (t == null) {
          structWriter.end();
          return ReadState.WRITE_SUCCEED;
        } else {
          readState = writeToVector(structWriter, t);
        }
      } else {
        t = seekForward(ElasticsearchConstants.SOURCE);
        readState = writeToVector(structWriter, t);
      }

      t = parser.nextToken();
      assert t == JsonToken.END_OBJECT : "Received " + t + " when expected to receive END_OBJECT";
      parser.skipChildren();
      assert parser.getCurrentToken() == JsonToken.END_OBJECT;

      structWriter.end();
    } else {
      readState = ReadState.END_OF_STREAM;
    }

    switch (readState) {
      case END_OF_STREAM:
        break;
      case WRITE_SUCCEED:
        break;
      default:
        throw getExceptionWithContext(
          UserException.dataReadError(), currentFieldName, null)
          .message("Failure while reading JSON. (Got an invalid read state %s )", readState.toString())
          .build(logger);
    }

    return readState;
  }

  private ReadState writeToVector(StructWriter writer, JsonToken t) throws IOException {
    switch (t) {
      case START_OBJECT:
      case START_ARRAY:
        writeDeclaredMap(writer, this.selection, rootDefinition, true /* move forward*/, null, true /* first level*/);
        break;
      case END_ARRAY:
      case NOT_AVAILABLE:
        return ReadState.END_OF_STREAM;
      default:
        throw getExceptionWithContext(
          UserException.dataReadError(), currentFieldName, null)
          .message("Failure while parsing JSON.  Found token of %s.  Dremio currently only supports parsing "
                  + "json strings that contain either lists or maps.  The root object cannot be a scalar.", t)
          .build(logger);
    }

    return ReadState.WRITE_SUCCEED;

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
        // just return;
    }
  }

  private void p(String str, Object...objects){
    if(EXTRA_STREAM_DEBUG){
      System.out.println(String.format(str,  objects));
    }
  }

  private void writeDeclaredMap(StructWriter map,
                         FieldSelection selection,
                         FieldReadDefinition parentDefinition,
                         boolean moveForward,
                         SchemaPath parentPath,
                         boolean firstLevel) throws IOException {
    p("enter map. %s", System.identityHashCode(map));
    if (!firstLevel) {
      map.start();
    }
    try {
      while (true) {
        JsonToken originalToken = parser.getCurrentToken();
        p("original: %s", originalToken);
        JsonToken t;
        if (moveForward) {
          t = parser.nextToken();
        } else {
          t = parser.getCurrentToken();
          moveForward = true;
        }

        p("updated: %s.", t);
        if (t == JsonToken.NOT_AVAILABLE || t == JsonToken.END_OBJECT) {
          return;
        }

        Preconditions.checkState(t == JsonToken.FIELD_NAME, String.format("Expected FIELD_NAME but got %s.", t.name()));

        final String fieldName = parser.getText();
        this.currentFieldName = fieldName;
        FieldSelection childSelection = selection.getChild(fieldName);
        if (childSelection.isNeverValid()) {
          consumeEntireNextValue();
          continue;
        }

        final FieldReadDefinition definition = parentDefinition.getChild(fieldName);

        if(definition == null){
          p("writing undeclared map: %s.", t);
          // since we're already partially reading a map, we want to avoid map.Start() events and also forward movement.
          writeUndeclaredMap(map, selection, false, true, true);
          continue;
        }

        // skip hidden fields.
        if(definition.isHidden()){
          consumeEntireNextValue();
          continue;
        }

        final SchemaPath path = parentPath == null ? SchemaPath.getSimplePath(fieldName) : parentPath.getChild(fieldName);

        t = parser.nextToken();

        p("updated2: %s.", t);
        if (t == JsonToken.VALUE_NULL) {
          // do nothing.
        } else if (t == JsonToken.START_ARRAY) {
          writeDeclaredList(map.list(fieldName), childSelection, definition.asNoList(), path, true, true);
        } else if (definition.isArray()){
          // we need to do a promoted scalar behavior (data is a scalar but elsewhere is a list, treat it as a list always).
          ListWriter list = map.list(fieldName);
          list.startList();
          if(t == JsonToken.START_OBJECT){
            writeDeclaredMap(list.struct(), childSelection, definition, true, path, false);
          } else {
            definition.writeList(list, t, parser);
          }
          list.endList();

        } else if (t == JsonToken.START_OBJECT) {
          writeDeclaredMap(map.struct(fieldName), childSelection, definition.asNoList(), true, path, false);
        } else if (t == JsonToken.END_OBJECT) {
          return;
        } else {
          definition.writeMap(map, t, parser);
        }
      }
    } finally {
      p("exit map %s.", System.identityHashCode(map));
      if (!firstLevel) {
        map.end();
      }
    }
  }

  private void writeDeclaredList(ListWriter list, FieldSelection selection, FieldReadDefinition readDef, SchemaPath path, boolean startList, boolean allowNestList) throws IOException {
    if(startList && allowNestList){
      list.startList();
    }
    while (true) {
      try {
        JsonToken token = parser.nextToken();
        if (token == JsonToken.START_ARRAY) {
          boolean innerAllowNestList = readDef.isArray() || readDef.isGeo();
          writeDeclaredList(innerAllowNestList ? list.list() : list, selection, readDef, path, true, innerAllowNestList);
        } else if (token == JsonToken.END_ARRAY) {
          break;
        } else if (token == JsonToken.END_OBJECT) {
          continue;
        } else if (token == JsonToken.START_OBJECT) {
          writeDeclaredMap(list.struct(), selection, readDef, true, path, false);
        } else {
          readDef.writeList(list, token, parser);
        }
      } catch (Exception e) {
        throw getExceptionWithContext(e, this.currentFieldName, null).build(logger);
      }
    }

    if(startList && allowNestList){
      list.endList();
    }
  }

  /**
   * Seek to desired field name.
   * @param fieldName The name of the field to find.
   * @return Either the current token or null if the position wasn't found.
   * @throws IOException
   * @throws JsonParseException
   */
  private JsonToken seekForward(String fieldName) throws IOException, JsonParseException {
    JsonToken token = null;

    String currentName;
    token = parser.getCurrentToken();
    if (token == null) {
      token = parser.nextToken();
    }

    while ((token = parser.nextToken()) != null) {
      if (token == JsonToken.START_OBJECT) {
        token = parser.nextToken();
      }
      if (token == JsonToken.FIELD_NAME) {
        // found a node, go one level deep
        currentName = parser.getCurrentName();
        if (currentName.equals(fieldName)) {
          return parser.nextToken();
        } else {
          // get field token (can be value, object or array)
          parser.nextToken();
          parser.skipChildren();
        }
      } else {
        break;
      }
    }

    return null;
  }


  private void writeUndeclaredMap(StructWriter map, FieldSelection selection, boolean moveForward, boolean skipMapStartEnd, boolean breakAfterSingle) throws IOException {
    //
    if(!skipMapStartEnd){
      map.start();
    }
    try {
      outside: while (true) {

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
          writeUndeclaredList(map.list(fieldName), childSelection);
          break;
        case START_OBJECT:
          writeUndeclaredMap(map.struct(fieldName), childSelection, true, false, false);
          break;
        case END_OBJECT:
          break outside;

        case VALUE_FALSE: {
          map.bit(fieldName).writeBit(0);
          break;
        }
        case VALUE_TRUE: {
          map.bit(fieldName).writeBit(1);
          break;
        }
        case VALUE_NULL:
          // do nothing as we don't have a type.
          break;
        case VALUE_NUMBER_FLOAT:
          map.float8(fieldName).writeFloat8(parser.getDoubleValue());
          break;
        case VALUE_NUMBER_INT:
          map.bigInt(fieldName).writeBigInt(parser.getLongValue());
          break;
        case VALUE_STRING:
          map.varChar(fieldName).writeVarChar(0, workingBuffer.prepareVarCharHolder(parser.getText()), workingBuffer.getBuf());
          break;

        default:
          throw getExceptionWithContext(UserException.dataReadError(), currentFieldName, null)
            .message("Unexpected token %s", parser.getCurrentToken()).build(logger);
        }

        if(breakAfterSingle){
          return;
        }
      }
    } finally {
      if(!skipMapStartEnd){
        map.end();
      }
    }

  }

  public static boolean parseElasticBoolean(String toParse) {
    assert toParse != null : "Elastic group by returned null string as key " + toParse;
    switch(toParse.toLowerCase()) {
      case "":
      case "0":
      case "false":
      case "off":
      case "no":
      case "0.0":
        return false;
      default:
        return true;
    }
  }

  private void writeUndeclaredList(ListWriter list, FieldSelection selection) throws IOException {
    list.startList();
    outside: while (true) {
      try {
        switch (parser.nextToken()) {
        case START_ARRAY:
          writeUndeclaredList(list.list(), selection);
          break;
        case START_OBJECT:
          writeUndeclaredMap(list.struct(), selection, true, false, false);
          break;
        case END_ARRAY:
        case END_OBJECT:
          break outside;

        case VALUE_EMBEDDED_OBJECT:
        case VALUE_FALSE: {
          list.bit().writeBit(0);
          break;
        }
        case VALUE_TRUE: {
          list.bit().writeBit(1);
          break;
        }
        case VALUE_NULL:
          throw UserException.unsupportedError()
            .message("Null values are not supported in lists read from Elasticsearch. " +
              "Please alter the query to filter out null values or remove the null values from Elasticsearch.")
            .build(logger);
        case VALUE_NUMBER_FLOAT:
          list.float8().writeFloat8(parser.getDoubleValue());
          break;
        case VALUE_NUMBER_INT:
          list.bigInt().writeBigInt(parser.getLongValue());
          break;
        case VALUE_STRING:
          list.varChar().writeVarChar(0, workingBuffer.prepareVarCharHolder(parser.getText()), workingBuffer.getBuf());
          break;
        default:
          throw UserException.dataReadError().message("Unexpected token %s", parser.getCurrentToken()).build(logger);
        }
      } catch (Exception e) {
        throw getExceptionWithContext(e, this.currentFieldName, null).build(logger);
      }
    }
    list.endList();

  }
}
