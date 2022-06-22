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
package com.dremio.exec.record;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FieldIdUtil2;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.BasePath;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.common.types.SupportsTypeCoercionsAndUpPromotions;
import com.dremio.exec.exception.NoSupportedUpPromotionOrCoercionException;
import com.dremio.exec.vector.complex.fn.FieldSelection;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.flatbuffers.FlatBufferBuilder;

import io.protostuff.ByteString;

/**
 * An Arrow schema that also carries BatchSchema.
 */
@JsonSerialize(using=BatchSchema.Ser.class)
@JsonDeserialize(using=BatchSchema.De.class)
public class BatchSchema extends org.apache.arrow.vector.types.pojo.Schema implements Iterable<Field> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchSchema.class);

  // Dummy schema used when we cannot sample (no data from source) and there is no data from the source.
  // This can happen if the table is defined but data is not yet present in sources like Mongo/Elasticsearch.
  public static final String SCHEMA_UNKNOWN_NO_DATA_COLNAME = "NO_DATA";
  public static final BatchSchema SCHEMA_UNKNOWN_NO_DATA = BatchSchema.newBuilder().addField(new Field(SCHEMA_UNKNOWN_NO_DATA_COLNAME, new FieldType(true, new ArrowType.Utf8(), null ), null)).build();
  public static final BatchSchema EMPTY = new BatchSchema(Collections.EMPTY_LIST);
  public static final UnknownSchema UNKNOWN_SCHEMA_OBJECT = new UnknownSchema();

  public static final String MIXED_TYPES_ERROR = "Mixed types are not supported as returned values over JDBC, ODBC and Flight connections.";

  public static final class UnknownSchema {
    public final String NO_DATA;
    private UnknownSchema() {
      this.NO_DATA = null;
    }
  }

  private final SelectionVectorMode selectionVectorMode;

  public BatchSchema(List<Field> fields) {
    super(fields);
    this.selectionVectorMode = SelectionVectorMode.NONE;
  }

  BatchSchema(SelectionVectorMode selectionVector, List<Field> fields) {
    super(fields);
    this.selectionVectorMode = selectionVector;
  }

  public static SchemaBuilder newBuilder() {
    return new SchemaBuilder();
  }

  public int getFieldCount() {
    return getFields().size();
  }

  public TypedFieldId getFieldId(BasePath path) {
    return FieldIdUtil2.getFieldId(this, path);
  }

  public Field getColumn(int index) {
    if (index < 0 || index >= getFields().size()) {
      return null;
    }
    return getFields().get(index);
  }

  public boolean isUnknownSchema() {
    return SCHEMA_UNKNOWN_NO_DATA.equals(this);
  }

  public static void assertNoUnion(List<Field> fields) {
    List<String> errorPath = doAssertNoUnion(fields, new ArrayList<>());
    if (errorPath == null) {
      return;
    } else {
      String errorMessage = MIXED_TYPES_ERROR;
      errorMessage += String.format(" Cast %s to a primitive data type either in the "
        + "select statement or the VDS definition.", errorPath.stream().collect(Collectors.joining(".", "\"", "\"")));
      throw UserException.unsupportedError().message(errorMessage).buildSilently();
    }
  }

  public static List<String> doAssertNoUnion(List<Field> fields, List<String> errorPath) {
    for (Field f : fields) {
      if(f.getFieldType().getType().getTypeID() == ArrowType.ArrowTypeID.Union) {
        errorPath.add(f.getName());
        return errorPath;
      } else {
        errorPath.add(f.getName());
        List<String> lowerErrorPath = doAssertNoUnion(f.getChildren(), errorPath);
        if (lowerErrorPath != null) {
          return lowerErrorPath;
        }
        errorPath.remove(errorPath.size() - 1);
      }
    }
    return null;
  }

  @Override
  public Iterator<Field> iterator() {
    return getFields().iterator();
  }

  public SelectionVectorMode getSelectionVectorMode() {
    return selectionVectorMode;
  }

  /**
   * Masks an existing schema based on the provided schemapath. Additionally, reorders the schema to match the requested schema path.
   * @param schemaPaths
   * @return
   */
  public BatchSchema maskAndReorder(List<? extends BasePath> schemaPaths){
    return mask(schemaPaths, true);
  }

  public BatchSchema mask(List<? extends BasePath> schemaPaths, boolean reorder){
    FieldSelection selection = FieldSelection.getFieldSelection(schemaPaths);

    // mask the fields.
    List<Field> newFields = maskFields(this.getFields(), selection);
    if(!reorder){
      return new BatchSchema(selectionVectorMode, newFields);
    }

    // now reorder them as the list declared them (to ensure Calcite and Dremio schema match at the scan level.
    Map<String, Field> updatedFields = FluentIterable.from(newFields).uniqueIndex(new Function<Field, String>(){
      @Override
      public String apply(Field input) {
        return input.getName().toLowerCase();
      }});

    if(selection.isAlwaysValid()){
      // don't reorder a schema that is a select all.
      return new BatchSchema(selectionVectorMode, newFields);
    }

    Set<String> requestedTopLevelFields = new HashSet<>();
    List<Field> updatedFieldList = new ArrayList<>();
    for(BasePath p : schemaPaths){
      final String name = p.getRootSegment().getPath().toLowerCase();
      if(requestedTopLevelFields.add(name)){
        Field f = Preconditions.checkNotNull(updatedFields.get(name), "The projected column %s was not found in the schema to be masked: %s with a mask of %s.", name, this, schemaPaths);
        updatedFieldList.add(f);
      }
    }

    Preconditions.checkArgument(updatedFieldList.size() == newFields.size(), "Expected reordered field list to use all %s fields, only used %s.", newFields.size(), updatedFieldList.size());

    return new BatchSchema(selectionVectorMode, updatedFieldList);
  }

  public BatchSchema removeNullFields() {
    return new BatchSchema(selectionVectorMode, removeNullFields(getFields()));
  }

  private List<Field> removeNullFields(List<Field> oldFields) {
    List<Field> newFields = new ArrayList<>();
    for (Field field : oldFields) {
      if (field.getFieldType().getType().getTypeID() == ArrowTypeID.Null) {
        continue;
      }
      List<Field> childern = removeNullFields(field.getChildren());
      if (field.getType().isComplex() && childern.isEmpty()) {
        continue;
      }
      newFields.add(new Field(field.getName(), field.getFieldType(), childern));
    }
    return newFields;
  }

  private static List<Field> maskFields(List<Field> fields, FieldSelection selection) {
    ImmutableList.Builder<Field> fieldsListBuilder = ImmutableList.builder();
    for (Field field : fields) {
      FieldSelection childSelection = selection.getChild(field.getName());
      if (!childSelection.isNeverValid()) {
        if (field.getType().getTypeID() == ArrowTypeID.List) {
          Field innerField = field.getChildren().get(0);
          List<Field> childFields = maskFields(innerField.getChildren(), selection.getChild(field.getName()));
          Field newInnerField = new Field(innerField.getName(), new FieldType(innerField.isNullable(), innerField.getType(), null), childFields);
          fieldsListBuilder.add(new Field(field.getName(),  new FieldType(field.isNullable(), field.getType(), null), Collections.singletonList(newInnerField)));
        } else {
          List<Field> childFields = maskFields(field.getChildren(), selection.getChild(field.getName()));
          fieldsListBuilder.add(new Field(field.getName(),  new FieldType(field.isNullable(), field.getType(), null), childFields));
        }
      }
    }
    return fieldsListBuilder.build();
  }

  @Override
  public BatchSchema clone() {
    return cloneWithFields(Collections.<Field>emptyList());
  }

  public BatchSchema cloneWithFields(List<Field> fields) {
    List<Field> newFields = Lists.newArrayList();
    newFields.addAll(getFields());
    newFields.addAll(fields);
    return new BatchSchema(selectionVectorMode, newFields);
  }



  public BatchSchema clone(SelectionVectorMode mode) {
    List<Field> newFields = Lists.newArrayList();
    newFields.addAll(getFields());
    return new BatchSchema(mode, newFields);
  }

  public String toStringVerbose() {
    return toString(getFields());
  }

  public static String toString(List<Field> fields) {
    StringBuilder b = new StringBuilder();
    for (Field field : fields) {
      toString(field, 0, b);
    }
    return b.toString();
  }

  public static void toString(Field field, int depth, StringBuilder b) {
    b.append("\n");
    for (int i = 0; i < depth; i++) {
      b.append(" ");
    }
    b.append(field.getName());
    b.append(";");
    b.append(field.isNullable());
    b.append(";");
    b.append(Describer.describe(field.getType()));
    for (Field child : field.getChildren()) {
      toString(child, depth + 1, b);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    sb.append("schema(");
    for(Field f : getFields()){
      if(!first){
        sb.append(", ");
      }else{
        first = false;
      }
      sb.append(Describer.describe(f));
    }

    if(selectionVectorMode != SelectionVectorMode.NONE){
      sb.append(" SelectionVectorMode::");
      sb.append(selectionVectorMode.name());
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Output the minimal schema info as JSON. This is different from the default serialization supported by {@link Field}
   * as it has lot more information (such as vector types) which is not needed for understanding the schema.
   * @return
   * @throws Exception
   */
  public String toJSONString() throws Exception {
    final JsonFactory factory = new JsonFactory();
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final JsonGenerator jsonGenerator = factory.createGenerator(outputStream);

    toJSONString("root", null, getFields(), jsonGenerator);

    jsonGenerator.flush();
    return outputStream.toString();
  }

  private void toJSONString(String name, ArrowTypeID typeID, List<Field> children, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeStartObject();
    jsonGenerator.writeFieldName("name");
    jsonGenerator.writeString(name);

    if (typeID != null) {
      jsonGenerator.writeFieldName("type");
      jsonGenerator.writeString(typeID.name());
    }

    if (children != null && children.size() > 0) {
      jsonGenerator.writeFieldName("children");
      jsonGenerator.writeStartArray(children.size());
      for(Field child : children) {
        toJSONString(child.getName(), child.getType().getTypeID(), child.getChildren(), jsonGenerator);
      }
      jsonGenerator.writeEndArray();
    }
    jsonGenerator.writeEndObject();
  }

  public static enum SelectionVectorMode {
    NONE(-1, false), TWO_BYTE(2, true), FOUR_BYTE(4, true);

    public boolean hasSelectionVector;
    public final int size;
    SelectionVectorMode(int size, boolean hasSelectionVector) {
      this.size = size;
    }

    public static SelectionVectorMode[] DEFAULT = {NONE};
    public static SelectionVectorMode[] NONE_AND_TWO = {NONE, TWO_BYTE};
    public static SelectionVectorMode[] NONE_AND_FOUR = {NONE, FOUR_BYTE};
    public static SelectionVectorMode[] ALL = {NONE, TWO_BYTE, FOUR_BYTE};
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getFields() == null) ? 0 : getFields().hashCode());
    result = prime * result + ((selectionVectorMode == null) ? 0 : selectionVectorMode.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof BatchSchema)) {
      return false;
    }

    BatchSchema that = (BatchSchema) obj;
    return Objects.equals(this.getFields(), that.getFields()) &&
            Objects.equals(this.selectionVectorMode, that.selectionVectorMode);
  }

  private static java.util.function.Function<Field, Field> UPPERCASE_NAME = field ->
    new Field(field.getName().toUpperCase(), field.getFieldType(), field.getChildren());

  public boolean equalsIgnoreCase(Object obj) {
    if (!(obj instanceof BatchSchema)) {
      return false;
    }

    BatchSchema that = (BatchSchema) obj;

    BatchSchema thatUpperCaseFields = new BatchSchema(that.getSelectionVectorMode(), that.getFields().stream().map(UPPERCASE_NAME).collect(Collectors.toList()));
    BatchSchema thisUpperCaseFields = new BatchSchema(this.getSelectionVectorMode(), this.getFields().stream().map(UPPERCASE_NAME).collect(Collectors.toList()));

    return thisUpperCaseFields.equals(thatUpperCaseFields);
  }

  private boolean compareFields(List<Field> srcFields, List<Field> tgtFields) {
    if (srcFields == null && tgtFields == null) {
      return true;
    }
    if (srcFields == null || tgtFields == null) {
      return false;
    }
    if (srcFields.size() != tgtFields.size()) {
      return false;
    }
    Map<String, Field> srcChildrenFields = new HashMap<>();
    for(Field srcField: srcFields) {
      srcChildrenFields.put(srcField.getName().toLowerCase(), srcField);
    }

    for(Field tgtChildField: tgtFields) {
      Field srcChildField = srcChildrenFields.get(tgtChildField.getName().toLowerCase());
      if (srcChildField == null) {
        return false;
      }
      if (!compareField(srcChildField, tgtChildField)) {
        return false;
      }
    }

    return true;
  }

  private boolean compareField(Field src, Field tgt) {
    Preconditions.checkArgument(src !=null && tgt != null, "Unexpected state");
    if(!src.getName().toLowerCase().equalsIgnoreCase(tgt.getName().toLowerCase())) {
      return false;
    }

    boolean typesEqual;
    CompleteType srcCompleteType = CompleteType.fromField(src);
    CompleteType tgtCompleteType = CompleteType.fromField(tgt);
    if(srcCompleteType.isUnion() && tgtCompleteType.isUnion()) {
      return compareFields(srcCompleteType.getChildren(), tgtCompleteType.getChildren());
    } else {
      typesEqual = Objects.equals(src.getType(), tgt.getType());
    }
    if(!Objects.equals(src.isNullable(), tgt.isNullable()) ||
      !typesEqual ||
      !Objects.equals(src.getDictionary(), tgt.getDictionary()) ||
      !Objects.equals(src.getMetadata(), tgt.getMetadata())) {
      return false;
    }
    return compareFields(src.getChildren(), tgt.getChildren());
  }

  public boolean equalsTypesWithoutPositions(BatchSchema that) {
    return compareFields(this.getFields(), that.getFields()) && Objects.equals(this.selectionVectorMode, that.selectionVectorMode);
  }

  public boolean equalsTypesAndPositions(BatchSchema schema){
    List<CompleteType> typesA = FluentIterable.from(getFields()).transform(TO_TYPES).toList();
    List<CompleteType> typesB = FluentIterable.from(schema.getFields()).transform(TO_TYPES).toList();
    return typesA.equals(typesB);
  }

  private static final Function<Field, CompleteType> TO_TYPES = new Function<Field, CompleteType>() {
    @Override
    public CompleteType apply(Field input) {
      return CompleteType.fromField(input);
    }
  };

  public byte[] serialize() {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    builder.finish(serialize(builder));
    return builder.sizedByteArray();
  }

  public ByteString toByteString(){
    return ByteString.copyFrom(serialize());
  }

  public static BatchSchema deserialize(byte[] bytes) {
    Schema schema = Schema.getRootAsSchema(ByteBuffer.wrap(bytes));
    org.apache.arrow.vector.types.pojo.Schema s = org.apache.arrow.vector.types.pojo.Schema.convertSchema(schema);
    return new BatchSchema(SelectionVectorMode.NONE, s.getFields());
  }

  public static BatchSchema deserialize(ByteString bytes) {
    Schema schema = Schema.getRootAsSchema(bytes.asReadOnlyByteBuffer());
    org.apache.arrow.vector.types.pojo.Schema s = org.apache.arrow.vector.types.pojo.Schema.convertSchema(schema);
    return new BatchSchema(SelectionVectorMode.NONE, s.getFields());
  }

  public int serialize(FlatBufferBuilder builder) {
    Preconditions.checkArgument(selectionVectorMode == SelectionVectorMode.NONE,
        "Serialization is only allowed for SelectionVectorMode.NONE. This was in SelectionVectorMode.%s", selectionVectorMode.name());
    org.apache.arrow.vector.types.pojo.Schema schema = new org.apache.arrow.vector.types.pojo.Schema(getFields());
    return schema.getSchema(builder);
  }

  public static class De extends JsonDeserializer<BatchSchema> {

    @Override
    public BatchSchema deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      return BatchSchema.deserialize(p.getBinaryValue());
    }

  }

  public static class Ser extends JsonSerializer<BatchSchema> {
    @Override
    public void serialize(BatchSchema value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException, JsonProcessingException {
      gen.writeBinary(value.serialize());
    }

  }


  public void materializeVectors(List<? extends BasePath> columns, OutputMutator mutator) {
    Preconditions.checkNotNull(columns, "A scan's column selection cannot be null.");
    Set<String> selectedColumns = new HashSet<>();
    for (BasePath sp : columns) {
      selectedColumns.add(sp.getRootSegment().getNameSegment().getPath());
    }
    for (Field field : this) {
      if (columns != null && !selectedColumns.contains("*") && !selectedColumns.contains(field.getName())) {
        continue;
      }
      mutator.addField(field, CompleteType.fromField(field).getValueVectorClass());
    }
  }

  /**
   * Find an estimated size of the vectors. Currently we assume constant size for variable length columns.
   */
  public static int estimateRecordSize(Map<String, ValueVector> vectorMap, int listSizeEstimate, int varFieldSizeEstimate) {
    int estimatedRecordSize = 0;
    for (final ValueVector v : vectorMap.values()) {
      estimatedRecordSize += BatchSchema.estimateFieldSize(v.getField(), listSizeEstimate, varFieldSizeEstimate);
    }
    return estimatedRecordSize;
  }

  /**
   * Find an estimated size of the record. Currently we assume constant size for variable length columns.
   * @return
   */
  public int estimateRecordSize(int listSizeEstimate, int varFieldSizeEstimate) {
    // calculate the target record size
    int estimatedRecordSize = 0;
    for(Field column : this) {
      estimatedRecordSize += estimateFieldSize(column, listSizeEstimate, varFieldSizeEstimate);
    }

    return estimatedRecordSize;
  }

  private static int estimateFieldSize(Field field, int listSizeEstimate, int varFieldSizeEstimate) {
    ArrowTypeID typeID = field.getType().getTypeID();
    final int estimatedFieldSize;
    switch (typeID) {
      case Int:
        estimatedFieldSize = ((Int) field.getType()).getBitWidth() / 8;
        break;
      case FloatingPoint:
        if (((FloatingPoint) field.getType()).getPrecision() == FloatingPointPrecision.DOUBLE) {
          estimatedFieldSize = 8;
        } else {
          estimatedFieldSize = 4;
        }
        break;
      case Struct:
        int childrenSize = 0;
        for(Field child : field.getChildren()) {
          childrenSize += estimateFieldSize(child, listSizeEstimate, varFieldSizeEstimate);
        }
        estimatedFieldSize = childrenSize;
        break;
      case List:
        // assume an average of 5 elements in a list.
        int elemSize = estimateFieldSize(field.getChildren().get(0), listSizeEstimate, varFieldSizeEstimate);
        estimatedFieldSize = elemSize * listSizeEstimate;
        break;
      case FixedSizeList:
        final int fixedListSize = ((FixedSizeList)field.getType()).getListSize();
        elemSize = estimateFieldSize(field.getChildren().get(0), listSizeEstimate, varFieldSizeEstimate);
        estimatedFieldSize =  elemSize * fixedListSize;
        break;
      case Union:
        // Take average of fields in a union
        if (field.getChildren().size() > 0) {
          int size = 0;
          for(Field child : field.getChildren()) {
            size += estimateFieldSize(child, listSizeEstimate, varFieldSizeEstimate);
          }
          estimatedFieldSize = size / field.getChildren().size();
        } else {
          estimatedFieldSize = 0;
        }
        break;
      case Utf8:
      case Binary:
        // large size constant number
        estimatedFieldSize = varFieldSizeEstimate;
        break;
      case Bool:
        estimatedFieldSize = 1;
        break;
      case Decimal:
        estimatedFieldSize = 16;
        break;
      case Date:
        estimatedFieldSize = 8;
        break;
      case Time:
        estimatedFieldSize = 4;
        break;
      case Timestamp:
        estimatedFieldSize = 8;
        break;
      case Interval:
        estimatedFieldSize = 8;
        break;
      default:
        estimatedFieldSize = 4;
    }

    return estimatedFieldSize;
  }


  public int getTotalFieldCount(){
    int count = 0;
    for(Field f : this){
      count = countFields(f, count);
    }
    return count;
  }

  private static int countFields(Field f, int count){
    if(f.getChildren().isEmpty()){
      count++;
    }else {
      for(Field child : f.getChildren()){
        count = countFields(child, count);
      }
    }
    return count;
  }

  public boolean isDeprecatedText(){
    if(getFieldCount() != 1){
      return false;
    }

    final Field f = getFields().get(0);
    if(!"columns".equals(f.getName())){
      return false;
    }

    CompleteType type = CompleteType.fromField(f);
    if(!type.isList()){
      return false;
    }

    CompleteType child = CompleteType.fromField(type.getChildren().get(0));
    if(child.isVariableWidthScalar()){
      return true;
    }

    return false;
  }

  public BatchSchema handleUnions(SupportsTypeCoercionsAndUpPromotions coercionRulesSet) {
    if (hasUnions()) {
      return mergeWithUpPromotion(this, coercionRulesSet);
    }
    return this;
  }

  public boolean hasUnions() {
    return hasUnions(getFields());
  }

  public static boolean hasUnions(List<Field> fields) {
    for (Field field : fields) {
      if (field.getType().getTypeID() == ArrowTypeID.Union || hasUnions(field.getChildren())) {
        return true;
      }
    }
    return false;
  }

  public BatchSchema merge(BatchSchema schemaToMergeIntoThis){
    List<Field> original = ImmutableList.copyOf(this);
    List<Field> newlyObserved = ImmutableList.copyOf(schemaToMergeIntoThis);
    return new BatchSchema(SelectionVectorMode.NONE, mergeFieldLists(original, newlyObserved));
  }

  public BatchSchema mergeWithUpPromotion(BatchSchema fileSchema, SupportsTypeCoercionsAndUpPromotions coercionRulesSet) {
    List<Field> fileFields = ImmutableList.copyOf(fileSchema);
    return new BatchSchema(SelectionVectorMode.NONE, mergeWithUpPromotion(fileFields, coercionRulesSet));
  }

  //newFields.mergeWithRetainOld(oldFields)
  //Should add only those fields to newFields which are not in the newFields
  public BatchSchema mergeWithRetainOld(BatchSchema toMerge){
    Map<String,Field> alreadyExisting = new LinkedHashMap<>();
    for (Field field : getFields()) {
      String dottedName = field.getName().toLowerCase();
      Field temp = field;
      while(!temp.getChildren().isEmpty()) {
        dottedName = dottedName.concat("." + temp.getChildren().get(0).getName().toLowerCase());
        temp = temp.getChildren().get(0);
      }
      alreadyExisting.put(dottedName, field);
    }

    Map<String,Field> newFields = new LinkedHashMap<>();
    for (Field field : toMerge.getFields()) {
      String dottedName = field.getName().toLowerCase();
      Field temp = field;
      while(!temp.getChildren().isEmpty()) {
        dottedName = dottedName.concat("." + temp.getChildren().get(0).getName().toLowerCase());
        temp = temp.getChildren().get(0);
      }
      newFields.put(dottedName, field);
    }

    List<Field> addedFields = Maps.difference(newFields, alreadyExisting).entriesOnlyOnLeft().values().stream().collect(Collectors.toList());
    addedFields.addAll(getFields());
    return new BatchSchema(addedFields);
  }

  public BatchSchema difference(BatchSchema schema) {
    Map<String,Field> alreadyExisting = new LinkedHashMap<>();
    for (Field field : getFields()) {
      String dottedName = field.getName().toLowerCase();
      Field temp = field;
      while(!temp.getChildren().isEmpty()) {
        dottedName = dottedName.concat("." + temp.getChildren().get(0).getName().toLowerCase());
        temp = temp.getChildren().get(0);
      }
      alreadyExisting.put(dottedName, field);
    }

    if(alreadyExisting.isEmpty()) {
      return BatchSchema.EMPTY;
    }

    Map<String,Field> newFields = new LinkedHashMap<>();
    for (Field field : schema.getFields()) {
      String dottedName = field.getName().toLowerCase();
      Field temp = field;
      while(!temp.getChildren().isEmpty()) {
        dottedName = dottedName.concat("." + temp.getChildren().get(0).getName().toLowerCase());
        temp = temp.getChildren().get(0);
      }
      newFields.put(dottedName, field);
    }

    List<Field> addedFields = Maps.difference(alreadyExisting, newFields).entriesOnlyOnLeft().values().stream().collect(Collectors.toList());
    return new BatchSchema(addedFields);
  }

  private List<Field> mergeWithUpPromotion(List<Field> fileFields, SupportsTypeCoercionsAndUpPromotions coercionRulesSet) {
    return CompleteType.mergeFieldListsWithUpPromotionOrCoercion(ImmutableList.copyOf(this), fileFields, coercionRulesSet);
  }

  private static List<Field> mergeFieldLists(List<Field> original, List<Field> newlyObserved) {
    Map<String,Field> secondFieldMap = new LinkedHashMap<>();
    List<Field> mergedList = new ArrayList<>();
    for (Field field : original) {
      secondFieldMap.put(field.getName().toLowerCase(), field);
    }
    for (Field field : newlyObserved) {
      Field matchingField = secondFieldMap.remove(field.getName().toLowerCase());
      if (matchingField != null) {
        CompleteType mergedType = null;
        CompleteType type1 = CompleteType.fromField(field);
        CompleteType type2 = CompleteType.fromField(matchingField);
        try {
          mergedType = type1.merge(type2);
        } catch (UnsupportedOperationException e) {
          StringBuilder stringBuilder = new StringBuilder("Mixed types ");
          stringBuilder.append(type1).append(" , ").append(type2).append(" for field ").append
            (field.getName()).append(" are not supported.");
          throw UserException.unsupportedError().message(stringBuilder.toString()).build(logger);
        }
        mergedList.add(mergedType.toField(field.getName()));
      } else {
        mergedList.add(field);
      }
    }
    for (Field field : secondFieldMap.values()) {
      mergedList.add(field);
    }
    return mergedList;
  }

  public static BatchSchema of(Field...fields) {
    return new BatchSchema(SelectionVectorMode.NONE, ImmutableList.copyOf(fields));
  }

  public java.util.Optional<Field> findFieldIgnoreCase(String fieldName) {
    return this.getFields().stream()
      .filter(field -> field.getName().equalsIgnoreCase(fieldName))
      .findFirst();
  }

  public BatchSchema applyUserDefinedSchemaAfterSchemaLearning(BatchSchema newSchema,
                                                                                      List<Field> droppedColumns,
                                                                                      List<Field> updatedColumns,
                                                                                      boolean isSchemaLearningDisabledByUser,
                                                                                      boolean isUserDefinedSchemaEnabled,
                                                                                      String filePath,
                                                                                      List<String> tableSchemaPath,
                                                                                      SupportsTypeCoercionsAndUpPromotions coercionRulesSet) {
    if (isUserDefinedSchemaEnabled && isSchemaLearningDisabledByUser) {
      return this;
    }
    BatchSchema finalSchema;
    try {
      finalSchema = this.mergeWithUpPromotion(newSchema, coercionRulesSet);
      for (Field field : droppedColumns) {
        finalSchema = finalSchema.dropField(field);
      }
      for (Field field : updatedColumns) {
        finalSchema = finalSchema.changeTypeRecursive(field);
      }
    } catch (NoSupportedUpPromotionOrCoercionException e) {
      e.addFilePath(filePath);
      e.addDatasetPath(tableSchemaPath);
      throw UserException.unsupportedError().message(e.getMessage()).build(logger);
    }
    return finalSchema.removeNullFields();
  }

  /**
   * returns Optional<BatchSchema> with subset of fields in the order they appear in fieldNames if fieldNames is not empty
   * returns Optional.empty() if fieldNames is empty
   * throws error if a field is not in schema
   * @param fieldNames
   * @return
   */
  public java.util.Optional<BatchSchema> subset(List<String> fieldNames) {
    if (fieldNames.isEmpty()) {
      return Optional.empty();
    }
    Set<String> missingColumns = new HashSet<>();

    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();

    fieldNames.forEach(f -> {
      Optional<Field> fieldInTable = this.findFieldIgnoreCase(f);
      if (fieldInTable.isPresent()) {
        schemaBuilder.addField(fieldInTable.get());
      } else {
        missingColumns.add(f);
      }
    });

    if (!missingColumns.isEmpty()) {
      throw UserException.validationError()
        .message("Specified column(s) %s not found in schema.", missingColumns).buildSilently();
    }
    return Optional.of(schemaBuilder.build());
  }

  public BatchSchema dropFields(List<List<String>> pathsToDrop) {
    //drop the field here and return the dropped field
    BatchSchema newSchema = new BatchSchema(getFields());
    for(List<String> path : pathsToDrop) {
      newSchema = newSchema.dropField(path);
    }
    return newSchema;
  }

  public BatchSchema addColumns(List<Field> columnsToAdd) {
    //drop the field here and return the dropped field
    Map<String,Field> originalFieldMap = new LinkedHashMap<>();
    for (Field field : getFields()) {
      originalFieldMap.put(field.getName().toLowerCase(), field);
    }

    BatchSchema newSchema = new BatchSchema(getFields());
    for(Field fieldToAdd : columnsToAdd) {
      addFieldToSchema(originalFieldMap, fieldToAdd);
    }
    return new BatchSchema(originalFieldMap.values().stream().collect(Collectors.toList()));
  }

  public BatchSchema addColumn(Field newField) {
    Map<String,Field> originalFieldMap = new LinkedHashMap<>();
    for (Field field : getFields()) {
      originalFieldMap.put(field.getName().toLowerCase(), field);
    }
    addFieldToSchema(originalFieldMap, newField);
    return new BatchSchema(originalFieldMap.values().stream().collect(Collectors.toList()));
  }

  private void addFieldToSchema(Map<String,Field> originalFieldMap, Field newField) {
    List<String> fieldPaths = new ArrayList<>();
    Field tempField = newField;
    fieldPaths.add(tempField.getName().toLowerCase());
    while(!tempField.getChildren().isEmpty()) {
      fieldPaths.add(tempField.getChildren().get(0).getName().toLowerCase());
      tempField = tempField.getChildren().get(0);
    }
    Field field = originalFieldMap.get(newField.getName().toLowerCase());

    if(!newField.getType().isComplex() || !originalFieldMap.containsKey(fieldPaths.get(0))) {
      originalFieldMap.put(fieldPaths.get(0), newField);
    } else {
      //if complex check for existence first
      Field newType = new Field(field.getName(), field.getFieldType(), addComplexTypes(field.getChildren(), fieldPaths, 1, newField.getChildren().get(0)));
      originalFieldMap.replace(fieldPaths.get(0), newType);
    }
  }

  private BatchSchema dropField(List<String> path) {
    path = path.stream().map(String::toLowerCase)
      .collect(Collectors.toList());

    Map<String,Field> originalFieldMap = new LinkedHashMap<>();
    for (Field field : getFields()) {
      originalFieldMap.put(field.getName().toLowerCase(), field);
    }

    Field field = originalFieldMap.get(path.get(0));

    if(field == null) {
      return new BatchSchema(originalFieldMap.values().stream().collect(Collectors.toList()));
    }

    if(path.size() == 1) {
      originalFieldMap.remove(path.get(0));
    } else {
      Field newType = new Field(field.getName(), field.getFieldType(), removeComplexTypes(field.getChildren(), path, 1));
      originalFieldMap.replace(path.get(0), newType);
    }
    return new BatchSchema(originalFieldMap.values().stream().collect(Collectors.toList()));
  }

  public BatchSchema changeTypeRecursive(Field newType) {
    return changeType(newType, true);
  }

  public BatchSchema changeTypeTopLevel(Field newType) {
    return changeType(newType, false);
  }

  public BatchSchema changeType(Field newType, boolean isRecursive) {
    Map<String,Field> originalFieldMap = new LinkedHashMap<>();
    for (Field field : getFields()) {
      originalFieldMap.put(field.getName().toLowerCase(), field);
    }

    if(!isRecursive) {
      originalFieldMap.replace(newType.getName().toLowerCase(), newType);
    } else {
      changeTypeOfField(originalFieldMap, newType);
    }

    return new BatchSchema(originalFieldMap.values().stream().collect(Collectors.toList()));
  }

  private void changeTypeOfField(Map<String,Field> originalFieldMap, Field newField) {
    if(!newField.getType().isComplex() ||
      (newField.getType().isComplex() &&
        originalFieldMap.get(newField.getName()) != null &&
        !originalFieldMap.get(newField.getName()).getType().isComplex())
    ) {
      originalFieldMap.replace(newField.getName().toLowerCase(), newField);
      return;
    }

    Field field = originalFieldMap.get(newField.getName().toLowerCase());
    Map<String,Field> childFieldMap = new LinkedHashMap<>();

    if(field == null) {
      return;
    }

    for (Field child: field.getChildren()){
      childFieldMap.put(child.getName().toLowerCase(), child);
    }

    for (Field newChild: newField.getChildren()) {
      changeTypeOfField(childFieldMap, newChild);
    }

    Field newFinalType = new Field(field.getName(), newField.getFieldType(), childFieldMap.values().stream().collect(Collectors.toList()));
    originalFieldMap.replace(field.getName().toLowerCase(), newFinalType);
  }

  public BatchSchema dropField(Field field) {
    //drop the field here and return the new schema
    BatchSchema newSchema = new BatchSchema(getFields());
    List<String> fieldPaths = new ArrayList<>();
    fieldPaths.add(field.getName());
    while(!field.getChildren().isEmpty()) {
      Preconditions.checkArgument(field.getChildren().size() == 1, "Cannot drop a field with more than once children");
      fieldPaths.add(field.getChildren().get(0).getName());
      field = field.getChildren().get(0);
    }
    return dropField(fieldPaths);
  }

  public BatchSchema dropField(String name) {
    name = name.toLowerCase();
    Map<String,Field> originalFieldMap = new LinkedHashMap<>();
    for (Field field : getFields()) {
      originalFieldMap.put(field.getName().toLowerCase(), field);
    }
    originalFieldMap.remove(name);
    return new BatchSchema(originalFieldMap.values().stream().collect(Collectors.toList()));
  }

  private List<Field> removeComplexTypes(List<Field> fields, List<String> nameSegments, int index) {
    String name = nameSegments.get(index);
    List<Field> newFieldList = new ArrayList<>(fields);
    if(index == nameSegments.size() - 1) {
      //last segment. Remove from the parent and return the field
      for(Field f : fields) {
        if(f.getName().equalsIgnoreCase(name)) {
          newFieldList.remove(f);
          return newFieldList;
        }
      }
    }

    for(int i = 0; i < fields.size(); i++) {
      if(fields.get(i).getName().equalsIgnoreCase(name)) {
        // the field to be manipulated
        Field originalField = fields.get(i);
        Field newField = new Field(originalField.getName(), originalField.getFieldType(),
          removeComplexTypes(originalField.getChildren(), nameSegments, ++index));
        newFieldList.set(i, newField);
      }
    }
    return newFieldList;
  }

  private List<Field> addComplexTypes(List<Field> fields, List<String> nameSegments, int index, Field finalType) {
    String name = nameSegments.get(index);
    List<Field> newFieldList = new ArrayList<>(fields);
    if(index == nameSegments.size() - 1) {
      //last segment.
      newFieldList.add(finalType);
      return newFieldList;
    }

    for(int i = 0; i < fields.size(); i++) {
      if(fields.get(i).getName().equalsIgnoreCase(name)) {
        // the field to be manipulated
        Field originalField = fields.get(i);
        Field newField = new Field(originalField.getName(), originalField.getFieldType(),
          addComplexTypes(originalField.getChildren(), nameSegments, ++index, finalType.getChildren().get(0)));
        newFieldList.set(i, newField);
      }
    }
    return newFieldList;
  }
}
