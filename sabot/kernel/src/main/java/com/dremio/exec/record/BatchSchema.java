/*
 * Copyright (C) 2017 Dremio Corporation
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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.vector.complex.FieldIdUtil2;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.planner.sql.TypeInferenceUtils;
import com.dremio.exec.vector.complex.fn.FieldSelection;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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
  public static final BatchSchema SCHEMA_UNKNOWN_NO_DATA = BatchSchema.newBuilder().addField(new Field(SCHEMA_UNKNOWN_NO_DATA_COLNAME, true, new ArrowType.Utf8(), null)).build();
  public static final UnknownSchema UNKNOWN_SCHEMA_OBJECT = new UnknownSchema();

  public final static class UnknownSchema {
    public final String NO_DATA;
    private UnknownSchema() {
      this.NO_DATA = null;
    }
  }

  private final SelectionVectorMode selectionVectorMode;

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

  public TypedFieldId getFieldId(SchemaPath path) {
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

  @Override
  public Iterator<Field> iterator() {
    return getFields().iterator();
  }

  public SelectionVectorMode getSelectionVectorMode() {
    return selectionVectorMode;
  }

  /**
   * Masks an existing schema based on the provided schemapath. Additionally, reorders the schema to match the requested schema path.
   * @param schemaPath
   * @return
   */
  public BatchSchema maskAndReorder(List<SchemaPath> schemaPaths){
    return mask(schemaPaths, true);
  }

  public BatchSchema mask(List<SchemaPath> schemaPaths, boolean reorder){
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
    for(SchemaPath p : schemaPaths){
      final String name = p.getRootSegment().getPath().toLowerCase();
      if(requestedTopLevelFields.add(name)){
        Field f = Preconditions.checkNotNull(updatedFields.get(name), "The projected column %s was not found in the schema to be masked: %s with a mask of %s.", name, this.toString(), schemaPaths);
        updatedFieldList.add(f);
      }
    }

    Preconditions.checkArgument(updatedFieldList.size() == newFields.size(), "Expected reordered field list to use all %d fields, only used %d.", newFields.size(), updatedFieldList.size());

    return new BatchSchema(selectionVectorMode, updatedFieldList);
  }

  private static List<Field> maskFields(List<Field> fields, FieldSelection selection) {
    ImmutableList.Builder<Field> fieldsListBuilder = ImmutableList.builder();
    for (Field field : fields) {
      FieldSelection childSelection = selection.getChild(field.getName());
      if (!childSelection.isNeverValid()) {
        if (field.getType().getTypeID() == ArrowTypeID.List) {
          Field innerField = field.getChildren().get(0);
          List<Field> childFields = maskFields(innerField.getChildren(), selection.getChild(field.getName()));
          Field newInnerField = new Field(innerField.getName(), innerField.isNullable(), innerField.getType(), childFields);
          fieldsListBuilder.add(new Field(field.getName(), field.isNullable(), field.getType(), Collections.singletonList(newInnerField)));
        } else {
          List<Field> childFields = maskFields(field.getChildren(), selection.getChild(field.getName()));
          fieldsListBuilder.add(new Field(field.getName(), field.isNullable(), field.getType(), childFields));
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


  public RelDataType toCalciteRecordType(RelDataTypeFactory factory, Set<String> fieldBlacklist){
    FieldInfoBuilder builder = new FieldInfoBuilder(factory);
    for(Field f : this) {
      if(!fieldBlacklist.contains(f.getName())){
        builder.add(f.getName(), CompleteType.toCalciteType(f, factory));
      }
    }
    RelDataType rowType = builder.build();
    if(rowType.getFieldCount() == 0){
      throw UserException.dataReadError().message("Selected table has no columns.").build(logger);
    }

    return rowType;
  }

  public RelDataType toCalciteRecordType(RelDataTypeFactory factory){
    return toCalciteRecordType(factory, ImmutableSet.<String>of());
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

  public boolean equalsTypesAndPositions(BatchSchema schema){
    List<CompleteType> typesA = FluentIterable.from(getFields()).transform(TO_TYPES).toList();
    List<CompleteType> typesB = FluentIterable.from(schema.getFields()).transform(TO_TYPES).toList();
    return typesA.equals(typesB);
  }

  private final static Function<Field, CompleteType> TO_TYPES = new Function<Field, CompleteType>() {
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

  public static BatchSchema fromDataset(DatasetConfig config){
    try{
      ByteString bytes = DatasetHelper.getSchemaBytes(config);
      if(bytes == null){
        throw new IllegalStateException(String.format("Schema is currently unavailable for dataset %s.", PathUtils.constructFullPath(config.getFullPathList())));
      }
      return deserialize(bytes);
    }catch(Exception ex){
      throw new IllegalStateException(String.format("Schema for dataset %s is corrupt.", PathUtils.constructFullPath(config.getFullPathList())), ex);
    }
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

  public static BatchSchema fromCalciteRowType(final RelDataType relDataType){
    Preconditions.checkArgument(relDataType.isStruct());

    SchemaBuilder builder = BatchSchema.newBuilder();
    for (Map.Entry<String,RelDataType> field : relDataType.getFieldList()) {
      MinorType minorType = TypeInferenceUtils.getMinorTypeFromCalciteType(field.getValue());
      if (minorType != null) {
        Field f = MajorTypeHelper.getFieldForNameAndMajorType(field.getKey(), Types.optional(minorType));
        builder.addField(f);
      }
    }
    return builder.build();
  }

  public static BatchSchema fromCalciteRowTypeJson(final RelDataType relDataType){
    Preconditions.checkArgument(relDataType.isStruct());

    SchemaBuilder builder = BatchSchema.newBuilder();
    for (Map.Entry<String,RelDataType> field : relDataType.getFieldList()) {
      MinorType minorType = TypeInferenceUtils.getMinorTypeFromCalciteType(field.getValue());
      if (minorType != null) {

        // if we're using json/rels reader, the types are going to be larger than typical.
        if(minorType == MinorType.INT){
          minorType = MinorType.BIGINT;
        }

        if(minorType == MinorType.FLOAT4){
          minorType = MinorType.FLOAT8;
        }
        Field f = MajorTypeHelper.getFieldForNameAndMajorType(field.getKey(), Types.optional(minorType));
        builder.addField(f);
      }
    }
    return builder.build();
  }

  public void materializeVectors(List<SchemaPath> columns, OutputMutator mutator) {
    Preconditions.checkNotNull(columns, "A scan's column selection cannot be null.");
    Set<String> selectedColumns = new HashSet<>();
    for (SchemaPath sp : columns) {
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
   * Find an estimated size of the record. Currently we assume constant size for variable length columns.
   * @return
   */
  public int estimateRecordSize() {
    // calculate the target record size
    int estimatedRecordSize = 0;
    for(Field column : this) {
      estimatedRecordSize += estimateFieldSize(column);
    }

    return estimatedRecordSize;
  }

  private static int estimateFieldSize(Field field) {
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
          childrenSize += estimateFieldSize(child);
        }
        estimatedFieldSize = childrenSize;
        break;
      case List:
        // assume an average of 5 elements in a list.
        estimatedFieldSize = estimateFieldSize(field.getChildren().get(0)) * 5;
        break;
      case FixedSizeList:
        final int fixedListSize = ((FixedSizeList)field.getType()).getListSize();
        estimatedFieldSize = estimateFieldSize(field.getChildren().get(0)) * fixedListSize;
        break;
      case Union:
        // Take average of fields in a union
        if (field.getChildren().size() > 0) {
          int size = 0;
          for(Field child : field.getChildren()) {
            size = estimateFieldSize(child);
          }
          estimatedFieldSize = size / field.getChildren().size();
        } else {
          estimatedFieldSize = 0;
        }
        break;
      case Utf8:
      case Binary:
        // large size constant number
        estimatedFieldSize = 15;
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

  public BatchSchema merge(BatchSchema schemaToMergeIntoThis){
    List<Field> original = ImmutableList.copyOf(this);
    List<Field> newlyObserved = ImmutableList.copyOf(schemaToMergeIntoThis);
    return new BatchSchema(SelectionVectorMode.NONE, mergeFieldLists(original, newlyObserved));
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
        CompleteType mergedType = CompleteType.fromField(field).merge(CompleteType.fromField(matchingField));
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
}
