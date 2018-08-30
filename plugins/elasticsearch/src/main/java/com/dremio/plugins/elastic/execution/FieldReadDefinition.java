/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.vector.complex.fn.WorkingBuffer;
import com.dremio.plugins.elastic.DateFormats;
import com.dremio.plugins.elastic.DateFormats.FormatterAndType;
import com.dremio.plugins.elastic.ElasticsearchConstants;
import com.dremio.plugins.elastic.execution.WriteHolders.WriteHolder;
import com.dremio.plugins.elastic.mapping.FieldAnnotation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

/**
 * Describes how we want to read each field. Everything we need to read a field
 * in Elastic should be provided by this file.
 */
public class FieldReadDefinition {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FieldReadDefinition.class);

  private final String name;
  private final boolean isList;
  private final CompleteType type;
  private final boolean geo;
  private final ImmutableMap<String, FieldReadDefinition> children;
  private final WriteHolder holder;
  private final FieldReadDefinition noList;
  private final boolean hidden;

  /**
   * Constructor for hidden fields.
   * @param hidden
   */
  private FieldReadDefinition(String name, boolean hidden){
    assert hidden;
    this.type = null;
    this.name = name;
    this.isList = false;
    this.geo = false;
    this.holder = null;
    this.children = null;
    this.hidden = true;
    this.noList = null;
  }

  private FieldReadDefinition(
      SchemaPath path,
      final String name,
      CompleteType type,
      boolean isList,
      boolean geo,
      WriteHolder holder,
      ImmutableMap<String, FieldReadDefinition> children
      ) {
    super();
    this.type = type;
    this.name = name;
    this.isList = isList;
    this.geo = geo;
    this.holder = holder;
    this.children = children;
    this.hidden = false;
    if(isList){
      noList = new FieldReadDefinition(path, name, type, false, geo, holder, children);
    } else {
      noList = null;
    }
  }

  public boolean isHidden(){
    return hidden;
  }

  public FieldReadDefinition asNoList(){
    return isList ? noList : this;
  }

  public boolean isGeo(){
    return geo;
  }

  public boolean isArray(){
    return isList;
  }

  public FieldReadDefinition getChild(String name){
    return children.get(name);
  }

  public void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException {
    if(parser.getValueAsString().length() != 0 || holder instanceof WriteHolders.VarCharWriteHolder || holder instanceof WriteHolders.VarBinaryWriteHolder) {
      holder.writeList(writer, token, parser);
    }
  }

  public void writeMap(StructWriter writer, JsonToken token, JsonParser parser) throws IOException {
    if(parser.getValueAsString().length() != 0 || holder instanceof WriteHolders.VarCharWriteHolder || holder instanceof WriteHolders.VarBinaryWriteHolder) {
      holder.writeMap(writer, token, parser);
    }
  }

  public static FieldReadDefinition getTree(
      final BatchSchema schema,
      final Map<SchemaPath, FieldAnnotation> annotationMap,
      final WorkingBuffer buffer){

    // for all hidden fields, create a list for each parent of the children where we should create field read definition that mark data as hidden.
    ImmutableListMultimap.Builder<SchemaPath, String> hiddenFieldsB = ImmutableListMultimap.builder();
    ImmutableList.Builder<String> topLevelHiddenFieldsB = ImmutableList.builder();
    for(Entry<SchemaPath, FieldAnnotation> e : annotationMap.entrySet()){
      if(e.getValue().isUnknown()){
        String name = e.getKey().getLastSegment().getNameSegment().getPath();
        if(e.getKey().getRootSegment().isLastPath()){
          topLevelHiddenFieldsB.add(name);
        } else {
          hiddenFieldsB.put(e.getKey().getParent(), name);
        }
      }
    }
    final ImmutableListMultimap<SchemaPath, String> hiddenFields = hiddenFieldsB.build();
    final ImmutableList<String> topLevelHiddenFields = topLevelHiddenFieldsB.build();

    return new FieldReadDefinition(null, null, null, false, false, new WriteHolders.InvalidWriteHolder(),
        FluentIterable.from(schema.getFields())

        .transform(new Function<Field, FieldReadDefinition>() {
          @Override
          public FieldReadDefinition apply(Field input) {
            return getDefinition(null, input, annotationMap, hiddenFields, buffer, false);
          }
        })

        // add hidden fields.
        .append(getHiddenFields(topLevelHiddenFields))

        .uniqueIndex(new Function<FieldReadDefinition, String>() {
          @Override
          public String apply(FieldReadDefinition input) {
            return input.name;
          }
        }));
  }

  private static Iterable<FieldReadDefinition> getHiddenFields(Iterable<String> names){
    return FluentIterable.from(names).transform(new Function<String, FieldReadDefinition>(){
      @Override
      public FieldReadDefinition apply(String input) {
        return new FieldReadDefinition(input, true);
      }});
  }

  private static FieldReadDefinition getDefinition(final SchemaPath parent, final Field field, final Map<SchemaPath, FieldAnnotation> annotations, final Multimap<SchemaPath, String> hiddenFields, final WorkingBuffer buffer, boolean incomingIsGeoShape){

    CompleteType type = CompleteType.fromField(field);
    final boolean isList = type.isList();
    if(isList){
      type = type.getOnlyChildType();
    }

    final SchemaPath path = parent == null ? SchemaPath.getSimplePath(field.getName()) : parent.getChild(field.getName());
    FieldAnnotation annotation = annotations.get(path);
    final boolean isGeoShape = incomingIsGeoShape || (annotation == null ? false : annotation.isGeoShape());
    final List<String> formats = annotation == null ? ImmutableList.<String>of() : annotation.getDateFormats();
    final WriteHolder holder = getWriteHolder(field.getName(), isList, type.toMinorType(), formats, path, buffer, isGeoShape);

    if(isGeoShape && type.isUnion() && ElasticsearchConstants.GEO_SHAPE_COORDINATES.equals(field.getName())){
      return new FieldReadDefinition(path, field.getName(), type, true, isGeoShape, holder, ImmutableMap.<String, FieldReadDefinition>of());
    }

    return new FieldReadDefinition(path, field.getName(), type, isList, isGeoShape, holder,
        FluentIterable.from(type.getChildren()).transform(new Function<Field, FieldReadDefinition>() {
          @Override
          public FieldReadDefinition apply(Field input) {
            return getDefinition(path, input, annotations, hiddenFields, buffer, isGeoShape);
          }
        }).append(getHiddenFields(hiddenFields.get(path)))
        .uniqueIndex(new Function<FieldReadDefinition, String>() {
          @Override
          public String apply(FieldReadDefinition input) {
            return input.name;
          }
        }));
  }

  private static WriteHolder getWriteHolder(String name, boolean isList, MinorType type, List<String> formats, SchemaPath path, WorkingBuffer buffer, boolean isGeoShape){
    final FormatterAndType[] formatterAndType;
    if(formats != null && !formats.isEmpty()){
      formatterAndType = FluentIterable.from(formats).transform(new Function<String, FormatterAndType>(){
      @Override
      public FormatterAndType apply(String input) {
        return DateFormats.getFormatterAndType(input);
      }}).toArray(FormatterAndType.class);
    } else {
      formatterAndType = DateFormats.DEFAULT_FORMATTERS;
    }

    // special handling for multi-dimensional geo shape types.
    if(isGeoShape && ElasticsearchConstants.GEO_SHAPE_COORDINATES.equals(name)){
      return new WriteHolders.DoubleWriteHolder(name);
    }

    switch(type){
    case BIGINT:
      return new WriteHolders.BigIntWriteHolder(name);
    case BIT:
        return new WriteHolders.BitWriteHolder(name);
    case DATE:
      return new WriteHolders.DateWriteHolder(name, path, formatterAndType);
    case FLOAT4:
      return new WriteHolders.FloatWriteHolder(name);
    case FLOAT8:
      return new WriteHolders.DoubleWriteHolder(name);
    case INT:
      return new WriteHolders.IntWriteHolder(name);
    case TIME:
      return new WriteHolders.TimeWriteHolder(name, path, formatterAndType);
    case TIMESTAMP:
      return new WriteHolders.TimestampWriteHolder(name, path, formatterAndType);
    case VARBINARY:
      return new WriteHolders.VarBinaryWriteHolder(name, buffer);
    case VARCHAR:
      return new WriteHolders.VarCharWriteHolder(name, buffer);
    default:
      return new WriteHolders.InvalidWriteHolder();
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("isList", isList).add("type", type)
        .add("children", children).add("holder", holder).toString();
  }


}
