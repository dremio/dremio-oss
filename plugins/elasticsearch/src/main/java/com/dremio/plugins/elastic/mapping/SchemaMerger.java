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
package com.dremio.plugins.elastic.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.common.expression.SchemaPath;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticAnnotation;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticSpecialType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet.ElasticField;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet.ElasticMapping;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet.Indexing;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Combines an Elastic declared schema with an observed schema.
 */
public class SchemaMerger {

  private static final Logger logger = LoggerFactory.getLogger(SchemaMerger.class);

  private final String datasetPath;

  public SchemaMerger(String datasetPath){
    this.datasetPath = datasetPath;
  }

  public MergeResult merge(ElasticMapping mapping, BatchSchema schema) {
    final ResultBuilder resultBuilder = new ResultBuilder();
    Collection<MergeField> fields = mergeFields(null, mapping.getFields(), schema != null ? schema.getFields() : ImmutableList.<Field>of());

    return resultBuilder.toResult(FluentIterable.from(fields)
        .transform(new Function<MergeField, Optional<Field>>() {
          @Override
          public Optional<Field> apply(MergeField input) {
            return Optional.fromNullable(input.toField(resultBuilder));
          }
        }).filter(new Predicate<Optional<Field>>() {
          @Override
          public boolean apply(Optional<Field> input) {
            return input.isPresent();
          }
        }).transform(new Function<Optional<Field>, Field>() {
          @Override
          public Field apply(Optional<Field> input) {
            return input.get();
          }
        })
        .toList());
  }

  @VisibleForTesting
  List<MergeField> mergeFields(SchemaPath parent, List<ElasticField> declaredFields, List<Field> observedFields){
    final Map<String, Field> batchFieldMap = new HashMap<>(FluentIterable.from(observedFields).uniqueIndex(new Function<Field, String>(){
      @Override
      public String apply(Field input) {
        return input.getName();
      }}));

    // we'll add all the elastic columns, associating where we have an observed schema.
    final List<MergeField> outputFields = new ArrayList<>();
    for(ElasticField declaredField : declaredFields){
      final Field observedField = batchFieldMap.remove(declaredField.getName());

      if(observedField == null){
        // this is a field that we haven't observed, add it with default settings.
        outputFields.add(new MergeField(parent, declaredField));
        continue;
      }

      outputFields.add(mergeField(parent, declaredField, CompleteType.fromField(observedField)));
    }

    // Any remaining field that we didn't match already should be added as well.
    for(Field f : batchFieldMap.values()){
      outputFields.add(new MergeField(parent, null, f));
    }

    return outputFields;
  }

  private static SchemaPath child(SchemaPath parent, String name){
    if(parent == null){
      return SchemaPath.getSimplePath(name);
    }
    return parent.getChild(name);
  }

  @VisibleForTesting
  MergeField mergeField(SchemaPath parent, ElasticField declaredField, CompleteType observedType){

    // if we have a union of scalar and list of the same type, promote all to list.
    if(observedType.isUnion()){
      return mergeUnion(parent, declaredField, observedType);
    } else if (observedType.isList()) {
      return mergeList(parent, declaredField, observedType);
    }

    // default behavior.
    if(declaredField.getType() != Type.OBJECT && declaredField.getType() != Type.NESTED) {
      // the exact types should match.
      if(!CompleteType.fromField(declaredField.toArrowField()).equals(observedType)){
        throw failure(parent, declaredField, observedType);
      }
      // scalar (or defined structure) types match, all is good.
      return new MergeField(parent, declaredField, observedType);

    }

    // we are in the object/nested state.
    if(observedType != null && !observedType.isStruct()){
      throw failure(parent, declaredField, observedType);
    }

    final List<MergeField> fields = mergeFields(parent, declaredField.getChildren(), observedType.getChildren());
    return new MergeField(parent, declaredField, fields);
  }

  private MergeField mergeUnion(SchemaPath parent, ElasticField declaredField, CompleteType observedType){
    List<Field> fields = observedType.getChildren();

    if(fields.size() != 2){
      throw failure(parent, declaredField, observedType);
    }

    Field f1 = fields.get(0);
    Field f2 = fields.get(1);
    CompleteType t1 = CompleteType.fromField(f1);
    CompleteType t2 = CompleteType.fromField(f2);

    if( !(t1.isList() && !t2.isList()) &&  !(!t1.isList() && t2.isList())){
      // one of the two types has to be a list type.
      throw failure(parent, declaredField, observedType);
    }


    CompleteType listType = t1.isList() ? t1 : t2;
    CompleteType nonListType = t1.isList() ? t2 : t1;

    Field listChild = listType.getOnlyChild();

    // check that the basic list types are the same. We don't compare full types here because it could be that the two different structs (only a subset of fields showed up in one or both structs).
    if(!listChild.getType().equals(nonListType.getType()) && !listChild.getType().equals(Null.INSTANCE)){
      throw failure(parent, declaredField, observedType);
    }

    CompleteType combined = nonListType.merge(CompleteType.fromField(listChild));

    return mergeField(parent, declaredField, combined).asList();

  }

  private MergeField mergeList(SchemaPath parent, ElasticField declaredField, CompleteType observedType){
    final CompleteType listChildType = CompleteType.fromField(observedType.getOnlyChild());
    return mergeField(parent, declaredField, listChildType).asList();
  }

  private UserException failure(SchemaPath path, ElasticField declaredField, CompleteType observedType){
    return UserException.dataReadError()
        .message(
            "Failure handling type. Dremio only supports a path to type mapping across all schema mappings. \n"
            + "\tDataset path %s.\n"
            + "\tPath to field %s.\n"
            + "\tDeclared Type %s.\n"
            + "\tObserved Type %s.\n",
            datasetPath,
            child(path, declaredField.getName()).getAsUnescapedPath(),
            Describer.describe(declaredField.toArrowField()),
            Describer.describe(observedType.toField(declaredField.getName()))
            )
        .build(logger);
  }

  public static class MergeField {
    private final SchemaPath parent;
    private final ElasticField elasticField;
    private final Field actualField;
    private final List<MergeField> children;

    public MergeField(SchemaPath parent, ElasticField elasticField) {
      this.parent = parent;
      this.elasticField = elasticField;
      this.actualField = elasticField.toArrowField();
      this.children = ImmutableList.of();
    }

    public MergeField(SchemaPath parent, ElasticField elasticField, CompleteType actualType) {
      super();
      this.parent = parent;
      this.elasticField = elasticField;
      this.actualField = actualType.toField(elasticField.getName());
      this.children = ImmutableList.of();
    }

    public MergeField(SchemaPath parent, ElasticField elasticField, Field actualField) {
      super();
      this.parent = parent;
      this.elasticField = elasticField;
      this.actualField = actualField;
      this.children = ImmutableList.of();
    }

    public MergeField(SchemaPath parent, ElasticField elasticField, Field actualField, List<MergeField> children) {
      super();
      this.parent = parent;
      this.elasticField = elasticField;
      this.actualField = actualField;
      this.children = children;
    }

    public MergeField(SchemaPath parent, ElasticField elasticField, List<MergeField> children) {
      super();
      this.parent = parent;
      this.elasticField = elasticField;
      this.actualField = CompleteType.struct().toField(elasticField.getName());
      this.children = children;
    }

    public MergeField asList(){
      return new MergeField(parent, elasticField, CompleteType.fromField(actualField).asList().toField(elasticField.getName()), children);
    }

    public Field toField(final ResultBuilder resultToPopulate) {
      final SchemaPath path = child(parent, elasticField != null ? elasticField.getName() : actualField.getName());

      if(elasticField != null && elasticField.getType() == Type.UNKNOWN){
        recordAnnotations(path, elasticField, resultToPopulate);
        return null;
      }

      if(!children.isEmpty()){
        List<Field> fieldChildren = FluentIterable.from(children).transform(new Function<MergeField, Field>(){
          @Override
          public Field apply(MergeField input) {
            return input.toField(resultToPopulate);
          }}).toList();

        CompleteType struct = CompleteType.struct(fieldChildren);
        if(actualField != null && CompleteType.fromField(actualField).isList()){
          struct = struct.asList();
        }
        recordAnnotations(path, elasticField, resultToPopulate);
        return struct.toField(elasticField.getName());
      } else if(elasticField != null && !elasticField.getChildren().isEmpty()){
        List<Field> fieldChildren = FluentIterable.from(elasticField.getChildren()).transform(new Function<ElasticField, Field>(){
          @Override
          public Field apply(ElasticField input) {
            return new MergeField(path.getChild(input.getName()), input).toField(resultToPopulate);
          }}).toList();

        CompleteType struct = CompleteType.struct(fieldChildren);
        if(actualField != null && CompleteType.fromField(actualField).isList()){
          struct = struct.asList();
        }
        recordAnnotations(path, elasticField, resultToPopulate);
        return struct.toField(elasticField.getName());
      }

      recordAnnotations(path, elasticField, resultToPopulate);

      return actualField;
    }
  }

  private static void recordAnnotations(SchemaPath path, ElasticField elasticField, ResultBuilder resultToPopulate){
    if(elasticField != null){
      if(elasticField.getIndexing() != Indexing.NOT_ANALYZED || elasticField.isNormalized()){
        resultToPopulate.isNotGroupable(path);
      }

      if(!elasticField.hasDocValues()){
        resultToPopulate.hasNoDocValue(path);
      }

      // handle special types.
      switch(elasticField.getType()){
      case GEO_POINT:
        resultToPopulate.isGeoPoint(path);
        break;

      case UNKNOWN:
        resultToPopulate.isUnknown(path);
        break;

      case GEO_SHAPE:
        resultToPopulate.isGeoShape(path);
        break;

      case IP:
        resultToPopulate.isIpType(path);
        break;

      case NESTED:
        resultToPopulate.isNestedType(path);
        break;

      case DATE:
      case TIMESTAMP:
      case TIME:
        resultToPopulate.isDate(path, elasticField.getFormats());
        break;

      default:
        break;

      }

    }
  }

  public static class MergeResult {

    private final BatchSchema schema;
    private final List<ElasticAnnotation> annotations;

    // add all the columns.
    public MergeResult(List<Field> fields, Collection<ElasticAnnotation> annotations){
      this.schema = BatchSchema.newBuilder().addFields(fields).build();
      this.annotations = ImmutableList.copyOf(annotations);
    }

    public BatchSchema getSchema() {
      return schema;
    }

    public List<ElasticAnnotation> getAnnotations() {
      return annotations;
    }

  }

  private static class ResultBuilder {
    private final Map<SchemaPath, ElasticAnnotation> annotations = new HashMap<>();

    public void isDate(SchemaPath path, List<String> dateFormats){
      annotations.put(path, anno(path).addAllDateFormats(dateFormats).build());
    }

    public void isUnknown(SchemaPath path){
      annotations.put(path,  anno(path).setSpecialType(ElasticSpecialType.UNKNOWN).build());
    }

    public void isGeoShape(SchemaPath path){
      annotations.put(path,  anno(path).setSpecialType(ElasticSpecialType.GEO_SHAPE).build());
    }

    public void isGeoPoint(SchemaPath path){
      annotations.put(path,  anno(path).setSpecialType(ElasticSpecialType.GEO_POINT).build());
    }

    public void isNestedType(SchemaPath path){
      annotations.put(path,  anno(path).setSpecialType(ElasticSpecialType.NESTED).build());
    }
    public void isIpType(SchemaPath path){
      annotations.put(path,  anno(path).setSpecialType(ElasticSpecialType.IP_TYPE).build());
    }

    public void isNotGroupable(SchemaPath path){
      annotations.put(path, anno(path).setAnalyzed(true).build());
    }

    public void hasNoDocValue(SchemaPath path){
      annotations.put(path, anno(path).setDocValueMissing(true).build());
    }

    public MergeResult toResult(List<Field> fields){
      return new MergeResult(fields, annotations.values());
    }

    private ElasticAnnotation.Builder anno(SchemaPath path){
      ElasticAnnotation annotation = annotations.get(path);
      if(annotation == null){
        return ElasticAnnotation.newBuilder().addAllPath(path.getNameSegments());
      }else{
        return annotation.toBuilder();
      }
    }
  }

}
