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
package com.dremio.plugins.elastic.mapping;

import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticAnnotation;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticSpecialType;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;

/**
 * Additional information about fields within Elasticsearch that are beyond what
 * Dremio typically needs for schema operation. Used during both planning and
 * execution.
 */
public final class FieldAnnotation {

  private final SchemaPath path;
  private final boolean analyzed;
  private final boolean notIndexed;
  private final boolean normalized;
  private final boolean docValueMissing;
  private final List<String> dateFormats;
  private final ElasticSpecialType specialType;

  private FieldAnnotation(ElasticAnnotation annotation) {
    super();
    this.path = SchemaPath.getCompoundPath(FluentIterable.from(annotation.getPathList()).toArray(String.class));
    this.analyzed = annotation.hasAnalyzed() && annotation.getAnalyzed();
    this.notIndexed = annotation.hasNotIndexed() && annotation.getNotIndexed();
    this.normalized = annotation.hasNormalized() && annotation.getNormalized();
    this.docValueMissing = annotation.hasDocValueMissing() && annotation.getDocValueMissing();
    this.dateFormats = annotation.getDateFormatsList();
    this.specialType = annotation.hasSpecialType() ? annotation.getSpecialType() : null;

  }

  public SchemaPath getPath() {
    return path;
  }

  public boolean isAnalyzed() {
    return analyzed;
  }

  public boolean isNotIndexed() {
    return notIndexed;
  }

  public boolean isNormalized() {
    return normalized;
  }

  public boolean isUnknown() {
    return specialType == ElasticSpecialType.UNKNOWN;
  }

  public boolean isIpType() {
    return specialType == ElasticSpecialType.IP_TYPE;
  }

  public boolean isGeoShape() {
    return specialType == ElasticSpecialType.GEO_SHAPE;
  }

  public boolean isGeoPoint() {
    return specialType == ElasticSpecialType.GEO_POINT;
  }

  public boolean hasSpecialType(){
    return specialType != null;
  }

  public ElasticSpecialType getSpecialType(){
    return specialType;
  }

  public boolean isDocValueMissing() {
    return docValueMissing;
  }

  public List<String> getDateFormats() {
    return dateFormats;
  }

  public static ImmutableMap<SchemaPath, FieldAnnotation> getAnnotationMap(List<ElasticAnnotation> annotations){
    final ImmutableMap.Builder<SchemaPath, FieldAnnotation> annotationMapB = ImmutableMap.builder();
    for(ElasticAnnotation a : annotations) {
      FieldAnnotation fa = new FieldAnnotation(a);
      annotationMapB.put(fa.getPath(), fa);
    }
    return annotationMapB.build();
  }
}
