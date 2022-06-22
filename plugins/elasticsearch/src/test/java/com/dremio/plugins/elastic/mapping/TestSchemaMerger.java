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

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;

public class TestSchemaMerger {
  private ElasticMappingSet.ElasticMapping mapping;
  private BatchSchema schema;
  private SchemaMerger schemaMerger;

  @Before
  public void setup() {
    createMapping();
    createSchema();
    schemaMerger = new SchemaMerger("/foo");
  }

  private void createMapping() {
    final Map<String, ElasticMappingSet.ElasticField> innerFields2 = new HashMap<>();
    innerFields2.put("1", createElasticTextField("stringLevel2"));
    innerFields2.put("2", createElasticFloatField("floatLevel2"));
    innerFields2.put("3", createElasticFloatField("floatListLevel2"));
    final ElasticMappingSet.ElasticField innerField2 = new ElasticMappingSet.ElasticField(
      "innerField2",
      ElasticMappingSet.Type.OBJECT,
      ElasticMappingSet.Indexing.NOT_ANALYZED,
      null,
      null,
      true,
      innerFields2
    );

    final Map<String, ElasticMappingSet.ElasticField> innerFields1 = new HashMap<>();
    innerFields1.put("1", createElasticTextField("stringLevel1"));
    innerFields1.put("2", createElasticFloatField("floatLevel1"));
    innerFields1.put("3", createElasticFloatField("floatListLevel1"));
    innerFields1.put("4", innerField2);
    final ElasticMappingSet.ElasticField innerField1 = new ElasticMappingSet.ElasticField(
      "innerField1",
      ElasticMappingSet.Type.OBJECT,
      ElasticMappingSet.Indexing.NOT_ANALYZED,
      null,
      null,
      true,
      innerFields1
    );

    final Map<String, ElasticMappingSet.ElasticField> fields = new HashMap<>();
    fields.put("1", createElasticTextField("stringLevel0"));
    fields.put("2", createElasticFloatField("floatLevel0"));
    fields.put("3", createElasticFloatField("floatListLevel0"));
    fields.put("4", innerField1);

    mapping = new ElasticMappingSet.ElasticMapping("_doc", fields);
  }

  private ElasticMappingSet.ElasticField createElasticTextField(String name) {
    return new ElasticMappingSet.ElasticField(
      name,
      ElasticMappingSet.Type.TEXT,
      ElasticMappingSet.Indexing.ANALYZED,
      null,
      null,
      true,
      Collections.emptyMap()
    );
  }

  private ElasticMappingSet.ElasticField createElasticFloatField(String name) {
    return new ElasticMappingSet.ElasticField(
      name,
      ElasticMappingSet.Type.FLOAT,
      ElasticMappingSet.Indexing.NOT_ANALYZED,
      null,
      null,
      true,
      Collections.emptyMap()
    );
  }

  private void createSchema() {
    final List<Field> innerChildren2 = new ArrayList<>();
    innerChildren2.add(createStringField("stringLevel2"));
    innerChildren2.add(createFloatField("floatLevel2"));
    innerChildren2.add(createFloatListField("floatListLevel2"));
    final Field innerField2 = new Field(
      "innerField2",
      FieldType.nullable(ArrowType.Struct.INSTANCE),
      innerChildren2
    );

    final List<Field> innerChildren1 = new ArrayList<>();
    innerChildren1.add(createStringField("stringLevel1"));
    innerChildren1.add(createFloatField("floatLevel1"));
    innerChildren1.add(createFloatListField("floatListLevel1"));
    innerChildren1.add(innerField2);
    final Field innerField1 = new Field(
      "innerField1",
      FieldType.nullable(ArrowType.Struct.INSTANCE),
      innerChildren1
    );

    final List<Field> children = new ArrayList<>();
    children.add(createStringField("stringLevel0"));
    children.add(createFloatField("floatLevel0"));
    children.add(createFloatListField("floatListLevel0"));
    children.add(innerField1);

    schema = new BatchSchema(children);
  }

  private Field createStringField(String name) {
    return Field.nullable(name, ArrowType.Utf8.INSTANCE);
  }

  private Field createFloatField(String name) {
    final List<Field> children = new ArrayList<>();
    children.add(Field.nullable("float4", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
    children.add(Field.nullable("float8", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));

    return new Field(
      name,
      FieldType.nullable(new ArrowType.Union(UnionMode.Sparse, new int[] {20, 21})),
      children
    );
  }

  private Field createFloatListField(String name) {
    final Field floatField = Field.nullable("float4", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
    final Field listItemField = Field.nullable("$data$", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
    final Field listField = new Field(
      "list",
      FieldType.nullable(new ArrowType.List()),
      Collections.singletonList(listItemField)
    );

    final List<Field> children = new ArrayList<>();
    children.add(floatField);
    children.add(listField);

    return new Field(
      name,
      FieldType.nullable(new ArrowType.Union(UnionMode.Sparse, new int[] {20, 34})),
      children);
  }

  @Test
  public void mergeWithoutForceDouble() throws Exception {
    mergeTest(false);
  }

  @Test
  public void mergeWithForceDouble() throws Exception {
    mergeTest(true);
  }

  private void mergeTest(boolean forceDoublePecision) throws Exception {
    final SchemaMerger.MergeResult mergeResult = schemaMerger.merge(mapping, schema, forceDoublePecision);
    for (Field field : mergeResult.getSchema().getFields()) {
      verifyField(field, forceDoublePecision);
    }
  }

  private void verifyField(Field field, boolean forceDoublePecision) {
    final CompleteType completeType = CompleteType.fromField(field);

    if (completeType.isStruct() || completeType.isList() || completeType.isUnion()) {
      for (Field child : field.getChildren()) {
        verifyField(child, forceDoublePecision);
      }
    } else if (completeType.isFloat() && forceDoublePecision) {
      fail("Found a float field when force double precision was enabled");
    } else if (completeType.isDouble() && !forceDoublePecision) {
      fail("Found a double field when force double precision was disabled");
    }
  }
}
