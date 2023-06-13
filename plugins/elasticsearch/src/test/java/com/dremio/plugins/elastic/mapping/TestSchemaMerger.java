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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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
      innerFields2,
      null);

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
      innerFields1,
      null);

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
      Collections.emptyMap(),
      null);
  }

  private ElasticMappingSet.ElasticField createElasticField(String name, ElasticMappingSet.Type type, boolean normalized) {
    return new ElasticMappingSet.ElasticField(
      name,
      type,
      ElasticMappingSet.Indexing.NOT_ANALYZED,
      normalized ? "true" : null,
      null,
      true,
      Collections.emptyMap(),
      null);
  }

  private ElasticMappingSet.ElasticField createElasticFloatField(String name) {
    return new ElasticMappingSet.ElasticField(
      name,
      ElasticMappingSet.Type.FLOAT,
      ElasticMappingSet.Indexing.NOT_ANALYZED,
      null,
      null,
      true,
      Collections.emptyMap(),
      null);
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

  @Test
  public void mergeTestTypes() throws Exception {
    for (ElasticMappingSet.Type type : ElasticMappingSet.Type.values()) {
      if (ElasticMappingSet.Type.NESTED == type || ElasticMappingSet.Type.OBJECT == type) {
        continue;
      }

      for (ElasticMappingSet.Type otherType : ElasticMappingSet.Type.values()) {
        if (ElasticMappingSet.Type.NESTED == otherType || ElasticMappingSet.Type.OBJECT == otherType) {
          continue;
        }

        final ElasticMappingSet.ElasticField field = createElasticField("field1", type, false);
        final ElasticMappingSet.ElasticField otherField = createElasticField("field1", otherType, false);
        final ElasticMappingSet.ElasticField mergedField = field.merge(null, otherField, null, null, null, null);

        final TypePair typePair = new TypePair(type, otherType);
        if (INVALID_MERGE_PAIRS.contains(typePair)) {
          assertNull(String.format("Type1 = %s, Type2 = %s", type.name(), otherType.name()), mergedField);
        } else {
          assertNotNull(String.format("Type1 = %s, Type2 = %s", type.name(), otherType.name()), mergedField);
          final ElasticMappingSet.Type expectedType;
          if (MERGE_RESULTANT_TYPES.containsKey(typePair)) {
            expectedType = MERGE_RESULTANT_TYPES.get(typePair);
          } else {
            expectedType = type;
          }
          assertEquals(String.format("Type1 = %s, Type2 = %s", type.name(), otherType.name()), expectedType, mergedField.getType());
        }
      }
    }
  }

  @Test
  public void mergeTestNormalizedMismatch() {
    final ElasticMappingSet.ElasticField field = createElasticField("field1", ElasticMappingSet.Type.INTEGER, true);
    final ElasticMappingSet.ElasticField otherField = createElasticField("field1", ElasticMappingSet.Type.LONG, false);

    final ElasticMappingSet.ElasticField mergedField = field.merge(null, otherField, null, null, null, null);
    assertTrue(mergedField.isNormalized());
  }

  private static class TypePair {
    private final ElasticMappingSet.Type type1;
    private final ElasticMappingSet.Type type2;

    public TypePair(ElasticMappingSet.Type type1, ElasticMappingSet.Type type2) {
      this.type1 = type1;
      this.type2 = type2;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type1, type2);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }

      if (!(obj instanceof TypePair)) {
        return false;
      }

      final TypePair other = (TypePair) obj;
      return Objects.equals(type1, other.type1) && Objects.equals(type2, other.type2);
    }
  }

  private static final Set<TypePair> INVALID_MERGE_PAIRS;
  private static final Map<TypePair, ElasticMappingSet.Type> MERGE_RESULTANT_TYPES;
  static {
    INVALID_MERGE_PAIRS = ImmutableSet.<TypePair>builder()
      .add(new TypePair(ElasticMappingSet.Type.BYTE, ElasticMappingSet.Type.SHORT))
      .add(new TypePair(ElasticMappingSet.Type.BYTE, ElasticMappingSet.Type.INTEGER))
      .add(new TypePair(ElasticMappingSet.Type.BYTE, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.SHORT, ElasticMappingSet.Type.BYTE))
      .add(new TypePair(ElasticMappingSet.Type.SHORT, ElasticMappingSet.Type.INTEGER))
      .add(new TypePair(ElasticMappingSet.Type.SHORT, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.INTEGER, ElasticMappingSet.Type.BYTE))
      .add(new TypePair(ElasticMappingSet.Type.INTEGER, ElasticMappingSet.Type.SHORT))
      .add(new TypePair(ElasticMappingSet.Type.INTEGER, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.LONG, ElasticMappingSet.Type.BYTE))
      .add(new TypePair(ElasticMappingSet.Type.LONG, ElasticMappingSet.Type.SHORT))
      .add(new TypePair(ElasticMappingSet.Type.LONG, ElasticMappingSet.Type.INTEGER))
      .add(new TypePair(ElasticMappingSet.Type.LONG, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.BYTE))
      .add(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.SHORT))
      .add(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.INTEGER))
      .add(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.LONG))
      .add(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.BYTE))
      .add(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.SHORT))
      .add(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.INTEGER))
      .add(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.LONG))
      .add(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.DOUBLE, ElasticMappingSet.Type.BYTE))
      .add(new TypePair(ElasticMappingSet.Type.DOUBLE, ElasticMappingSet.Type.SHORT))
      .add(new TypePair(ElasticMappingSet.Type.DOUBLE, ElasticMappingSet.Type.INTEGER))
      .add(new TypePair(ElasticMappingSet.Type.DOUBLE, ElasticMappingSet.Type.LONG))
      .add(new TypePair(ElasticMappingSet.Type.DOUBLE, ElasticMappingSet.Type.HALF_FLOAT))
      .add(new TypePair(ElasticMappingSet.Type.DOUBLE, ElasticMappingSet.Type.FLOAT))
      .add(new TypePair(ElasticMappingSet.Type.DOUBLE, ElasticMappingSet.Type.SCALED_FLOAT))
      .add(new TypePair(ElasticMappingSet.Type.DOUBLE, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.SCALED_FLOAT, ElasticMappingSet.Type.BYTE))
      .add(new TypePair(ElasticMappingSet.Type.SCALED_FLOAT, ElasticMappingSet.Type.SHORT))
      .add(new TypePair(ElasticMappingSet.Type.SCALED_FLOAT, ElasticMappingSet.Type.INTEGER))
      .add(new TypePair(ElasticMappingSet.Type.SCALED_FLOAT, ElasticMappingSet.Type.LONG))
      .add(new TypePair(ElasticMappingSet.Type.SCALED_FLOAT, ElasticMappingSet.Type.HALF_FLOAT))
      .add(new TypePair(ElasticMappingSet.Type.SCALED_FLOAT, ElasticMappingSet.Type.FLOAT))
      .add(new TypePair(ElasticMappingSet.Type.SCALED_FLOAT, ElasticMappingSet.Type.DOUBLE))
      .add(new TypePair(ElasticMappingSet.Type.SCALED_FLOAT, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.BOOLEAN, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.BINARY, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.STRING, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.BYTE))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.SHORT))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.INTEGER))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.LONG))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.HALF_FLOAT))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.FLOAT))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.DOUBLE))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.SCALED_FLOAT))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.BOOLEAN))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.BINARY))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.STRING))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.KEYWORD))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.DATE))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.IP))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.TIME))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.TIMESTAMP))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.GEO_POINT))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.GEO_SHAPE))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.ATTACHMENT))
      .add(new TypePair(ElasticMappingSet.Type.TEXT, ElasticMappingSet.Type.UNKNOWN))
      .add(new TypePair(ElasticMappingSet.Type.KEYWORD, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.IP, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.TIME, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.TIMESTAMP, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.GEO_POINT, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.GEO_SHAPE, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.ATTACHMENT, ElasticMappingSet.Type.TEXT))
      .add(new TypePair(ElasticMappingSet.Type.UNKNOWN, ElasticMappingSet.Type.TEXT))
      .build();

    MERGE_RESULTANT_TYPES = ImmutableMap.<TypePair, ElasticMappingSet.Type>builder()
      .put(new TypePair(ElasticMappingSet.Type.BYTE, ElasticMappingSet.Type.LONG), ElasticMappingSet.Type.LONG)
      .put(new TypePair(ElasticMappingSet.Type.BYTE, ElasticMappingSet.Type.HALF_FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.BYTE, ElasticMappingSet.Type.FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.BYTE, ElasticMappingSet.Type.DOUBLE), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.BYTE, ElasticMappingSet.Type.SCALED_FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.SHORT, ElasticMappingSet.Type.LONG), ElasticMappingSet.Type.LONG)
      .put(new TypePair(ElasticMappingSet.Type.SHORT, ElasticMappingSet.Type.HALF_FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.SHORT, ElasticMappingSet.Type.FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.SHORT, ElasticMappingSet.Type.DOUBLE), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.SHORT, ElasticMappingSet.Type.SCALED_FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.INTEGER, ElasticMappingSet.Type.LONG), ElasticMappingSet.Type.LONG)
      .put(new TypePair(ElasticMappingSet.Type.INTEGER, ElasticMappingSet.Type.HALF_FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.INTEGER, ElasticMappingSet.Type.FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.INTEGER, ElasticMappingSet.Type.DOUBLE), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.INTEGER, ElasticMappingSet.Type.SCALED_FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.LONG, ElasticMappingSet.Type.HALF_FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.LONG, ElasticMappingSet.Type.FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.LONG, ElasticMappingSet.Type.DOUBLE), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.LONG, ElasticMappingSet.Type.SCALED_FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.HALF_FLOAT), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.FLOAT), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.DOUBLE), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.SCALED_FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.BOOLEAN), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.BINARY), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.STRING), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.KEYWORD), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.DATE), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.IP), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.TIME), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.TIMESTAMP), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.GEO_POINT), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.GEO_SHAPE), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.ATTACHMENT), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.HALF_FLOAT, ElasticMappingSet.Type.UNKNOWN), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.DOUBLE), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.SCALED_FLOAT), ElasticMappingSet.Type.DOUBLE)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.BOOLEAN), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.BINARY), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.STRING), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.KEYWORD), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.DATE), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.IP), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.TIME), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.TIMESTAMP), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.GEO_POINT), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.GEO_SHAPE), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.ATTACHMENT), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.FLOAT, ElasticMappingSet.Type.UNKNOWN), ElasticMappingSet.Type.FLOAT)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.BYTE), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.SHORT), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.INTEGER), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.LONG), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.HALF_FLOAT), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.FLOAT), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.DOUBLE), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.SCALED_FLOAT), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.BOOLEAN), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.BINARY), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.STRING), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.KEYWORD), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.DATE), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.IP), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.TIME), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.TIMESTAMP), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.GEO_POINT), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.GEO_SHAPE), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.ATTACHMENT), ElasticMappingSet.Type.TIMESTAMP)
      .put(new TypePair(ElasticMappingSet.Type.DATE, ElasticMappingSet.Type.UNKNOWN), ElasticMappingSet.Type.TIMESTAMP)
      .build();
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
