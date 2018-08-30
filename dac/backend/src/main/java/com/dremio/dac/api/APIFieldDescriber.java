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
package com.dremio.dac.api;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.AbstractArrowTypeVisitor;
import com.dremio.common.expression.SqlTypeNameVisitor;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * Describes an Arrow field into a JSON generator for the REST API
 */
public class APIFieldDescriber {
  /**
   * Describes an Arrow field into a JSON generator
   */
  public static final class FieldDescriber extends AbstractArrowTypeVisitor<Void> {
    private final JsonGenerator generator;
    private final TypeDescriber typeDescriber;
    private final Field field;
    private boolean skipName;

    public FieldDescriber(JsonGenerator jsonGenerator, Field field, boolean skipName) {
      generator = jsonGenerator;
      typeDescriber = new TypeDescriber(jsonGenerator);
      this.field = field;
      this.skipName = skipName;
    }

    @Override
    protected Void visitGeneric(ArrowType type) {
      try {
        generator.writeStartObject();

        if (!skipName) {
          generator.writeFieldName("name");
          generator.writeString(field.getName());
        }

        generator.writeFieldName("type");

        generator.writeStartObject();
        generator.writeFieldName("name");
        type.accept(typeDescriber);
        generator.writeEndObject();

        generator.writeEndObject();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    @Override
    public Void visit(ArrowType.Struct type) {
      try {
        generator.writeStartObject();

        if (!skipName) {
          generator.writeFieldName("name");
          generator.writeString(field.getName());
        }

        generator.writeFieldName("type");

        generator.writeStartObject();
        generator.writeFieldName("name");
        type.accept(typeDescriber);

        List<Field> children = field.getChildren();
        if (children != null) {
          generator.writeFieldName("subSchema");
          generator.writeStartArray();

          for (Field field : children) {
            APIFieldDescriber.FieldDescriber describer = new APIFieldDescriber.FieldDescriber(generator, field, false);
            field.getType().accept(describer);
          }

          generator.writeEndArray();
        }
        generator.writeEndObject();

        generator.writeEndObject();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    @Override
    public Void visit(ArrowType.List type) {
      try {
        generator.writeStartObject();

        if (!skipName) {
          generator.writeFieldName("name");
          generator.writeString(field.getName());
        }

        generator.writeFieldName("type");

        generator.writeStartObject();
        generator.writeFieldName("name");
        type.accept(typeDescriber);

        List<Field> children = field.getChildren();
        if (children != null) {
          generator.writeFieldName("subSchema");
          generator.writeStartArray();

          for (Field field : children) {
            APIFieldDescriber.FieldDescriber describer = new APIFieldDescriber.FieldDescriber(generator, field, true);
            field.getType().accept(describer);
          }

          generator.writeEndArray();
        }
        generator.writeEndObject();

        generator.writeEndObject();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    @Override
    public Void visit(ArrowType.Union type) {
      try {
        generator.writeStartObject();

        if (!skipName) {
          generator.writeFieldName("name");
          generator.writeString(field.getName());
        }

        generator.writeFieldName("type");

        generator.writeStartObject();
        generator.writeFieldName("name");
        type.accept(typeDescriber);

        List<Field> children = field.getChildren();
        if (children != null) {
          generator.writeFieldName("subSchema");
          generator.writeStartArray();

          for (Field field : children) {
            APIFieldDescriber.FieldDescriber describer = new APIFieldDescriber.FieldDescriber(generator, field, true);
            field.getType().accept(describer);
          }

          generator.writeEndArray();
        }
        generator.writeEndObject();

        generator.writeEndObject();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  /**
   * Describes a Arrow type into a JSON generator
   */
  private static class TypeDescriber implements ArrowType.ArrowTypeVisitor<Void> {
    private final JsonGenerator generator;
    private final SqlTypeNameVisitor sqlTypeNameVisitor;

    private Void writeString(String name) {
      try {
        generator.writeString(name);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    public TypeDescriber(JsonGenerator jsonGenerator) {
      this.generator = jsonGenerator;
      this.sqlTypeNameVisitor = new SqlTypeNameVisitor();
    }

    @Override
    public Void visit(ArrowType.Null aNull) {
      return writeString("OTHER");
    }

    @Override
    public Void visit(ArrowType.Struct struct) {
      return writeString("STRUCT");
    }

    @Override
    public Void visit(ArrowType.List list) {
      return writeString("LIST");
    }

    @Override
    public Void visit(ArrowType.FixedSizeList fixedSizeList) {
      return writeString("LIST");
    }

    @Override
    public Void visit(ArrowType.FixedSizeBinary fixedSizeBinary) {
      return writeString("BINARY");
    }

    @Override
    public Void visit(ArrowType.Union union) {
      return writeString(sqlTypeNameVisitor.visit(union));
    }

    @Override
    public Void visit(ArrowType.Int anInt) {
      return writeString(sqlTypeNameVisitor.visit(anInt));
    }

    @Override
    public Void visit(ArrowType.FloatingPoint floatingPoint) {
      return writeString(sqlTypeNameVisitor.visit(floatingPoint));
    }

    @Override
    public Void visit(ArrowType.Utf8 utf8) {
      return writeString("VARCHAR");
    }

    @Override
    public Void visit(ArrowType.Binary binary) {
      return writeString("VARBINARY");
    }

    @Override
    public Void visit(ArrowType.Bool bool) {
      return writeString(sqlTypeNameVisitor.visit(bool));
    }

    @Override
    public Void visit(Decimal decimal) {
      writeString(sqlTypeNameVisitor.visit(decimal));

      try {
        generator.writeFieldName("precision");
        generator.writeNumber(decimal.getPrecision());

        generator.writeFieldName("scale");
        generator.writeNumber(decimal.getScale());
      } catch (IOException e) {
        e.printStackTrace();
      }

      return null;
    }

    @Override
    public Void visit(ArrowType.Date date) {
      return writeString(sqlTypeNameVisitor.visit(date));
    }

    @Override
    public Void visit(ArrowType.Time time) {
      return writeString(sqlTypeNameVisitor.visit(time));
    }

    @Override
    public Void visit(ArrowType.Timestamp timestamp) {
      return writeString(sqlTypeNameVisitor.visit(timestamp));
    }

    @Override
    public Void visit(ArrowType.Interval interval) {
      return writeString(sqlTypeNameVisitor.visit(interval));
    }
  }
}
