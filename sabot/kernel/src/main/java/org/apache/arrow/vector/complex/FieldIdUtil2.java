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
package org.apache.arrow.vector.complex;

import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.common.expression.AbstractArrowTypeVisitor;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.TypedFieldId;

public class FieldIdUtil2 {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FieldIdUtil2.class);

  private static TypedFieldId getFieldIdIfMatchesUnion(Field field, TypedFieldId.Builder builder, boolean addToBreadCrumb, PathSegment seg) {
    if (seg == null) {
      if (addToBreadCrumb) {
        builder.intermediateType(CompleteType.fromField(field));
      }
      return builder.finalType(CompleteType.fromField(field)).build();
    }
    if (seg.isNamed()) {
      FieldWithOrdinal ford = getChildField(field, "map");
      if (ford != null) {
        return getFieldIdIfMatches(ford.field, builder, addToBreadCrumb, seg);
      } else {
        return null;
      }
    } else if (seg.isArray()) {
      FieldWithOrdinal ford = getChildField(field, "list");
      if (ford != null) {
        return getFieldIdIfMatches(ford.field, builder, addToBreadCrumb, seg);
      } else {
        return null;
      }
    }
    return null;
  }

  public static TypedFieldId getFieldId(BatchSchema schema, SchemaPath path){
    final boolean isHyper = schema.getSelectionVectorMode() == SelectionVectorMode.FOUR_BYTE;
    return getFieldId(schema, path, isHyper);
  }

  public static TypedFieldId getFieldId(Schema schema, SchemaPath path, boolean isHyper){
    int i = 0;
    for (Field f : schema.getFields()) {
      TypedFieldId id = getFieldId(f, i, path, isHyper);
      if (id != null) {
        return id;
      }
      i++;
    }
    return null;
  }

  public static TypedFieldId getFieldId(final Field field, final int id, final SchemaPath expectedPath, final boolean hyper) {
    if (!expectedPath.getRootSegment().getNameSegment().getPath().equalsIgnoreCase(field.getName())) {
      return null;
    }
    final PathSegment seg = expectedPath.getRootSegment();


    final TypedFieldId.Builder builder = TypedFieldId.newBuilder();
    if (hyper) {
      builder.hyper();
    }

    return field.getType().accept(new AbstractArrowTypeVisitor<TypedFieldId>(){

      @Override
      public TypedFieldId visit(Struct incoming) {
        // we're looking for a multi path.
        builder.intermediateType(CompleteType.fromField(field));
        builder.addId(id);
        return getFieldIdIfMatches(field, builder, true, expectedPath.getRootSegment().getChild());
      }

      @Override
      public TypedFieldId visit(org.apache.arrow.vector.types.pojo.ArrowType.List incoming) {
        builder.intermediateType(CompleteType.fromField(field));
        builder.addId(id);
        return getFieldIdIfMatches(field, builder, true, expectedPath.getRootSegment().getChild());
      }

      @Override
      public TypedFieldId visit(Union incoming) {
        builder.addId(id).remainder(expectedPath.getRootSegment().getChild());

        CompleteType type = CompleteType.fromField(field);
        builder.intermediateType(type);
        if (seg.isLastPath()) {
          builder.finalType(type);
          return builder.build();
        } else {
          return getFieldIdIfMatchesUnion(field, builder, false, seg.getChild());
        }
      }

      @Override
      protected TypedFieldId visitGeneric(ArrowType type) {
        builder.intermediateType(CompleteType.fromField(field));
        builder.addId(id);
        builder.finalType(CompleteType.fromField(field));
        if (seg.isLastPath()) {
          return builder.build();
        } else {
          PathSegment child = seg.getChild();
          if (child.isArray() && child.isLastPath()) {
            builder.remainder(child);
            builder.withIndex();
            builder.finalType(CompleteType.fromField(field));
            return builder.build();
          } else {
            return null;
          }

        }
      }

    });

  }

  private static TypedFieldId getFieldIdIfMatches(
      final Field field,
      final TypedFieldId.Builder builder,
      boolean addToBreadCrumb,
      final PathSegment seg) {
    if (seg == null) {
      if (addToBreadCrumb) {
        builder.intermediateType(CompleteType.fromField(field));
      }
      return builder.finalType(CompleteType.fromField(field)).build();
    }

    final ArrowTypeID typeType = field.getType().getTypeID();

    if (seg.isArray()) {
      if (seg.isLastPath()) {
        CompleteType type;
        if (typeType == ArrowTypeID.Struct) {
          type = CompleteType.fromField(field);
        } else if (typeType == ArrowTypeID.List) {
          type = CompleteType.fromField(field.getChildren().get(0));
          builder.listVector();
        } else {
          throw new UnsupportedOperationException("FieldIdUtil does not support field of type " + field.getType());
        }
        builder //
                .withIndex() //
                .finalType(type);

        // remainder starts with the 1st array segment in SchemaPath.
        // only set remainder when it's the only array segment.
        if (addToBreadCrumb) {
          addToBreadCrumb = false;
          builder.remainder(seg);
        }
        return builder.build();
      } else {
        if (addToBreadCrumb) {
          addToBreadCrumb = false;
          builder.remainder(seg);
        }
      }
    } else {
      if (typeType == ArrowTypeID.List) {
        return null;
      }
    }

    final Field inner;
    if (typeType == ArrowTypeID.Struct) {
      if(seg.isArray()){
        return null;
      }
      FieldWithOrdinal ford = getChildField(field, seg.isArray() ? null : seg.getNameSegment().getPath());
      if (ford == null) {
        return null;
      }
      inner = ford.field;
      if (addToBreadCrumb) {
        builder.intermediateType(CompleteType.fromField(inner));
        builder.addId(ford.ordinal);
      }
    } else if (typeType == ArrowTypeID.List) {
      inner = field.getChildren().get(0);
    } else {
      throw new UnsupportedOperationException("FieldIdUtil does not support field of type " + field.getType());
    }

    final ArrowTypeID innerTypeType = inner.getType().getTypeID();
    if (innerTypeType == ArrowTypeID.List || innerTypeType == ArrowTypeID.Struct) {
      return getFieldIdIfMatches(inner, builder, addToBreadCrumb, seg.getChild());
    } else if (innerTypeType == ArrowTypeID.Union) {
      return getFieldIdIfMatchesUnion(inner, builder, addToBreadCrumb, seg.getChild());
    } else {
      if (seg.isNamed()) {
        if(addToBreadCrumb) {
          builder.intermediateType(CompleteType.fromField(inner));
        }
        builder.finalType(CompleteType.fromField(inner));
      } else {
        builder.finalType(CompleteType.fromField(inner));
      }

      if (seg.isLastPath()) {
        return builder.build();
      } else {
        PathSegment child = seg.getChild();
        if (child.isLastPath() && child.isArray()) {
          if (addToBreadCrumb) {
            builder.remainder(child);
          }
          builder.finalType(CompleteType.fromField(inner));
          return builder.build();
        } else {
          logger.warn("You tried to request a complex type inside a scalar object or path or type is wrong.");
          return null;
        }
      }
    }
  }

  private static FieldWithOrdinal getChildField(Field f, String name) {
    Map<String, FieldWithOrdinal> children = new HashMap<>();
    int i = 0;
    for(Field child : f.getChildren()){
      children.put(child.getName().toLowerCase(), new FieldWithOrdinal(child, i));
      i++;
    }

    return children.get(name.toLowerCase());
  }

  private static class FieldWithOrdinal {
    private final Field field;
    private final int ordinal;

    public FieldWithOrdinal(Field field, int ordinal) {
      super();
      this.field = field;
      this.ordinal = ordinal;
    }

  }


}
