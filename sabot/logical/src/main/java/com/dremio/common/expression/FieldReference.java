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
package com.dremio.common.expression;

import java.io.IOException;

import com.dremio.common.expression.PathSegment.NameSegment;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

// Need a custom deserializer as ExprParser can not distinguish between
// SchemaPath and FieldReference
@JsonDeserialize(using = FieldReference.De.class)
public class FieldReference extends SchemaPath {

  private final CompleteType overrideType;

  public FieldReference(CharSequence value) {
    this(value, true);
  }

  public FieldReference(CharSequence value, boolean check) {
    this(validate(value, check), null);
  }

  public FieldReference(String value, CompleteType dataType) {
    this(validate(value, true), dataType);
  }

  public FieldReference(SchemaPath sp) {
    this(validate(sp), null);
  }

  private FieldReference(SchemaPath sp, CompleteType override) {
    super(sp);
    this.overrideType = override;
  }

  private static SchemaPath validate(CharSequence value, boolean check){
    if (check && value.toString().contains(".")) {
      throw new UnsupportedOperationException(
          String.format(
              "Unhandled field reference \"%s\"; a field reference identifier"
              + " must not have the form of a qualified name (i.e., with \".\").",
              value));
    }
    return new SchemaPath(new NameSegment(value));
  }

  private static SchemaPath validate(SchemaPath path) {
    if (path.getRootSegment().getChild() != null) {
      throw new UnsupportedOperationException("Field references must be singular names.");
    }
    return path;
  }

  public static FieldReference getWithQuotedRef(CharSequence safeString) {
    return new FieldReference(safeString, false);
  }


  @Override
  public CompleteType getCompleteType() {
    if(overrideType != null){
      return overrideType;
    } else {
      return super.getCompleteType();
    }
  }

  public static class De extends StdDeserializer<FieldReference> {
    private final SchemaPath.De deserializer = new SchemaPath.De();
    public De() {
      super(FieldReference.class);
    }

    @Override
    public FieldReference deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      SchemaPath sp = deserializer.deserialize(jp, ctxt);
      return new FieldReference(sp);
    }
  }

}
