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
package com.dremio.dac.explore.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.IOException;

import org.junit.Test;

import com.dremio.dac.proto.model.dataset.FieldConvertCase;
import com.dremio.dac.proto.model.dataset.FieldConvertDateToNumber;
import com.dremio.dac.proto.model.dataset.FieldConvertDateToText;
import com.dremio.dac.proto.model.dataset.FieldConvertFloatToDecimal;
import com.dremio.dac.proto.model.dataset.FieldConvertFloatToInteger;
import com.dremio.dac.proto.model.dataset.FieldConvertFromJSON;
import com.dremio.dac.proto.model.dataset.FieldConvertListToText;
import com.dremio.dac.proto.model.dataset.FieldConvertNumberToDate;
import com.dremio.dac.proto.model.dataset.FieldConvertTextToDate;
import com.dremio.dac.proto.model.dataset.FieldConvertToJSON;
import com.dremio.dac.proto.model.dataset.FieldConvertToTypeIfPossible;
import com.dremio.dac.proto.model.dataset.FieldConvertToTypeWithPatternIfPossible;
import com.dremio.dac.proto.model.dataset.FieldExtract;
import com.dremio.dac.proto.model.dataset.FieldExtractList;
import com.dremio.dac.proto.model.dataset.FieldExtractMap;
import com.dremio.dac.proto.model.dataset.FieldReplaceCustom;
import com.dremio.dac.proto.model.dataset.FieldReplacePattern;
import com.dremio.dac.proto.model.dataset.FieldReplaceRange;
import com.dremio.dac.proto.model.dataset.FieldReplaceValue;
import com.dremio.dac.proto.model.dataset.FieldSimpleConvertToType;
import com.dremio.dac.proto.model.dataset.FieldSplit;
import com.dremio.dac.proto.model.dataset.FieldTrim;
import com.dremio.dac.proto.model.dataset.FieldUnnestList;
import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests the acceptor mapping
 */
public class TestFieldTransformationBase {
  @Test
  public void test() throws Exception {
    FieldTransformationBase exp = new FieldConvertToJSON();
    validate(exp);
  }

  @Test
  public void testToExpression() throws Exception {
    FieldTransformationBase exp = new FieldConvertToJSON();
    FieldTransformationBase actual = FieldTransformationBase.unwrap(exp.wrap());
    assertSame(exp, actual);
  }

  @Test
  public void testVisitor() {
    FieldTransformationBase exp = new FieldConvertToJSON();
    String name = exp.accept(new FieldTransformationBase.FieldTransformationVisitor<String>() {
      @Override
      public String visit(FieldConvertCase col) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldTrim changeCase) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldExtract extract) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldConvertFloatToInteger trim) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldConvertFloatToDecimal calculatedField) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldConvertDateToText fieldTransformation) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldConvertNumberToDate numberToDate) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldConvertDateToNumber dateToNumber) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldConvertTextToDate textToDate) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldConvertListToText fieldTransformation) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldConvertToJSON fieldTransformation) throws Exception {
        return "json";
      }
      @Override
      public String visit(FieldUnnestList unnest) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldReplacePattern replacePattern) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldReplaceCustom replacePattern) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldReplaceValue replacePattern) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldReplaceRange replaceRange) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldExtractMap extract) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldExtractList extract) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldSplit split) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldSimpleConvertToType toType) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(FieldConvertToTypeIfPossible toTypeIfPossible) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }

      @Override
      public String visit(FieldConvertToTypeWithPatternIfPossible toTypeIfPossible) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }

      @Override
      public String visit(FieldConvertFromJSON fromJson) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
    });
    assertEquals("json", name);
  }

  private void validate(FieldTransformationBase e)
      throws JsonProcessingException, IOException, JsonParseException, JsonMappingException {
    ObjectMapper om = JSONUtil.mapper();
    String value = om.writeValueAsString(e);
    FieldTransformationBase readValue = om.readValue(value, FieldTransformationBase.class);
    assertEquals(e, readValue);
  }
}
