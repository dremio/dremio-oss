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

import com.dremio.dac.proto.model.dataset.ExpCalculatedField;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.ExpConvertCase;
import com.dremio.dac.proto.model.dataset.ExpConvertType;
import com.dremio.dac.proto.model.dataset.ExpExtract;
import com.dremio.dac.proto.model.dataset.ExpFieldTransformation;
import com.dremio.dac.proto.model.dataset.ExpMeasure;
import com.dremio.dac.proto.model.dataset.ExpTrim;
import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests the acceptor mapping
 */
public class TestExpressionBase {
  @Test
  public void test() throws Exception {
    ExpressionBase exp = new ExpCalculatedField("foo");
    validate(exp);
  }

  @Test
  public void testToExpression() throws Exception {
    ExpressionBase exp = new ExpCalculatedField("foo");
    ExpressionBase actual = ExpressionBase.unwrap(exp.wrap());
    assertSame(exp, actual);
  }

  @Test
  public void testVisitor() {
    ExpressionBase exp = new ExpCalculatedField("foo");
    String name = exp.accept(new ExpressionBase.ExpressionVisitor<String>() {
      @Override
      public String visit(ExpColumnReference col) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(ExpConvertCase changeCase) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(ExpExtract extract) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(ExpTrim trim) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(ExpCalculatedField calculatedField) throws Exception {
        return "calc";
      }
      @Override
      public String visit(ExpFieldTransformation fieldTransformation) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(ExpConvertType convertType) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(ExpMeasure measure) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
    });
    assertEquals("calc", name);
  }

  private void validate(ExpressionBase e)
      throws JsonProcessingException, IOException, JsonParseException, JsonMappingException {
    ObjectMapper om = JSONUtil.mapper();
    String value = om.writeValueAsString(e);
    ExpressionBase readValue = om.readValue(value, ExpressionBase.class);
    assertEquals(e, readValue);
  }
}
