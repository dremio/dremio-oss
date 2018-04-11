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

import static com.dremio.dac.proto.model.dataset.ConvertCase.LOWER_CASE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.IOException;

import org.junit.Test;

import com.dremio.dac.explore.model.TransformBase.TransformVisitor;
import com.dremio.dac.proto.model.dataset.FieldConvertCase;
import com.dremio.dac.proto.model.dataset.IndexType;
import com.dremio.dac.proto.model.dataset.TransformAddCalculatedField;
import com.dremio.dac.proto.model.dataset.TransformConvertCase;
import com.dremio.dac.proto.model.dataset.TransformConvertToSingleType;
import com.dremio.dac.proto.model.dataset.TransformCreateFromParent;
import com.dremio.dac.proto.model.dataset.TransformDrop;
import com.dremio.dac.proto.model.dataset.TransformExtract;
import com.dremio.dac.proto.model.dataset.TransformField;
import com.dremio.dac.proto.model.dataset.TransformFilter;
import com.dremio.dac.proto.model.dataset.TransformGroupBy;
import com.dremio.dac.proto.model.dataset.TransformJoin;
import com.dremio.dac.proto.model.dataset.TransformLookup;
import com.dremio.dac.proto.model.dataset.TransformRename;
import com.dremio.dac.proto.model.dataset.TransformSort;
import com.dremio.dac.proto.model.dataset.TransformSorts;
import com.dremio.dac.proto.model.dataset.TransformSplitByDataType;
import com.dremio.dac.proto.model.dataset.TransformTrim;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Tests for TransformBase
 */
public class TestTransformBase {

  @Test
  public void test() throws Exception {
    TransformBase transform = new TransformExtract("source", "new", DatasetsUtil.pattern("\\d+", 0, IndexType.INDEX), false);
    validate(transform);
  }

  @Test
  public void testConvert() throws Exception {
    TransformBase transform = new TransformField("source", "new", false, new FieldConvertCase(LOWER_CASE).wrap());
    validate(transform);
  }

  @Test
  public void testToTransform() throws Exception {
    TransformBase transform = new TransformExtract("source", "new", DatasetsUtil.pattern("\\d+", 0, IndexType.INDEX), false);
    TransformBase actual = TransformBase.unwrap(transform.wrap());
    assertSame(transform, actual);
  }

  @Test
  public void testVisitor() {
    TransformBase transform = new TransformExtract("source", "new", DatasetsUtil.pattern("\\d+", 0, IndexType.INDEX), false);
    String name = transform.accept(new TransformVisitor<String>() {
      @Override
      public String visit(TransformLookup lookup) throws Exception {
        return "lookup";
      }
      @Override
      public String visit(TransformJoin join) throws Exception {
        return "join";
      }
      @Override
      public String visit(TransformSort sort) throws Exception {
        return "sort";
      }
      @Override
      public String visit(TransformSorts sortMultiple) throws Exception {
        return "sortMultiple";
      }
      @Override
      public String visit(TransformDrop drop) throws Exception {
        return "drop";
      }
      @Override
      public String visit(TransformRename rename) throws Exception {
        return "rename";
      }
      @Override
      public String visit(TransformConvertCase convertCase) throws Exception {
        return "convertCase";
      }
      @Override
      public String visit(TransformTrim trim) throws Exception {
        return "trim";
      }
      @Override
      public String visit(TransformExtract extract) throws Exception {
        return "extract";
      }
      @Override
      public String visit(TransformAddCalculatedField addCalculatedField) throws Exception {
        return "addCalculatedField";
      }
      @Override
      public String visit(TransformUpdateSQL updateSQL) throws Exception {
        return "updateSQL";
      }
      @Override
      public String visit(TransformField field) throws Exception {
        return "field";
      }
      @Override
      public String visit(TransformConvertToSingleType convertToSingleType) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(TransformSplitByDataType splitByDataType) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(TransformGroupBy groupBy) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(TransformFilter filter) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
      @Override
      public String visit(TransformCreateFromParent createFromParent) throws Exception {
        throw new UnsupportedOperationException("NYI");
      }
    });
    assertEquals("extract", name);
  }

  private void validate(TransformBase t)
      throws JsonProcessingException, IOException, JsonParseException, JsonMappingException {
    ObjectMapper om = JSONUtil.prettyMapper();
    String value = om.writerWithDefaultPrettyPrinter().writeValueAsString(t);
    TransformBase readValue = om.readValue(value, TransformBase.class);
    assertEquals(value, t, readValue);
  }

}
