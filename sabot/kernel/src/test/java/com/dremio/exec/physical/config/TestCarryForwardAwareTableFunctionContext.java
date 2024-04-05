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
package com.dremio.exec.physical.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.IcebergFileType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

/** Tests for {@link CarryForwardAwareTableFunctionContext} */
public class TestCarryForwardAwareTableFunctionContext {
  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testCarryForwardContext() throws JsonProcessingException {
    CarryForwardAwareTableFunctionContext ctx =
        new CarryForwardAwareTableFunctionContext(
            SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA,
            null,
            true,
            ImmutableMap.of(
                SchemaPath.getCompoundPath(SystemSchemas.SPLIT_IDENTITY, SplitIdentity.PATH),
                SchemaPath.getSimplePath(SystemSchemas.FILE_PATH)),
            SystemSchemas.FILE_TYPE,
            IcebergFileType.MANIFEST_LIST.name());

    String serialized = mapper.writeValueAsString(ctx);
    CarryForwardAwareTableFunctionContext deserialized =
        mapper.readValue(serialized, CarryForwardAwareTableFunctionContext.class);

    assertThat(deserialized)
        .extracting("isCarryForwardEnabled", "inputColMap", "constValCol", "constVal")
        .containsExactly(
            ctx.isCarryForwardEnabled(),
            ctx.getInputColMap(),
            ctx.getConstValCol(),
            ctx.getConstVal());
  }
}
