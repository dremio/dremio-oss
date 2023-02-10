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
package com.dremio.exec.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link OptionalVarBinaryVectorHolder}
 */
public class TestOptionalVarBinaryVectorHolder {
  private static final String TEST_COL_NAME = "testCol";
  private static BatchSchema TEST_SCHEMA = new BatchSchema(ImmutableList.of(
    Field.nullable(TEST_COL_NAME, Types.MinorType.VARBINARY.getType())
  ));

  private BufferAllocator testAllocator;

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(90, TimeUnit.SECONDS); // 90secs

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setupBeforeTest() {
    testAllocator = allocatorRule.newAllocator("test", 0, Long.MAX_VALUE);
  }

  @After
  public void cleanupAfterTest() {
    testAllocator.close();
  }

  @Test
  public void testExclusionCase() {
    try (VectorContainer vc = new VectorContainer(testAllocator)) {
      vc.addSchema(TEST_SCHEMA);
      vc.buildSchema();
      vc.setAllCount(2);

      OptionalVarBinaryVectorHolder expectedEmpty = new OptionalVarBinaryVectorHolder(vc, "invalidCol");

      Assertions.assertDoesNotThrow(() -> expectedEmpty.setSafe(0, () -> "VALUE".getBytes(StandardCharsets.UTF_8)));
      assertThat(expectedEmpty.get(0).isPresent()).isFalse();
    }
  }

  @Test
  public void testInclusionCase() {
    try (VarBinaryVector varBinaryVector = new VarBinaryVector(TEST_SCHEMA.findField(TEST_COL_NAME), testAllocator);
         VectorContainer vc = new VectorContainer(testAllocator)) {
      vc.add(varBinaryVector);
      vc.buildSchema();
      vc.setAllCount(1);

      byte[] value = "VALUE".getBytes(StandardCharsets.UTF_8);

      OptionalVarBinaryVectorHolder expected = new OptionalVarBinaryVectorHolder(vc, TEST_COL_NAME);

      Assertions.assertDoesNotThrow(() -> expected.setSafe(0, () -> value));
      assertThat(expected.get(0).isPresent()).isTrue();
      assertThat(Arrays.equals(expected.get(0).get(), value)).isTrue();
    }
  }
}
