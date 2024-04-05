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

import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.util.LongRange;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.Test;

/** Tests for {@link ManifestScanFilters} */
public class TestManifestScanFilters {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final LongRange TEST_FILE_SIZE_RANGE = new LongRange(100_000L, 300_000L);
  private static final Expression TEST_EXPR = Expressions.equal("key", "value");

  @Test
  public void testJsonSerDe() throws JsonProcessingException {
    ManifestScanFilters raw = newInstance(TEST_EXPR, TEST_FILE_SIZE_RANGE, 1);

    String json = OBJECT_MAPPER.writeValueAsString(raw);

    ManifestScanFilters deserialized = OBJECT_MAPPER.readValue(json, ManifestScanFilters.class);
    assertThat(raw.getIcebergAnyColExpressionDeserialized().toString())
        .isEqualTo(deserialized.getIcebergAnyColExpressionDeserialized().toString());
    assertThat(raw.getSkipDataFileSizeRange()).isEqualTo(deserialized.getSkipDataFileSizeRange());
    assertThat(raw.getMinPartitionSpecId()).isEqualTo(deserialized.getMinPartitionSpecId());
  }

  @Test
  public void testEmptyScanFilters() {
    ManifestScanFilters empty = ManifestScanFilters.empty();

    assertThat(empty.doesSkipDataFileSizeRangeExist()).isFalse();
    assertThat(empty.doesIcebergAnyColExpressionExists()).isFalse();
    assertThat(empty.doesMinPartitionSpecIdExist()).isFalse();

    assertThat(empty.getSkipDataFileSizeRange()).isNull();
    assertThat(empty.getIcebergAnyColExpression()).isNull();
    assertThat(empty.getMinPartitionSpecId()).isEqualTo(0);

    ManifestScanFilters emptyValues = newInstance(null, null, 0);

    assertThat(emptyValues.doesSkipDataFileSizeRangeExist()).isFalse();
    assertThat(emptyValues.doesIcebergAnyColExpressionExists()).isFalse();
    assertThat(emptyValues.doesMinPartitionSpecIdExist()).isFalse();

    assertThat(emptyValues.getSkipDataFileSizeRange()).isNull();
    assertThat(emptyValues.getIcebergAnyColExpression()).isNull();
    assertThat(emptyValues.getMinPartitionSpecId()).isEqualTo(0);
  }

  @Test
  public void testIcebergExpressionVariants() {
    ManifestScanFilters nullExpr = newInstance(null, TEST_FILE_SIZE_RANGE, 1);

    assertThat(nullExpr.doesIcebergAnyColExpressionExists()).isFalse();
    assertThat(nullExpr.getIcebergAnyColExpressionDeserialized()).isNull();
    assertThat(nullExpr.getIcebergAnyColExpression()).isNull();

    ManifestScanFilters withExpr = newInstance(TEST_EXPR, TEST_FILE_SIZE_RANGE, 1);

    assertThat(withExpr.doesIcebergAnyColExpressionExists()).isTrue();
    assertThat(withExpr.getIcebergAnyColExpression()).isNotNull();
    assertThat(TEST_EXPR.toString())
        .isEqualTo(withExpr.getIcebergAnyColExpressionDeserialized().toString());
  }

  @Test
  public void testSkipDataFileSizeRange() {
    ManifestScanFilters nullRange = newInstance(TEST_EXPR, null, 1);

    assertThat(nullRange.doesSkipDataFileSizeRangeExist()).isFalse();
    assertThat(nullRange.getSkipDataFileSizeRange()).isNull();

    ManifestScanFilters withRange = newInstance(TEST_EXPR, TEST_FILE_SIZE_RANGE, 1);

    assertThat(withRange.doesSkipDataFileSizeRangeExist()).isTrue();
    assertThat(TEST_FILE_SIZE_RANGE).isEqualTo(withRange.getSkipDataFileSizeRange());
  }

  @Test
  public void testMinPartitionSpecId() {
    ManifestScanFilters noSpecId = newInstance(TEST_EXPR, null, 0);

    assertThat(noSpecId.doesMinPartitionSpecIdExist()).isFalse();
    assertThat(noSpecId.getMinPartitionSpecId()).isEqualTo(0);

    ManifestScanFilters withSpecId = newInstance(TEST_EXPR, TEST_FILE_SIZE_RANGE, 2);

    assertThat(withSpecId.doesMinPartitionSpecIdExist()).isTrue();
    assertThat(withSpecId.getMinPartitionSpecId()).isEqualTo(2);
  }

  private ManifestScanFilters newInstance(
      Expression expr, LongRange longRange, Integer partitionSpecId) {
    ImmutableManifestScanFilters.Builder manifestScanBuilders =
        new ImmutableManifestScanFilters.Builder();
    if (expr != null) {
      manifestScanBuilders.setIcebergAnyColExpression(
          IcebergSerDe.serializeToByteArrayUnchecked(expr));
    }
    return manifestScanBuilders
        .setMinPartitionSpecId(partitionSpecId)
        .setSkipDataFileSizeRange(longRange)
        .build();
  }
}
