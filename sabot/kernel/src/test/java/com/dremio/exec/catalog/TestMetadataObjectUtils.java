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
package com.dremio.exec.catalog;

import static org.junit.Assert.assertEquals;

import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import java.util.Optional;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;

/** Test information schema catalog. */
public class TestMetadataObjectUtils {
  private static final BatchSchema BATCH_SCHEMA =
      BatchSchema.of(Field.nullablePrimitive("field", ArrowType.Bool.INSTANCE));
  private DatasetConfig oldConfig;

  /** Dummy class for testing with a DatasetMetadata instance */
  abstract static class DummyDatasetMetadata implements DatasetMetadata {
    @Override
    public Schema getRecordSchema() {
      return BATCH_SCHEMA;
    }
  }

  @Before
  public void setUp() {
    oldConfig = new DatasetConfig().setSchemaVersion(1);
  }

  @Test
  public void testOverrideExtendedHasRowCountSupplied() {
    final DatasetMetadata datasetMetadata =
        new DummyDatasetMetadata() {
          @Override
          public DatasetStats getDatasetStats() {
            // This should should throw when asking for record count.
            return DatasetStats.of(ScanCostFactor.OTHER.getFactor());
          }
        };

    final long expectedRecordCount = 999L;
    MetadataObjectsUtils.overrideExtended(
        oldConfig, datasetMetadata, Optional.empty(), expectedRecordCount, 1000);

    assertEquals(
        expectedRecordCount, (long) oldConfig.getReadDefinition().getScanStats().getRecordCount());
  }

  @Test
  public void testOverrideExtendedDoesNotHaveRowCountSupplied() {
    final long expectedRecordCount = 800L;
    final DatasetMetadata datasetMetadata =
        new DummyDatasetMetadata() {
          @Override
          public DatasetStats getDatasetStats() {
            // This should should throw when asking for record count.
            return DatasetStats.of(expectedRecordCount, ScanCostFactor.OTHER.getFactor());
          }
        };

    MetadataObjectsUtils.overrideExtended(oldConfig, datasetMetadata, Optional.empty(), -1L, 1000);
    assertEquals(
        expectedRecordCount, (long) oldConfig.getReadDefinition().getScanStats().getRecordCount());
  }
}
