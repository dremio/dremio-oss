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
package com.dremio.exec.store.easy.triggerpipe;

import com.dremio.exec.ingestion.IngestionSchema;
import com.dremio.exec.physical.config.copyinto.IngestionProperties;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.util.VectorUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;

/** Utility class for managing vectors used during trigger pipe scans. */
public final class TriggerPipeScanUtils {

  private VarCharVector pipeIdVector;
  private VarCharVector pipeNameVector;
  private VarCharVector requestIdVector;
  private VarCharVector ingestionSourceTypeVector;
  private BigIntVector modificationTimeVector;
  private final List<IngestionProperties> splitsIngestionProperties = new ArrayList<>();

  /**
   * Initializes the vectors used for storing pipe information.
   *
   * @param accessible The {@link VectorAccessible} providing access to the vectors.
   */
  public void initializeTriggerPipeIncomingVectors(VectorAccessible accessible) {
    pipeIdVector =
        (VarCharVector) VectorUtil.getVectorFromSchemaPath(accessible, IngestionSchema.PIPE_ID);
    pipeNameVector =
        (VarCharVector) VectorUtil.getVectorFromSchemaPath(accessible, IngestionSchema.PIPE_NAME);
    requestIdVector =
        (VarCharVector) VectorUtil.getVectorFromSchemaPath(accessible, IngestionSchema.REQUEST_ID);
    ingestionSourceTypeVector =
        (VarCharVector)
            VectorUtil.getVectorFromSchemaPath(accessible, IngestionSchema.INGESTION_SOURCE_TYPE);
    modificationTimeVector =
        (BigIntVector)
            VectorUtil.getVectorFromSchemaPath(accessible, IngestionSchema.MODIFICATION_TIME);
  }

  /**
   * Retrieves ingestion properties from the initialized vectors and stores it in class level list
   *
   * @param idx The index of the record
   */
  public void addSplitIngestionProperties(int idx) {
    splitsIngestionProperties.add(
        new IngestionProperties(
            pipeNameVector.isNull(idx) ? null : new String(pipeNameVector.get(idx)),
            pipeIdVector.isNull(idx) ? null : new String(pipeIdVector.get(idx)),
            ingestionSourceTypeVector.isNull(idx)
                ? null
                : new String(ingestionSourceTypeVector.get(idx)),
            requestIdVector.isNull(idx) ? null : new String(requestIdVector.get(idx)),
            modificationTimeVector.isNull(idx) ? null : modificationTimeVector.get(idx)));
  }

  public List<IngestionProperties> getSplitsIngestionProperties() {
    return splitsIngestionProperties;
  }
}
