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
package com.dremio.service.accelerator;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.service.job.proto.JoinAnalysis;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.proto.DataPartition;
import com.dremio.service.reflection.store.MaterializationStore;

/**
 * Helper methods for extracting a list of {@link com.dremio.service.reflection.proto.Materialization} from a
 * {@link MaterializationStore} and converting them to a list of
 * {@link com.dremio.exec.store.sys.accel.AccelerationListManager.MaterializationInfo}.
 */
public class AccelerationMaterializationUtils {
  private static final Logger logger = LoggerFactory.getLogger(AccelerationMaterializationUtils.class);
  private static final Serializer<JoinAnalysis> JOIN_ANALYSIS_SERIALIZER = ProtostuffSerializer.of(JoinAnalysis.getSchema());

  private static String dataPartitionsToString(List<DataPartition> partitions) {
    if (partitions == null || partitions.isEmpty()) {
      return "";
    }

    final StringBuilder dataPartitions = new StringBuilder();
    for (int i = 0; i < partitions.size() - 1; i++) {
      dataPartitions.append(partitions.get(i).getAddress()).append(", ");
    }
    dataPartitions.append(partitions.get(partitions.size() - 1).getAddress());
    return dataPartitions.toString();
  }

  public static Iterable<AccelerationListManager.MaterializationInfo> getMaterializationsFromStore(MaterializationStore materializationStore) {
    return StreamSupport.stream(ReflectionUtils.getAllMaterializations(materializationStore).spliterator(), false)
      .map(materialization -> {
          long footPrint = -1L;
          try {
            footPrint = materializationStore.getMetrics(materialization).getFootprint();
          } catch (Exception e) {
            // let's not fail the query if we can't retrieve the footprint for one materialization
          }

          String joinAnalysisJson = null;
          try {
            if (materialization.getJoinAnalysis() != null) {
              joinAnalysisJson = JOIN_ANALYSIS_SERIALIZER.toJson(materialization.getJoinAnalysis());
            }
          } catch (IOException e) {
            logger.debug("Failed to serialize join analysis", e);
          }

          final String failureMsg = materialization.getFailure() != null ? materialization.getFailure().getMessage() : null;

          return new AccelerationListManager.MaterializationInfo(
            materialization.getReflectionId().getId(),
            materialization.getId().getId(),
            new Timestamp(materialization.getCreatedAt()),
            new Timestamp(Optional.ofNullable(materialization.getExpiration()).orElse(0L)),
            footPrint,
            materialization.getSeriesId(),
            materialization.getInitRefreshJobId(),
            materialization.getSeriesOrdinal(),
            joinAnalysisJson,
            materialization.getState().toString(),
            Optional.ofNullable(failureMsg).orElse("NONE"),
            dataPartitionsToString(materialization.getPartitionList()),
            new Timestamp(Optional.ofNullable(materialization.getLastRefreshFromPds()).orElse(0L))
          );
       }).collect(Collectors.toList());
  }
}
