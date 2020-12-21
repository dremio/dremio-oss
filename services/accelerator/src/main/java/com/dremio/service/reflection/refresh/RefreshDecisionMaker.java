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
package com.dremio.service.reflection.refresh;

import java.util.List;

import org.apache.calcite.rel.RelNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.acceleration.PlanHasher;
import com.dremio.exec.planner.serialization.LogicalPlanSerializer;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.job.proto.ScanPath;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.IncrementalUpdateServiceUtils;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;

import io.protostuff.ByteString;

class RefreshDecisionMaker {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RefreshDecisionMaker.class);

  /**
   * Determine whether the provided materialization will be a partial or a full along with associated updateId, seriesId, etc.
   * @return The refresh decisions made
   */
  static RefreshDecision getRefreshDecision(
      ReflectionEntry entry,
      Materialization materialization,
      ReflectionSettings reflectionSettings,
      NamespaceService namespace,
      MaterializationStore materializationStore,
      RelNode plan,
      RelNode strippedPlan,
      Iterable<DremioTable> requestedTables,
      RelSerializerFactory serializerFactory,
      boolean strictRefresh,
      boolean forceFullUpdate) {

    final long newSeriesId = System.currentTimeMillis();

    final RefreshDecision decision = new RefreshDecision();

    // We load settings here to determine what type of update we need to do (full or incremental)
    final AccelerationSettings settings = IncrementalUpdateServiceUtils.extractRefreshSettings(strippedPlan, reflectionSettings);

    decision.setAccelerationSettings(settings);

    if (requestedTables != null && !Iterables.isEmpty(requestedTables)) {
      // store all physical dataset paths in the refresh decision
      final List<ScanPath> scanPathsList = FluentIterable.from(requestedTables)
        .filter(new Predicate<DremioTable>() {
          @Override
          public boolean apply(DremioTable table) {
            final DatasetConfig dataset = table.getDatasetConfig();
            return dataset != null && ReflectionUtils.isPhysicalDataset(dataset.getType());
          }
        }).transform(new Function<DremioTable, ScanPath>() {
          @Override
          public ScanPath apply(DremioTable table) {
            final List<String> datasetPath = table.getPath().getPathComponents();
            return new ScanPath().setPathList(datasetPath);
          }
        }).toList();
      decision.setScanPathsList(scanPathsList);
    }

    final LogicalPlanSerializer serializer = serializerFactory.getSerializer(plan.getCluster());
    decision.setLogicalPlan(ByteString.copyFrom(serializer.serializeToBytes(plan)));
    decision.setLogicalPlanStrippedHash(PlanHasher.hash(strippedPlan));

    if(settings.getMethod() == RefreshMethod.FULL) {
      logger.trace("Incremental either not set or not supported for this query.");
      return decision.setInitialRefresh(true)
          .setSeriesId(newSeriesId);
    }

    // This is an incremental update dataset.
    // if we already have valid refreshes, we should use the their seriesId
    final Refresh refresh = materializationStore.getMostRecentRefresh(materialization.getReflectionId());

    final Integer entryDatasetHash;
    final Integer decisionDatasetHash;
    try {
      final DatasetConfig dataset = namespace.findDatasetByUUID(entry.getDatasetId());
      if (!strictRefresh) {
        if (entry.getShallowDatasetHash() == null && refresh != null) {
          decisionDatasetHash = ReflectionUtils.computeDatasetHash(dataset, namespace, false);
          decision.setDatasetHash(ReflectionUtils.computeDatasetHash(dataset, namespace, true));
          entryDatasetHash = entry.getDatasetHash();
        } else {
          decisionDatasetHash = ReflectionUtils.computeDatasetHash(dataset, namespace, true);
          decision.setDatasetHash(decisionDatasetHash);
          entryDatasetHash = entry.getShallowDatasetHash();
        }
      } else {
        decisionDatasetHash = ReflectionUtils.computeDatasetHash(dataset, namespace, false);
        decision.setDatasetHash(decisionDatasetHash);
        entryDatasetHash = entry.getDatasetHash();
      }
    } catch (Exception e) {
      throw UserException.validationError()
        .message("Couldn't expand a materialized view on a non existing dataset")
        .addContext("reflectionId", entry.getId().getId())
        .addContext("datasetId", entry.getDatasetId())
        .build(logger);
    }

    // if this the first refresh of this materialization, let's do a initial refresh.
    if(refresh == null) {
      logger.trace("No existing refresh, doing an initial refresh.");
      return decision.setInitialRefresh(true)
          .setUpdateId(new UpdateId())
          .setSeriesId(newSeriesId);
    }

    if (forceFullUpdate) {
      logger.trace("Forcing full update.");
      return decision.setInitialRefresh(true)
        .setUpdateId(new UpdateId())
        .setSeriesId(newSeriesId);
    }

    // if the refresh settings changed, do an initial refresh.
    if (entry.getRefreshMethod() != settings.getMethod() || !Objects.equal(entry.getRefreshField(), settings.getRefreshField())) {
      logger.trace("Change in refresh method, doing an initial refresh.");
      return decision.setInitialRefresh(true)
          .setUpdateId(new UpdateId())
          .setSeriesId(newSeriesId);
    }

    if (!Objects.equal(entryDatasetHash, decisionDatasetHash)) {
      logger.trace("Change in dataset hash, doing an initial refresh.");
      return decision.setInitialRefresh(true)
          .setUpdateId(new UpdateId())
          .setSeriesId(newSeriesId);
    }

    return decision.setInitialRefresh(false)
        .setUpdateId(refresh.getUpdateId())
        .setSeriesId(refresh.getSeriesId())
        .setSeriesOrdinal(refresh.getSeriesOrdinal() + 1);
  }

}
