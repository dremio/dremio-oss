/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.utils.SqlUtils;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.JobDetails;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationId;
import com.dremio.service.accelerator.proto.MaterializationMetrics;
import com.dremio.service.accelerator.proto.MaterializationUpdate;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.jobs.JobData;
import com.dremio.service.jobs.JobDataFragment;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 *  Materialization task for incrementally updated datasets
 */
public class IncrementalMaterializationTask extends MaterializationTask {
  private static final Logger logger = LoggerFactory.getLogger(IncrementalMaterializationTask.class);

  private long timeStamp;
  private boolean fullRefresh;

  public IncrementalMaterializationTask(MaterializationContext materializationContext, Layout layout, Acceleration acceleration, long gracePeriod) {
    super(materializationContext, layout, acceleration, gracePeriod);
  }

  /**
   * The materialization has a root path, which is the path used by the accelerator when a substitution is made. The root
   * path contains cumulative data for all updates. Each update has a path to a directory within the root directory which
   * is the target directory for a particular update task
   *
   * @param source
   * @param id
   * @param materialization
   * @return
   */
  @Override
  protected String getCtasSql(List<String> source, MaterializationId id, Materialization materialization) {
    timeStamp = System.currentTimeMillis();

    final String viewSql = String.format("SELECT * FROM %s", SqlUtils.quotedCompound(source));

    final List<String> ctasDest = ImmutableList.of(
        getContext().getAcceleratorStorageName(),
        String.format("%s/%s/%s", getLayout().getId().getId(), id.getId(), timeStamp)
        );

    final String ctasSql = getCTAS(ctasDest, viewSql);

    logger.info(ctasSql);



    final List<String> destination = ImmutableList.of(
        getContext().getAcceleratorStorageName(),
        getLayout().getId().getId(),
        id.getId());

    materialization.setPathList(destination);
    logger.info("Materialization path: {}", destination);

    return ctasSql;
  }

  /**
   * For incremental updates, we need to get the latest max $updateId, which is returned as the result of the CTAS query
   * So we submit a task to wait for the result and then update the materialization
   */
  @Override
  public void updateMetadataAndSave(long refreshChainStartTime) {
    final Materialization materialization = getMaterialization();

    long maxValue = Long.MIN_VALUE;
    JobData completeJobData = getJob().getData();
    long outputRecords = getJob().getJobAttempt().getStats().getOutputRecords();
    logger.debug("Materialization wrote {} records", outputRecords);
    int offset = 0;
    JobDataFragment data = completeJobData.range(offset, 1000);
    while (data.getReturnedRowCount() > 0) {
      for (int i = 0; i < data.getReturnedRowCount(); i++) {
        byte[] b = (byte[]) data.extractValue("Metadata", i);
        long val = Long.parseLong(new String(b));
        maxValue = Math.max(maxValue, val);
      }
      offset += data.getReturnedRowCount();
      data = completeJobData.range(offset, 1000);
    }

    // update the update id if new records were written.
    if (outputRecords > 0) {
      materialization.setUpdateId(maxValue);
    }
    List<MaterializationUpdate> updates = FluentIterable.from(materialization.getUpdatesList())
      .append(new MaterializationUpdate().setJobId(getJob().getJobId()).setTimestamp(timeStamp))
      .toList();
    materialization.setUpdatesList(updates);
    materialization.setRefreshChainStartTime(refreshChainStartTime);
    save(materialization);

    if (outputRecords > 0) {
      if (fullRefresh) {
        createMetadata(materialization);
      } else {
        refreshMetadata(materialization);
      }
    }
  }

  /**
   * If the layout has already been materialized, we simply augment the existing materialization. If this is the first
   * materialization task, we create a new materialization
   * @return
   */
  @Override
  protected Materialization getOrCreateMaterialization() {
    final Optional<MaterializedLayout> materializedLayout = getContext().getMaterializationStore().get(getLayout().getId());
    final List<Materialization> materializations = AccelerationUtils.selfOrEmpty(AccelerationUtils.getAllMaterializations(materializedLayout));
    if (!materializations.isEmpty()) {
      final Materialization materialization = materializations.get(0);
      logger.info("incremental refresh with updateId {} for layout {}", materialization.getUpdateId(), getLayout().getId());
      fullRefresh = false;
      return materialization;
    }

    logger.info("full incremental refresh for layout {}", getLayout().getId());
    fullRefresh = true;
    final MaterializationId id = newRandomId();
    return new Materialization()
      // unique materialization id
      .setId(id)
      // owning layout id
      .setLayoutId(getLayout().getId())
      // initialize job
      .setJob(new JobDetails())
      // initialize metrics
      .setMetrics(new MaterializationMetrics())
      // set version
      .setLayoutVersion(getLayout().getVersion())
      // initialize updates
      .setUpdatesList(ImmutableList.<MaterializationUpdate>of());
  }
}
