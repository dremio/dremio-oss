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
import com.google.common.collect.ImmutableList;

/**
 * Basic materialization task, which does a full rewrite of the materialization
 */
public class BasicMaterializationTask extends MaterializationTask {
  private static final Logger logger = LoggerFactory.getLogger(BasicMaterializationTask.class);

  BasicMaterializationTask(final MaterializationContext materializationContext, Layout layout, Acceleration acceleration, long gracePeriod) {
    super(materializationContext, layout, acceleration, gracePeriod);
  }

  @Override
  protected String getCtasSql(List<String> source, MaterializationId id, Materialization materialization) {

    final String viewSql = String.format("SELECT * FROM %s", SqlUtils.quotedCompound(source));

    final List<String> ctasDest = ImmutableList.of(
        getContext().getAcceleratorStorageName(),
        String.format("%s/%s", getLayout().getId().getId(), id.getId())
        );

    final String ctasSql = getCTAS(ctasDest, viewSql);

    logger.info(ctasSql);

    final List<String> destination = ImmutableList.of(
        getContext().getAcceleratorStorageName(),
        getLayout().getId().getId(),
        id.getId()
        );

    materialization.setPathList(destination);
    logger.info("Materialization path: {}", destination);
    return ctasSql;
  }

  @Override
  public void updateMetadataAndSave(long refreshChainStartTime) {
    final Materialization materialization = getMaterialization();
    materialization.setUpdateId(-1L);
    materialization.setRefreshChainStartTime(refreshChainStartTime);
    save(materialization); //TODO move this to ChainExecutor
    createMetadata(materialization); //TODO move this to ChainExecutor ?
  }

  @Override
  protected Materialization getOrCreateMaterialization() {
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
      .setLayoutVersion(getLayout().getVersion());
  }
}
