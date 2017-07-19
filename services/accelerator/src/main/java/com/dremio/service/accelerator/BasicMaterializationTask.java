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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.JobDetails;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationId;
import com.dremio.service.accelerator.proto.MaterializationMetrics;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.collect.ImmutableList;

/**
 * Basic materialization task, which does a full rewrite of the materialization
 */
public class BasicMaterializationTask extends MaterializationTask {
  private static final Logger logger = LoggerFactory.getLogger(BasicMaterializationTask.class);

  public BasicMaterializationTask(final String acceleratorStorageName, final MaterializationStore materializationStore,
                                  JobsService jobsService, NamespaceService namespaceService, CatalogService catalogService, Layout layout, ExecutorService executorService, Acceleration acceleration,
                                  final FileSystemPlugin plugin) {
    super(acceleratorStorageName, materializationStore, jobsService, namespaceService, catalogService, layout, executorService, acceleration, plugin);
  }

  @Override
  protected String getCtasSql(List<String> source, MaterializationId id, Materialization materialization) {


    final String viewSql = String.format("SELECT * FROM %s", SqlUtils.quotedCompound(source));

    final List<String> ctasDest = ImmutableList.of(
        getAcceleratorStorageName(),
        String.format("%s/%s", getLayout().getId().getId(), id.getId())
        );

    final String ctasSql = getCTAS(ctasDest, viewSql);

    logger.info(ctasSql);

    final List<String> destination = ImmutableList.of(
        getAcceleratorStorageName(),
        getLayout().getId().getId(),
        id.getId()
        );

    materialization.setPathList(destination);
    logger.info("Materialization path: {}", destination);
    return ctasSql;
  }

  @Override
  protected void handleJobComplete(final Materialization materialization, final AtomicReference<Job> jobRef) {
    materialization.setUpdateId(-1L);
    save(materialization);
    createMetadata(materialization);
    if (getNext() != null) {
      getExecutorService().submit(getNext());
    }
  }

  @Override
  protected Materialization getMaterialization() {
    Materialization materialization;
    final MaterializationId id = newRandomId();
    materialization = new Materialization()
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
    return materialization;
  }
}
