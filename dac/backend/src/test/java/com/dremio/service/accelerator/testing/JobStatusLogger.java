/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.service.accelerator.testing;

import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.planner.PlannerPhase;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.metadata.QueryMetadata;

/**
 * Logger that reports status of test acceleration job submissions.
 */
public class JobStatusLogger implements JobStatusListener {

  private static final Logger logger = LoggerFactory.getLogger(JobStatusLogger.class);

  @Override
  public void jobSubmitted(JobId paramJobId) {
    logger.debug(paramJobId.getId() + ": " + "Job Submitted");
  }

  @Override
  public void planRelTransform(PlannerPhase paramPlannerPhase, RelNode paramRelNode1, RelNode paramRelNode2,
                               long paramLong) {
  }

  @Override
  public void metadataCollected(QueryMetadata paramQueryMetadata) {
  }

  @Override
  public void jobFailed(Exception paramException) {
    logger.error("Job failed." , paramException);
  }

  @Override
  public void jobCompleted() {
    logger.debug("Job completed");
  }

  @Override
  public void jobCancelled() {
    logger.debug("Job cancelled");
  }

}
