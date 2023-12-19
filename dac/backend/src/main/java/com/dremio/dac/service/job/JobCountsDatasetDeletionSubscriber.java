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
package com.dremio.dac.service.job;

import javax.inject.Provider;

import com.dremio.service.jobcounts.DeleteJobCountsRequest;
import com.dremio.service.jobcounts.JobCountsClient;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvent;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusSubscriber;
import com.dremio.service.namespace.catalogstatusevents.events.DatasetDeletionCatalogStatusEvent;

public class JobCountsDatasetDeletionSubscriber implements CatalogStatusSubscriber {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobCountsDatasetDeletionSubscriber.class);

  private final Provider<JobCountsClient> jobCountsClientProvider;

  public JobCountsDatasetDeletionSubscriber(
    Provider<JobCountsClient> jobCountsClientProvider) {
    this.jobCountsClientProvider = jobCountsClientProvider;
  }

  @Override
  public void onCatalogStatusEvent(CatalogStatusEvent event) {
    if (!(event instanceof DatasetDeletionCatalogStatusEvent)) {
      return;
    }

    DatasetDeletionCatalogStatusEvent deletionEvent = (DatasetDeletionCatalogStatusEvent) event;
    try {
      jobCountsClientProvider.get().getBlockingStub()
        .deleteJobCounts(DeleteJobCountsRequest.newBuilder().addIds(deletionEvent.getDatasetPath()).build());
    } catch (NullPointerException ex) {
      // TODO - DX-85600 - Handle the initialization of system plugins more gracefully, so there is no null
      //  JobCountsClient
      logger.debug("Dataset deletion event had a null pointer exception thrown.");
    }
  }
}
