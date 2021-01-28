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
package com.dremio.dac.util;

import static java.lang.String.format;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.WebApplicationException;

import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.explore.model.DownloadFormat;
import com.dremio.dac.service.datasets.DatasetDownloadManager;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.GetJobRequest;
import com.dremio.service.jobs.JobDataClientUtils;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.google.common.collect.ImmutableSet;

/**
 * Download Utils
 */
public class DownloadUtil {
  private static final Logger logger = LoggerFactory.getLogger(DownloadUtil.class);

  private static final String DOWNLOAD_POOL_SIZE = "dremio_download_pool_size";
  private static final Set<QueryType> JOB_TYPES_TO_DOWNLOAD = ImmutableSet.of(QueryType.UI_RUN,
    QueryType.UI_PREVIEW, QueryType.UI_INTERNAL_RUN, QueryType.UI_INTERNAL_PREVIEW, QueryType.UI_EXPORT);

  private static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(
    Integer.parseInt(System.getProperty(DOWNLOAD_POOL_SIZE, "5")),
    r -> new Thread(r, "job-download"));

  private final JobsService jobsService;
  private final DatasetVersionMutator datasetService;

  public DownloadUtil(JobsService jobsService, DatasetVersionMutator datasetService) {
    this.jobsService = jobsService;
    this.datasetService = datasetService;
  }

  public ChunkedOutput<byte[]> startChunckedDownload(JobId previewJobId, String currentUser, DownloadFormat downloadFormat,
                                                     long delay) throws JobNotFoundException {

    //first check that current user has access to preview data
    final GetJobRequest previewJobRequest = GetJobRequest.newBuilder()
      .setJobId(previewJobId)
      .setUserName(currentUser)
      .build();

    // ensure that we could access to the job.
    final JobDetails previewJobDetails = jobsService.getJobDetails(JobDetailsRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(previewJobId))
      .setUserName(currentUser)
      .build());
    final JobInfo previewJobInfo = JobsProtoUtil.getLastAttempt(previewJobDetails).getInfo();
    final List<String> datasetPath = previewJobInfo.getDatasetPathList();

    if (!JOB_TYPES_TO_DOWNLOAD.contains(previewJobInfo.getQueryType())) {
      logger.error("Not supported job type: {} for job '{}'. Supported job types are: {}",
        previewJobInfo.getQueryType(), previewJobId,
        String.join(", ", JOB_TYPES_TO_DOWNLOAD.stream()
          .map(queryType -> String.valueOf(queryType.getNumber()))
          .collect(Collectors.toList())));
      throw new IllegalArgumentException("Data for the job could not be downloaded");
    }

    final ChunkedOutput<byte[]> output = new ChunkedOutput<>(byte[].class);

    executorService.schedule(() -> {
      getJobResults(previewJobId, datasetPath, downloadFormat, currentUser, output);
    }, delay, TimeUnit.MILLISECONDS);

    return output;
  }

  private void getJobResults(JobId previewJobId, List<String> datasetPath, DownloadFormat downloadFormat, String currentUser, ChunkedOutput<byte[]> output) {
    try {
      JobDataClientUtils.waitForFinalState(jobsService, previewJobId);
      JobDetails previewJobDetails = jobsService.getJobDetails(JobDetailsRequest.newBuilder()
        .setJobId(JobsProtoUtil.toBuf(previewJobId))
        .setUserName(currentUser)
        .build());
      //read current job state
      checkJobCompletionState(previewJobDetails);

      JobDetails downloadJobDetails = previewJobDetails;
      final JobInfo previewJobInfo = JobsProtoUtil.getLastAttempt(previewJobDetails).getInfo();

      if (previewJobInfo.getQueryType() != QueryType.UI_EXPORT) {
        DatasetDownloadManager manager = datasetService.downloadManager();

        JobId downloadJobId = manager.scheduleDownload(datasetPath, previewJobInfo.getSql(),
          downloadFormat, previewJobInfo.getContextList(), currentUser, previewJobId);

        JobDataClientUtils.waitForFinalState(jobsService, downloadJobId);
        //get the final state of a job after completion
        JobDetailsRequest jobDetailsRequest = JobDetailsRequest.newBuilder()
          .setUserName(currentUser)
          .setJobId(JobsProtoUtil.toBuf(downloadJobId))
          .build();
        downloadJobDetails = jobsService.getJobDetails(jobDetailsRequest);
        checkJobCompletionState(downloadJobDetails);
      }

      final DatasetDownloadManager.DownloadDataResponse downloadDataResponse =
        datasetService.downloadData(JobsProtoUtil.getLastAttempt(downloadJobDetails).getInfo().getDownloadInfo(), currentUser);

      try (InputStream input = downloadDataResponse.getInput(); ChunkedOutput toClose = output) {
        byte[] buf = new byte[4096];
        int bytesRead = input.read(buf);
        while (bytesRead >= 0) {
          if (bytesRead < buf.length) {
            output.write(Arrays.copyOf(buf, bytesRead));
          } else {
            output.write(buf);
          }
          bytesRead = input.read(buf);
        }
      }
    } catch (JobNotFoundException | IOException e) {
      throw new WebApplicationException(e);
    }
  }

  private static void checkJobCompletionState(final JobDetails jobDetails) throws ForbiddenException {
    final JobState jobState = JobsProtoUtil.getLastAttempt(jobDetails).getState();
    final JobInfo jobInfo = JobsProtoUtil.getLastAttempt(jobDetails).getInfo();

    // requester should check that job state is COMPLETED before downloading, but return nicely
    switch (jobState) {
      case COMPLETED:
        // job is completed. Do nothing
        break;
      case FAILED:
        throw new ForbiddenException(jobInfo.getFailureInfo());
      case CANCELED:
        if (jobInfo.getCancellationInfo() != null) {
          throw new ForbiddenException(jobInfo.getCancellationInfo().getMessage());
        }
        throw new ForbiddenException("Download job was canceled, but further information is unavailable");
      default:
        throw new ForbiddenException(format("Could not download results (job: %s, state: %s)", jobDetails.getJobId(), jobState));
    }
  }
}
