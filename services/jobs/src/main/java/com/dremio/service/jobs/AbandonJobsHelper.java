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
package com.dremio.service.jobs;

import com.dremio.common.logging.StructuredLogger;
import com.dremio.common.utils.protos.AttemptIdUtils;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.proto.beans.NodeEndpoint;
import com.dremio.service.job.NodeStatusRequest;
import com.dremio.service.job.NodeStatusResponse;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.jobtelemetry.DeleteProfileRequest;
import com.dremio.service.jobtelemetry.JobTelemetryServiceGrpc;
import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper methods for terminating abandoned jobs due to coordinator restart */
public final class AbandonJobsHelper {
  private static final Logger logger = LoggerFactory.getLogger(AbandonJobsHelper.class);

  private AbandonJobsHelper() {}

  @VisibleForTesting
  @WithSpan("terminate-abandoned-jobs-due-to-coordinator-death")
  static void setAbandonedJobsToFailedState(
      JobTelemetryServiceGrpc.JobTelemetryServiceBlockingStub jobTelemetryServiceStub,
      LegacyIndexedStore<JobId, JobResult> jobStore,
      Collection<CoordinationProtos.NodeEndpoint> coordinators,
      StructuredLogger<Job> jobResultLogger,
      RemoteJobServiceForwarder forwarder,
      NodeEndpoint identity,
      long ttlExpireAtInMillis) {
    logger.debug("Running abandon jobs task");
    final Set<Map.Entry<JobId, JobResult>> apparentlyAbandoned =
        StreamSupport.stream(
                jobStore
                    .find(
                        new LegacyIndexedStore.LegacyFindByCondition()
                            .setCondition(JobsServiceUtil.getApparentlyAbandonedQuery()))
                    .spliterator(),
                false)
            .collect(Collectors.toSet());
    final Set<CoordinationProtos.NodeEndpoint> coordEndpoints = new HashSet<>(coordinators);
    final Map<CoordinationProtos.NodeEndpoint, Boolean> coordStatus = new HashMap<>();
    for (final Map.Entry<JobId, JobResult> entry : apparentlyAbandoned) {
      logger.debug("{} Checking if job is abandoned", entry.getKey().getId());
      final JobResult jobResult = entry.getValue();
      final List<JobAttempt> attempts = jobResult.getAttemptsList();
      final int numAttempts = attempts.size();
      if (numAttempts > 0) {
        final JobAttempt lastAttempt = attempts.get(numAttempts - 1);
        // .. check again; the index may not be updated, but the store maybe
        // set to failed only if issuing coordinator is no longer present
        boolean shouldAbandon =
            JobsServiceUtil.isNonFinalState(lastAttempt.getState())
                && !coordEndpoints.contains(JobsServiceUtil.toPB(lastAttempt.getEndpoint()))
                && !isNodeActive(
                    JobsServiceUtil.toPB(lastAttempt.getEndpoint()),
                    forwarder,
                    identity,
                    coordStatus);
        if (shouldAbandon) {
          // Delete sub-profiles related to the corresponding queryId before terminating the job.
          for (JobAttempt attempt : attempts) {
            UserBitShared.QueryId queryId =
                AttemptIdUtils.fromString(attempt.getAttemptId()).toQueryId();
            DeleteProfileRequest request =
                DeleteProfileRequest.newBuilder()
                    .setQueryId(queryId)
                    .setOnlyDeleteSubProfiles(true)
                    .build();
            try {
              jobTelemetryServiceStub.deleteProfile(request);
            } catch (Exception e) {
              logger.warn(
                  "Not able to delete sub profiles for queryId {}",
                  QueryIdHelper.getQueryId(queryId),
                  e);
            }
          }
          logger.debug("{} Failing abandoned job", lastAttempt.getInfo().getJobId().getId());
          final long finishTimestamp = System.currentTimeMillis();
          List<com.dremio.exec.proto.beans.AttemptEvent> attemptEventList =
              lastAttempt.getStateListList();
          if (attemptEventList == null) {
            attemptEventList = new ArrayList<>();
          }
          attemptEventList.add(
              JobsServiceUtil.createAttemptEvent(AttemptEvent.State.FAILED, finishTimestamp));
          JobInfo jobInfo = lastAttempt.getInfo();
          jobInfo
              .setFinishTime(finishTimestamp)
              .setFailureInfo(
                  "Query failed as Dremio was restarted. Details and profile information "
                      + "for this job may be missing.");
          if (jobInfo.getTtlExpireAt() == null) {
            jobInfo.setTtlExpireAt(ttlExpireAtInMillis);
          }
          final JobAttempt newLastAttempt =
              lastAttempt
                  .setState(JobState.FAILED)
                  .setStateListList(attemptEventList)
                  .setInfo(jobInfo);
          attempts.remove(numAttempts - 1);
          attempts.add(newLastAttempt);
          jobResult.setCompleted(true); // mark the job as completed
          jobResult.setProfileDetailsCapturedPostTermination(true);
          // mark ProfileDetailsCapturedPostTermination as true
          // Don't fetch missing profile for abandoned job
          jobStore.put(entry.getKey(), jobResult);
          Job job = new Job(entry.getKey(), jobResult);
          jobResultLogger.info(
              job,
              "Query: {}; outcome: {}",
              job.getJobId().getId(),
              job.getJobAttempt().getState());
        }
      }
    }
  }

  // verify the status of given node by making rpc
  // startTime in NodeEndpoint changes when node restarts
  // cache the status in coordStatus to reuse
  private static boolean isNodeActive(
      CoordinationProtos.NodeEndpoint targetEndpoint,
      RemoteJobServiceForwarder forwarder,
      NodeEndpoint identity,
      Map<CoordinationProtos.NodeEndpoint, Boolean> coordStatus) {
    if (coordStatus.containsKey(targetEndpoint)) {
      return coordStatus.get(targetEndpoint);
    }
    if (targetEndpoint.getAddress().equals(identity.getAddress())) {
      boolean isActive = targetEndpoint.getStartTime() == identity.getStartTime();
      coordStatus.put(targetEndpoint, isActive);
    } else {
      try {
        NodeStatusResponse response =
            forwarder.getNodeStatus(targetEndpoint, NodeStatusRequest.getDefaultInstance());
        boolean isActive = targetEndpoint.getStartTime() == response.getStartTime();
        coordStatus.put(targetEndpoint, isActive);
      } catch (Throwable e) {
        // rpc fails with UNAVAILABLE, DEADLINE_EXCEEDED if node is not present
        coordStatus.put(targetEndpoint, false);
      }
    }
    logger.debug(
        "isNodeActive for {} : {}", targetEndpoint.getAddress(), coordStatus.get(targetEndpoint));
    return coordStatus.get(targetEndpoint);
  }
}
