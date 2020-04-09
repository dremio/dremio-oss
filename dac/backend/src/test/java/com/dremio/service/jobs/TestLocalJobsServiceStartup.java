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

import static com.dremio.service.jobs.JobsServiceUtil.finalJobStates;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Local job service tests for tasks on startup.
 */
public class TestLocalJobsServiceStartup {

  @SuppressWarnings("unchecked")
  @Test
  public void cleanupJobStateOnStartUp() throws Exception {
    final LegacyIndexedStore<JobId, JobResult> jobStore = (LegacyIndexedStore<JobId, JobResult>) mock(LegacyIndexedStore.class);
    when(jobStore.find(any(LegacyFindByCondition.class)))
        .thenReturn(FluentIterable.from(Sets.difference(EnumSet.allOf(JobState.class), finalJobStates))
            .transform(
                new Function<JobState, Entry<JobId, JobResult>>() {
                  @Override
                  public Entry<JobId, JobResult> apply(JobState input) {
                    return newJobResult(input);
                  }
                }));

    final List<JobResult> returns = Lists.newLinkedList();
    doAnswer(
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            returns.add(JobResult.class.cast(invocation.getArguments()[1]));
            return null;
          }
        })
        .when(jobStore)
        .put(any(JobId.class), any(JobResult.class));

    LocalJobsService.setAbandonedJobsToFailedState(jobStore);

    assertTrue("all job states must be final, or handled by the above method",
        returns.size() + finalJobStates.size() == JobState.values().length);
    for (JobResult result : returns) {
      assertTrue(result.getCompleted());
      assertEquals(result.getAttemptsList().get(0).getState(),
          JobState.FAILED);
      assertTrue(result.getAttemptsList()
          .get(0)
          .getInfo()
          .getFailureInfo()
          .contains("Query failed as Dremio was restarted"));
    }
  }

  private static Entry<JobId, JobResult> newJobResult(final JobState jobState) {
    return new Entry<JobId, JobResult>() {

      private final JobId id = new JobId(UUID.randomUUID().toString())
          .setName("job-name");

      private final JobResult jobResult = new JobResult()
          .setAttemptsList(Lists.newArrayList(new JobAttempt()
              .setAttemptId(UUID.randomUUID().toString())
              .setInfo(new JobInfo(id, "sql", "dataset-version", QueryType.UI_RUN))
              .setState(jobState)));

      @Override
      public JobId getKey() {
        return id;
      }

      @Override
      public JobResult getValue() {
        return jobResult;
      }

      @Override
      public JobResult setValue(JobResult value) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
