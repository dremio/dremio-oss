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
package com.dremio.dac.explore;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;

import javax.ws.rs.core.SecurityContext;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.History;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.Transform;
import com.dremio.dac.proto.model.dataset.TransformType;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.dataset.DatasetVersion;

/**
 * Tests for DatasetTool
 */
public class TestDatasetTool {
  @Test
  public void testBrokenHistory() throws Exception {
    DatasetPath datasetPath = new DatasetPath(Arrays.asList("space", "dataset"));
    DatasetVersion current = new DatasetVersion("123");
    DatasetVersion tip = new DatasetVersion("456");
    DatasetVersion broken = new DatasetVersion("001");

    // tip dataset whose previous version points at an non existent history
    VirtualDatasetUI tipDataset = new VirtualDatasetUI();
    tipDataset.setCreatedAt(0L);
    tipDataset.setFullPathList(datasetPath.toPathList());
    tipDataset.setVersion(tip);
    tipDataset.setPreviousVersion(new NameDatasetRef()
      .setDatasetVersion(broken.getVersion())
      .setDatasetPath(datasetPath.toString()));
    Transform transform = new Transform(TransformType.updateSQL);
    transform.setUpdateSQL(new TransformUpdateSQL("sql"));
    tipDataset.setLastTransform(transform);

    DatasetVersionMutator datasetVersionMutator = mock(DatasetVersionMutator.class);
    // the tip history request
    when(datasetVersionMutator.getVersion(datasetPath, tip)).thenReturn(tipDataset);
    when(datasetVersionMutator.get(any())).thenReturn(tipDataset);
    when(datasetVersionMutator.getVersion(datasetPath, broken)).thenThrow(DatasetNotFoundException.class);

    JobsService jobsService = mock(JobsService.class);
    when(jobsService.searchJobs(any())).thenReturn(Collections.emptyList());
    QueryExecutor executor = mock(QueryExecutor.class);

    SecurityContext securityContext = new SecurityContext() {
      @Override
      public Principal getUserPrincipal() {
        return new Principal() {
          @Override
          public String getName() {
            return "user";
          }
        };
      }

      @Override
      public boolean isUserInRole(String role) {
        return false;
      }

      @Override
      public boolean isSecure() {
        return false;
      }

      @Override
      public String getAuthenticationScheme() {
        return null;
      }
    };

    final DatasetTool tool = new DatasetTool(datasetVersionMutator, jobsService, executor, securityContext);

    History history = tool.getHistory(datasetPath, current, tip);
    Assert.assertEquals(1, history.getItems().size());
  }
}
