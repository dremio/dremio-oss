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
package com.dremio.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;

import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.service.coordinator.ClusterCoordinator;

public class TestMultiNodeSetup extends BaseTestQuery {

  private static final int NUM_NODES = 3;

  @BeforeClass
  public static void setupMultiNodeCluster() throws Exception {
    updateTestCluster(NUM_NODES, null);
  }

  @Test
  public void verifyRoles() throws Exception {
    assertEquals(NUM_NODES, nodes.length);

    // first node has all roles
    assertEquals(EnumSet.allOf(ClusterCoordinator.Role.class).size(),
        nodes[0].getContext().getRoles().size());
    assertTrue(nodes[0].getContext().getRoles()
        .containsAll(EnumSet.allOf(ClusterCoordinator.Role.class)));

    // all others are only executors
    for (int i = 1; i < nodes.length; i++) {
      assertEquals(1, nodes[i].getContext().getRoles().size());
      assertTrue(nodes[0].getContext().getRoles()
          .contains(ClusterCoordinator.Role.EXECUTOR));
    }
  }
}
