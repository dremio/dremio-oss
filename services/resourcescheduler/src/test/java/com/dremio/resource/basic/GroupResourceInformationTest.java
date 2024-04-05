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
package com.dremio.resource.basic;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.options.OptionManager;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.SelectedExecutorsResourceInformation;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

/** Tests the functions of GroupResourceInformation. */
public class GroupResourceInformationTest {
  private final CoordinationProtos.NodeEndpoint ep1 =
      CoordinationProtos.NodeEndpoint.newBuilder()
          .setAddress("172.21.1.1")
          .setFabricPort(30000)
          .setAvailableCores(32)
          .build();
  private final CoordinationProtos.NodeEndpoint ep2 =
      CoordinationProtos.NodeEndpoint.newBuilder(ep1).setAddress("172.21.1.2").build();

  // If MAX_WIDTH_PER_NODE_KEY is set, it is given precedence.
  @Test
  public void configuredWidth() {
    final OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(GroupResourceInformation.MAX_WIDTH_PER_NODE)).thenReturn(16L);
    when(optionManager.getOption(GroupResourceInformation.MAX_WIDTH_PER_NODE_FRACTION))
        .thenReturn(0.9);

    GroupResourceInformation groupInfo =
        new SelectedExecutorsResourceInformation(ImmutableList.of(ep1, ep2));
    assertEquals(16, groupInfo.getAverageExecutorCores(optionManager));
  }

  @Test
  public void configuredWidthFraction() {
    final OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(GroupResourceInformation.MAX_WIDTH_PER_NODE)).thenReturn(0L);
    when(optionManager.getOption(GroupResourceInformation.MAX_WIDTH_PER_NODE_FRACTION))
        .thenReturn(0.25);

    GroupResourceInformation groupInfo =
        new SelectedExecutorsResourceInformation(ImmutableList.of(ep1, ep2));
    assertEquals(8, groupInfo.getAverageExecutorCores(optionManager));

    when(optionManager.getOption(GroupResourceInformation.MAX_WIDTH_PER_NODE_FRACTION))
        .thenReturn(0.75);
    assertEquals(24, groupInfo.getAverageExecutorCores(optionManager));
  }
}
