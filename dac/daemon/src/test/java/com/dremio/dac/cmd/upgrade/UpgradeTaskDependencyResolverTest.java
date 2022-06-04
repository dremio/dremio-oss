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
package com.dremio.dac.cmd.upgrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * To test dependency based sort
 */
public class UpgradeTaskDependencyResolverTest {

  @Test
  public void testDependencySort() {
    // no dependencies
    UpgradeTask1 upgradeTask1 = new UpgradeTask1(ImmutableList.of());
    // depends on 1
    UpgradeTask2 upgradeTask2 = new UpgradeTask2(ImmutableList.of(upgradeTask1.getTaskUUID()));
    // depends on 1 and 2
    UpgradeTask3 upgradeTask3 = new UpgradeTask3(ImmutableList.of(upgradeTask1.getTaskUUID(), upgradeTask2.getTaskUUID()));
    // depends on 3
    UpgradeTask4 upgradeTask4 = new UpgradeTask4(ImmutableList.of(upgradeTask3.getTaskUUID()));
    UpgradeTaskDependencyResolver upgradeTaskDependencyResolver = new UpgradeTaskDependencyResolver(
      ImmutableList.of(upgradeTask1, upgradeTask2, upgradeTask3, upgradeTask4)
    );

    List<UpgradeTask> sortedUpgradeTasks = upgradeTaskDependencyResolver.topologicalTasksSort();

    assertThat(sortedUpgradeTasks).hasSize(4);
    assertThat(sortedUpgradeTasks.get(0)).isInstanceOf(UpgradeTask1.class);
    assertThat(sortedUpgradeTasks.get(1)).isInstanceOf(UpgradeTask2.class);
    assertThat(sortedUpgradeTasks.get(2)).isInstanceOf(UpgradeTask3.class);
    assertThat(sortedUpgradeTasks.get(3)).isInstanceOf(UpgradeTask4.class);
  }

  @Test
  public void testDependencySortLargeTasksNumber() throws Exception {

    UpgradeTaskAny upgradeTaskAny1 = new UpgradeTaskAny("upgradeTaskAny1","5", ImmutableList.of());
    UpgradeTaskAny upgradeTaskAny2 = new UpgradeTaskAny("upgradeTaskAny2", "7", ImmutableList.of());
    UpgradeTaskAny upgradeTaskAny3 = new UpgradeTaskAny("upgradeTaskAny3","3", ImmutableList.of());
    UpgradeTaskAny upgradeTaskAny4 = new UpgradeTaskAny("upgradeTaskAny4", "11",
      ImmutableList.of(upgradeTaskAny1.getTaskUUID(), upgradeTaskAny2.getTaskUUID()));
    UpgradeTaskAny upgradeTaskAny5 = new UpgradeTaskAny("upgradeTaskAny5", "8",
      ImmutableList.of(upgradeTaskAny2.getTaskUUID(), upgradeTaskAny3.getTaskUUID()));
    UpgradeTaskAny upgradeTaskAny6 = new UpgradeTaskAny("upgradeTaskAny6","2", ImmutableList.of
      (upgradeTaskAny4.getTaskUUID()));
    UpgradeTaskAny upgradeTaskAny7 = new UpgradeTaskAny("upgradeTaskAny7","9",
      ImmutableList.of(upgradeTaskAny4.getTaskUUID(), upgradeTaskAny5.getTaskUUID()));
    UpgradeTaskAny upgradeTaskAny8 = new UpgradeTaskAny("upgradeTaskAny8", "10",
      ImmutableList.of(upgradeTaskAny4.getTaskUUID(), upgradeTaskAny3.getTaskUUID()));
    List<UpgradeTask> upgradeTasks = new ArrayList<>();
    upgradeTasks.add(upgradeTaskAny1);
    upgradeTasks.add(upgradeTaskAny2);
    upgradeTasks.add(upgradeTaskAny3);
    upgradeTasks.add(upgradeTaskAny4);
    upgradeTasks.add(upgradeTaskAny5);
    upgradeTasks.add(upgradeTaskAny6);
    upgradeTasks.add(upgradeTaskAny7);
    upgradeTasks.add(upgradeTaskAny8);

    // to make sure they are not in order
    Collections.shuffle(upgradeTasks);
    UpgradeTaskDependencyResolver upgradeTaskDependencyResolver = new UpgradeTaskDependencyResolver(upgradeTasks);

    List<UpgradeTask> sortedUpgradeTasks = upgradeTaskDependencyResolver.topologicalTasksSort();
    Map<String, Integer> nameToIndexMap = Maps.newHashMap();
    int i = 0;
    for (UpgradeTask upgradeTask : sortedUpgradeTasks) {
      nameToIndexMap.put(upgradeTask.getDescription(), i);
      i++;
    }
    // make sure dependencies are met
    assertThat(nameToIndexMap.get("upgradeTaskAny8")).isGreaterThan(nameToIndexMap.get("upgradeTaskAny4"));
    assertThat(nameToIndexMap.get("upgradeTaskAny8")).isGreaterThan(nameToIndexMap.get("upgradeTaskAny3"));
    assertThat(nameToIndexMap.get("upgradeTaskAny7")).isGreaterThan(nameToIndexMap.get("upgradeTaskAny4"));
    assertThat(nameToIndexMap.get("upgradeTaskAny7")).isGreaterThan(nameToIndexMap.get("upgradeTaskAny5"));
    assertThat(nameToIndexMap.get("upgradeTaskAny5")).isGreaterThan(nameToIndexMap.get("upgradeTaskAny2"));
    assertThat(nameToIndexMap.get("upgradeTaskAny5")).isGreaterThan(nameToIndexMap.get("upgradeTaskAny3"));
    assertThat(nameToIndexMap.get("upgradeTaskAny6")).isGreaterThan(nameToIndexMap.get("upgradeTaskAny4"));
    assertThat(nameToIndexMap.get("upgradeTaskAny4")).isGreaterThan(nameToIndexMap.get("upgradeTaskAny1"));
    assertThat(nameToIndexMap.get("upgradeTaskAny4")).isGreaterThan(nameToIndexMap.get("upgradeTaskAny2"));
  }

  @Test
  public void detectLoopTest() throws Exception {
    // no dependencies
    UpgradeTask1 upgradeTask1 = new UpgradeTask1(ImmutableList.of());
    // depends on task 4
    UpgradeTask2 upgradeTask2 = new UpgradeTask2(ImmutableList.of("UpgradeTask4"));
    // depends on 1 and 2
    UpgradeTask3 upgradeTask3 = new UpgradeTask3(ImmutableList.of(upgradeTask1.getTaskUUID(), upgradeTask2.getTaskUUID()));
    // depends on 3
    UpgradeTask4 upgradeTask4 = new UpgradeTask4(ImmutableList.of(upgradeTask3.getTaskUUID()));
    UpgradeTaskDependencyResolver upgradeTaskDependencyResolver = new UpgradeTaskDependencyResolver(
      ImmutableList.of(upgradeTask1, upgradeTask2, upgradeTask3, upgradeTask4)
    );

    assertThatThrownBy(upgradeTaskDependencyResolver::topologicalTasksSort)
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("Dependencies loop detected: UpgradeTask2");
  }

  private static class UpgradeTask1 extends UpgradeTask {

    protected UpgradeTask1(List<String> dependencies) {
      super("UpgradeTask1Desc", dependencies);
    }

    @Override
    public String getTaskUUID() {
      return "UpgradeTask1";
    }

    @Override
    public void upgrade(UpgradeContext context) throws Exception {

    }
  }

  private static class UpgradeTask2 extends UpgradeTask {

    protected UpgradeTask2(List<String> dependencies) {
      super("UpgradeTask2Desc", dependencies);
    }

    @Override
    public String getTaskUUID() {
      return "UpgradeTask2";
    }

    @Override
    public void upgrade(UpgradeContext context) throws Exception {

    }
  }

  private static class UpgradeTask3 extends UpgradeTask {

    protected UpgradeTask3(List<String> dependencies) {
      super("UpgradeTask3Desc", dependencies);
    }

    @Override
    public String getTaskUUID() {
      return "UpgradeTask3";
    }

    @Override
    public void upgrade(UpgradeContext context) throws Exception {

    }
  }

  private static class UpgradeTask4 extends UpgradeTask {

    protected UpgradeTask4(List<String> dependencies) {
      super("UpgradeTask4Desc", dependencies);
    }

    @Override
    public String getTaskUUID() {
      return "UpgradeTask4";
    }

    @Override
    public void upgrade(UpgradeContext context) throws Exception {

    }
  }

  private static class UpgradeTaskAny extends UpgradeTask {

    private String uuid;
    protected UpgradeTaskAny(String name, String uuid, List<String> dependencies) {
      super(name, dependencies);
      this.uuid = uuid;
    }

    @Override
    public String getTaskUUID() {
      return uuid;
    }

    @Override
    public void upgrade(UpgradeContext context) throws Exception {

    }
  }

}
