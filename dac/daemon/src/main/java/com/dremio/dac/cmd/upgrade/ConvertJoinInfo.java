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
package com.dremio.dac.cmd.upgrade;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import com.dremio.common.Version;
import com.dremio.datastore.IndexedStore;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.JoinAnalysis;
import com.dremio.service.job.proto.JoinCondition;
import com.dremio.service.job.proto.JoinConditionInfo;
import com.dremio.service.job.proto.JoinInfo;
import com.dremio.service.job.proto.JoinStats;
import com.dremio.service.job.proto.JoinTable;
import com.dremio.service.jobs.LocalJobsService.JobsStoreCreator;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Converts the deprecated JoinInfo to then new JoinAnalysis
 */
public class ConvertJoinInfo extends UpgradeTask implements LegacyUpgradeTask {

  //DO NOT MODIFY
  static final String taskUUID = "518bbc49-37be-474f-938d-c4960dfbdab8";

  public ConvertJoinInfo() {
    super("Convert Join Info", ImmutableList.of(MoveFromAccelerationSettingsToReflectionSettings.taskUUID));
  }

  @Override
  public Version getMaxVersion() {
    return VERSION_150;
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) {
    IndexedStore<JobId, JobResult> store = context.getKVStoreProvider().getStore(JobsStoreCreator.class);

    for (Entry<JobId, JobResult> entry : store.find()) {
      try {
        JobResult newJobResult = updateJobResult(entry.getValue());
        store.put(entry.getKey(), newJobResult);
      } catch (Exception e) {
        System.out.printf("Exception converting join info: %s. Skipping.\n", e.getMessage());
      }
    }
  }

  private JobResult updateJobResult(JobResult jobResult) {
    for (JobAttempt attempt : jobResult.getAttemptsList()) {
      List<JoinInfo> joinInfoList = attempt.getInfo().getJoinsList();

      if (joinInfoList == null) {
        continue;
      }

      List<JoinStats> joinStatsList = new ArrayList<>();

      Set<List<String>> tables = FluentIterable.from(joinInfoList)
        .transformAndConcat(new Function<JoinInfo, Iterable<List<String>>>() {
          @Override
          public Iterable<List<String>> apply(@Nullable JoinInfo joinInfo) {
            if (joinInfo != null) {
              return ImmutableList.of(joinInfo.getLeftTablePathList(), joinInfo.getRightTablePathList());
            }
            return ImmutableList.of();
          }
        })
      .toSet();

      Map<List<String>, Integer> tableMap = new HashMap<>();
      int cnt = 0;
      for (List<String> table : tables) {
        tableMap.put(table, cnt++);
      }

      for (final JoinInfo joinInfo : joinInfoList) {
        final Integer buildTable = tableMap.get(joinInfo.getLeftTablePathList());
        final Integer probeTable = tableMap.get(joinInfo.getRightTablePathList());

        List<JoinCondition> joinConditions = FluentIterable.from(joinInfo.getConditionsList())
          .transform(new Function<JoinConditionInfo, JoinCondition>() {
            @Nullable
            @Override
            public JoinCondition apply(@Nullable JoinConditionInfo joinConditionInfo) {
              if (joinConditionInfo != null) {
                JoinCondition joinCondition = new JoinCondition();
                List<String> tableA = joinConditionInfo.getTableAList();
                String colA = joinConditionInfo.getColumnA();
                String colB = joinConditionInfo.getColumnB();
                if (tableA.equals(joinInfo.getLeftTablePathList())) {
                  joinCondition.setBuildSideColumn(colA);
                  joinCondition.setBuildSideTableId(buildTable);
                  joinCondition.setProbeSideColumn(colB);
                  joinCondition.setProbeSideTableId(probeTable);
                } else {
                  joinCondition.setProbeSideColumn(colA);
                  joinCondition.setProbeSideTableId(probeTable);
                  joinCondition.setBuildSideColumn(colB);
                  joinCondition.setBuildSideTableId(buildTable);
                }
                return joinCondition;
              }
              return null;
            }
          })
          .filter(Predicates.<JoinCondition>notNull())
          .toList();
        JoinStats stats = new JoinStats()
          .setJoinType(joinInfo.getJoinType())
          .setJoinConditionsList(joinConditions);

        joinStatsList.add(stats);
      }
      List<JoinTable> joinTables = FluentIterable.from(tableMap.entrySet())
        .transform(new Function<Entry<List<String>,Integer>, JoinTable>() {
          @Override
          public JoinTable apply(Entry<List<String>, Integer> entry) {
            return new JoinTable().setTableSchemaPathList(entry.getKey()).setTableId(entry.getValue());
          }
        })
        .toList();

      attempt.getInfo().setJoinAnalysis(new JoinAnalysis().setJoinTablesList(joinTables).setJoinStatsList(joinStatsList));
    }
    return jobResult;
  }

  @Override
  public String toString() {
    return String.format("'%s' up to %s)", getDescription(), getMaxVersion());
  }
}
