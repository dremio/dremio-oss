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
package com.dremio.dac.model.job;

import static com.dremio.dac.util.QueryProfileConstant.DEFAULT_LONG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.stream.Collectors;

import com.dremio.dac.util.QueryProfileUtil;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobAnalysis.proto.Node;
import com.dremio.service.jobAnalysis.proto.OperatorData;
import com.dremio.service.jobAnalysis.proto.OperatorDataList;
import com.dremio.service.jobAnalysis.proto.PhaseData;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * class for building profile Phase data
 */
public class JobProfileVisualizerUI {

  /**
   * Method to extract all the information about Phase Summary and Graph information for Query Profile in new UI.
   */
  public List<PhaseData> getPhaseDetail(UserBitShared.QueryProfile profile) throws JsonProcessingException, ClassNotFoundException {
    List<PhaseData> phaseData = new ArrayList<>();
    if (!(profile.getJsonPlan().isEmpty() && profile.getFragmentProfileCount() == 0)) {
      QueryProfileUtil details = new QueryProfileUtil(profile);
      List<Node> nodeList = details.getNodeDetails();
      Map<String, List<Node>> mapData = nodeList.stream().collect(Collectors.groupingBy(phase -> phase.getPhaseId(), Collectors.toList()));
      Map<String, LongSummaryStatistics> phaseMaxSetupTimeMetrics = bytesProcessedSummary(nodeList);

      for (Map.Entry<String, List<Node>> keySet : mapData.entrySet()) {
        OperatorDataList operatorDataList = new OperatorDataList();
        List<OperatorData> operatorData = new ArrayList<>();
        List<Node> dataset = mapData.get(keySet.getKey());
        long totalMaxRecords = DEFAULT_LONG;
        long PeakMemory = DEFAULT_LONG;
        long phaseProcessingTime = DEFAULT_LONG;
        long bytesProcessed = DEFAULT_LONG;
        long numThreads = DEFAULT_LONG;

        if (!profile.getFragmentProfileList().isEmpty()) {
          totalMaxRecords = nodeList.stream().filter(node -> node.getPhaseId().equals(keySet.getKey()))
            .collect(Collectors.groupingBy(phase -> phase.getPhaseId(),
              Collectors.summarizingLong(row -> row.getBaseMetrics().getRecordsProcessed()))).get(keySet.getKey()).getSum();
          PeakMemory = profile.getFragmentProfile(Integer.parseInt(keySet.getKey())).getNodePhaseProfileList().stream()
            .collect(Collectors.summarizingLong(memory -> memory.getMaxMemoryUsed())).getSum();
          phaseProcessingTime = profile.getFragmentProfile(Integer.parseInt(keySet.getKey())).getMinorFragmentProfileList().stream()
            .flatMap(b -> b.getOperatorProfileList().stream()).collect(Collectors.summarizingLong(processingTime -> processingTime.getProcessNanos())).getSum();
          bytesProcessed = phaseMaxSetupTimeMetrics.get(keySet.getKey()).getSum();
        }
        getOperatorDetails(operatorData, dataset);
        operatorDataList.setOperatorDataList(operatorData);

        phaseData.add(new PhaseData(
          keySet.getKey(),
          phaseProcessingTime,
          PeakMemory,
          totalMaxRecords,
          bytesProcessed,
          numThreads,
          operatorDataList));
      }
    }
    return phaseData;
  }

  /**
   * This Method Will return Summary of Bytes Processed in Phase
   */
  public Map<String, LongSummaryStatistics> bytesProcessedSummary(List<Node> nodeList) {
    Map<String, LongSummaryStatistics> phaseByteData = new HashMap<>();
    nodeList.stream()
      .collect(Collectors.groupingBy(phase -> phase.getPhaseId(), Collectors.summarizingLong(bytesProcessed -> bytesProcessed.getBaseMetrics().getBytesProcessed())))
      .forEach((phaseId, bytesData) ->
        phaseByteData.put(phaseId, bytesData));
    return phaseByteData;
  }

  /**
   * This method builds the OperatorIdList
   */
  private void getOperatorDetails(List<OperatorData> operatorData, List<Node> dataset) {
    for (int i = 0; i < dataset.size(); i++) {
      operatorData.add(new OperatorData(
        dataset.get(i).getNodeId(),
        dataset.get(i).getOperatorName(),
        dataset.get(i).getOperatorType(),
        dataset.get(i).getBaseMetrics(),
        dataset.get(i).getMergeNodeName(),
        dataset.get(i).getSuccessorId()
      ));
    }
  }
}
