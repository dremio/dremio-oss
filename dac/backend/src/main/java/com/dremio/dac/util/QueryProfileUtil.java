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

import static com.dremio.dac.util.QueryProfileConstant.DEFAULT_LONG;

import java.util.List;
import java.util.stream.Collectors;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobAnalysis.proto.BaseMetrics;
import com.dremio.service.jobAnalysis.proto.ThreadData;

/**
 * class for building Node data from Profile Object
 */
public class QueryProfileUtil {
  /**
   * This Method is Used to get the String value of Node Id or Phase Id with Left padding of Zero if required
   */
  public static String getStringIds(int majorId) {
    String id = String.valueOf(majorId);
    if(String.valueOf(majorId).length() == 1) {
      id = "0" + id;
    }
    return id;
  }

  /**
   * This Method will build BaseMetrics on for an operator
   */
  public static void buildBaseMetrics(List<ThreadData> threadLevelMetrics, BaseMetrics baseMetrics) {

    long tempProcessTime = DEFAULT_LONG;
    long tempPeakMemory = DEFAULT_LONG;
    long tempWaitTime = DEFAULT_LONG;
    long tempSetupTime = DEFAULT_LONG;
    long tempRunTime = DEFAULT_LONG;
    long totalMemory = 0;
    long tempRecordsProcessed = 0;
    for (int threadIndex = 0; threadIndex<threadLevelMetrics.size(); threadIndex++) {
      ThreadData threadData = threadLevelMetrics.get(threadIndex);
      tempProcessTime = threadData.getProcessingTime() > tempProcessTime ? threadData.getProcessingTime() : tempProcessTime;
      tempPeakMemory = threadData.getPeakMemory() > tempPeakMemory ? threadData.getPeakMemory() : tempPeakMemory;
      tempWaitTime = threadData.getIoWaitTime() > tempWaitTime ? threadData.getIoWaitTime() : tempWaitTime;
      tempSetupTime = threadData.getSetupTime() > tempSetupTime ? threadData.getSetupTime() : tempSetupTime;
      long threadRunTime = threadData.getProcessingTime() + threadData.getIoWaitTime() + threadData.getSetupTime();
      tempRunTime = threadRunTime > tempRunTime ? threadRunTime : tempRunTime;
      totalMemory += threadData.getPeakMemory();
      if(threadData.getRecordsProcessed() != null) {
        tempRecordsProcessed = tempRecordsProcessed + threadData.getRecordsProcessed();
      }
    }

    baseMetrics.setNumThreads(-1L);
    baseMetrics.setProcessingTime(tempProcessTime);
    baseMetrics.setRecordsProcessed(tempRecordsProcessed);
    baseMetrics.setPeakMemory(tempPeakMemory);
    baseMetrics.setSetupTime(tempSetupTime);
    baseMetrics.setIoWaitTime(tempWaitTime);
    baseMetrics.setRunTime(tempRunTime);
    baseMetrics.setTotalMemory(totalMemory);
  }

  /**
   *This Method will build a list of TheadLevel Operator Metrics
   */
  public static void buildTheadLevelMetrics(UserBitShared.MajorFragmentProfile major, List<ThreadData> threadLevelMetricsList) {
    major.getMinorFragmentProfileList().stream().forEach(
      minor -> {
        minor.getOperatorProfileList().stream().forEach(
          operatorProfile -> {
            long maxThreadLevelRecords = operatorProfile.getInputProfileList().stream().collect(Collectors.summarizingLong(row -> row.getRecords())).getMax();
            threadLevelMetricsList.add(new ThreadData(
              QueryProfileUtil.getStringIds(operatorProfile.getOperatorId()),
              getOperatorName(operatorProfile.getOperatorType()),
              operatorProfile.getOperatorType(),
              operatorProfile.getWaitNanos(),
              operatorProfile.getPeakLocalMemoryAllocated(),
              operatorProfile.getProcessNanos(),
              operatorProfile.getSetupNanos(),
              maxThreadLevelRecords
            ));
          }
        );
      }
    );
  }

  /**
   * This Method is Used to get the Actual Operator Name
   */
  private static String getOperatorName(int operatorType) {
    String OperatorName = "";
    if (!(operatorType == -1)) {
      OperatorName = String.valueOf(UserBitShared.CoreOperatorType.forNumber(operatorType));
    }
    return OperatorName;
  }

}
