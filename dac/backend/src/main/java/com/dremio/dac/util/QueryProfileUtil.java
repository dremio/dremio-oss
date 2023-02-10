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
import static com.dremio.dac.util.QueryProfileConstant.MINIMUM_THREADS_TO_CHECK_FOR_SKEW;
import static com.dremio.dac.util.QueryProfileConstant.ONE_SEC;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.dremio.dac.model.job.JobProfileOperatorHealth;
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
    long tempOutputRecords = 0;
    long tempOutputBytes = 0;
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
      tempOutputRecords += threadData.getOutputRecords();
      tempOutputBytes += threadData.getOutputBytes();
    }

    baseMetrics.setNumThreads(-1L);
    baseMetrics.setProcessingTime(tempProcessTime);
    baseMetrics.setRecordsProcessed(tempRecordsProcessed);
    baseMetrics.setPeakMemory(tempPeakMemory);
    baseMetrics.setSetupTime(tempSetupTime);
    baseMetrics.setIoWaitTime(tempWaitTime);
    baseMetrics.setRunTime(tempRunTime);
    baseMetrics.setTotalMemory(totalMemory);
    baseMetrics.setOutputRecords(tempOutputRecords);
    baseMetrics.setOutputBytes(tempOutputBytes);
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
              maxThreadLevelRecords,
              operatorProfile.hasOutputRecords()?operatorProfile.getOutputRecords():-1,
              operatorProfile.hasOutputBytes()?operatorProfile.getOutputBytes():-1
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

  private static  Function <ThreadData, Long> retrieveRecordsProcessed = ThreadData::getRecordsProcessed;
  private static Function <ThreadData, Long> retrieveProcessingTime = ThreadData::getProcessingTime;
  private static Function <ThreadData, Long> retrievePeakMemory = ThreadData::getPeakMemory;

  private static long getMaxAcrossThreads(List<ThreadData> threadLevelMetrics, Function <ThreadData, Long> retrieveStatFunction) {
    return threadLevelMetrics.stream().mapToLong(thread -> retrieveStatFunction.apply(thread)).max().orElse(-1);
  }

  private static long getMinAcrossThreads(List<ThreadData> threadLevelMetrics, Function <ThreadData, Long> retrieveStatFunction) {
    return threadLevelMetrics.stream().mapToLong(thread -> retrieveStatFunction.apply(thread)).min().orElse(-1);
  }

  private static long getAverageAcrossThreads(List<ThreadData> threadLevelMetrics, Function <ThreadData, Long> retrieveStatFunction) {
    return (threadLevelMetrics.stream().mapToLong(thread -> retrieveStatFunction.apply(thread)).sum()) / (threadLevelMetrics.size()>0?threadLevelMetrics.size():1);
  }

  //If the skewness logic changes, testcases in TestQueryProfileUtil needs to be reviewed.
  private static boolean checkOperatorSkewness(List<ThreadData> threadLevelMetrics, Function <ThreadData, Long> retrieveStatFunction) {
    long average = getAverageAcrossThreads(threadLevelMetrics, retrieveStatFunction);
    List<ThreadData> statsList = threadLevelMetrics.stream().filter(thread -> retrieveStatFunction.apply(thread) < average ).collect(Collectors.toList());
    return statsList.size() > (0.75 * threadLevelMetrics.size());
  }

  public static void isOperatorSkewedAcrossThreads(List<ThreadData> threadLevelMetrics, JobProfileOperatorHealth jpOperatorHealth) {
    jpOperatorHealth.setIsSkewedOnRecordsProcessed(checkOperatorSkewness(threadLevelMetrics, retrieveRecordsProcessed));
    jpOperatorHealth.setIsSkewedOnProcessingTime(checkOperatorSkewness(threadLevelMetrics, retrieveProcessingTime));
    jpOperatorHealth.setIsSkewedOnPeakMemory(checkOperatorSkewness(threadLevelMetrics, retrievePeakMemory));
  }

  public static void getJobProfileOperatorHealth(List<ThreadData> threadLevelMetrics, JobProfileOperatorHealth jpOperatorHealth, BaseMetrics baseMetrics) {
    if(threadLevelMetrics.size() > MINIMUM_THREADS_TO_CHECK_FOR_SKEW && baseMetrics.getProcessingTime() > ONE_SEC) {
      isOperatorSkewedAcrossThreads(threadLevelMetrics, jpOperatorHealth);
      if (jpOperatorHealth.getIsSkewedOnRecordsProcessed() || jpOperatorHealth.getIsSkewedOnProcessingTime() || jpOperatorHealth.getIsSkewedOnPeakMemory()) {
        jpOperatorHealth.setOperatorInformationPerThreadList(threadLevelMetrics);
      }
    }
  }

  private static String getRelNodeInfoMapKey(int phaseId, int operatorId) {
    return phaseId + ":*:" + operatorId;
  }
  public static UserBitShared.RelNodeInfo getOperatorAttributes(UserBitShared.QueryProfile profile, int phaseId, int operatorId) {
    UserBitShared.RelNodeInfo relNodeInfo = null;
    Map<String, UserBitShared.RelNodeInfo> relNodeInfoMap = profile.getRelInfoMapMap();
    if(relNodeInfoMap !=null && !relNodeInfoMap.isEmpty()){
      relNodeInfo = relNodeInfoMap.get(getRelNodeInfoMapKey(phaseId, operatorId));
    }
    return relNodeInfo;
  }
}
