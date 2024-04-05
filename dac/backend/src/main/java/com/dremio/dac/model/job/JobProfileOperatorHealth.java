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

import com.dremio.service.jobAnalysis.proto.ThreadData;
import java.util.List;

/** class for Operator Health information */
public class JobProfileOperatorHealth {
  private Boolean isSkewedOnRecordsProcessed;
  private Boolean isSkewedOnProcessingTime;
  private Boolean isSkewedOnPeakMemory;
  private Boolean isThreadsParallelizationProper;
  private List<ThreadData> operatorInformationPerThread;

  public JobProfileOperatorHealth() {}

  public Boolean getIsSkewedOnRecordsProcessed() {
    return isSkewedOnRecordsProcessed;
  }

  public JobProfileOperatorHealth setIsSkewedOnRecordsProcessed(
      Boolean isSkewedOnRecordsProcessed) {
    this.isSkewedOnRecordsProcessed = isSkewedOnRecordsProcessed;
    return this;
  }

  public Boolean getIsSkewedOnProcessingTime() {
    return isSkewedOnProcessingTime;
  }

  public JobProfileOperatorHealth setIsSkewedOnProcessingTime(Boolean isSkewedOnProcessingTime) {
    this.isSkewedOnProcessingTime = isSkewedOnProcessingTime;
    return this;
  }

  public Boolean getIsSkewedOnPeakMemory() {
    return isSkewedOnPeakMemory;
  }

  public JobProfileOperatorHealth setIsSkewedOnPeakMemory(Boolean isSkewedOnPeakMemory) {
    this.isSkewedOnPeakMemory = isSkewedOnPeakMemory;
    return this;
  }

  public Boolean getIsThreadsParallelizationProper() {
    return isThreadsParallelizationProper;
  }

  public JobProfileOperatorHealth setIsThreadsParallelizationProper(
      Boolean isThreadsParallelizationProper) {
    this.isThreadsParallelizationProper = isThreadsParallelizationProper;
    return this;
  }

  public List<ThreadData> getOperatorInformationPerThreadList() {
    return operatorInformationPerThread;
  }

  public JobProfileOperatorHealth setOperatorInformationPerThreadList(
      List<ThreadData> operatorInformationPerThread) {
    this.operatorInformationPerThread = operatorInformationPerThread;
    return this;
  }
}
