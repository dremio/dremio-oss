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

package com.dremio.dac.server.admin.profile;

import java.math.BigDecimal;
import java.math.BigInteger;

public class HostProcessingRate implements Comparable<HostProcessingRate> {
  private Integer major;
  private String hostname;
  private BigInteger numRecords, processNanos, numThreads;

  public HostProcessingRate(
      Integer major,
      String hostname,
      BigInteger numRecords,
      BigInteger processNanos,
      BigInteger numThreads) {
    this.major = major;
    this.hostname = hostname;
    this.numRecords = numRecords;
    this.processNanos = processNanos;
    this.numThreads = numThreads;
  }

  // sorting should be based on processingRate in ascending order.
  @Override
  public int compareTo(HostProcessingRate o) {
    return computeProcessingRate().compareTo(o.computeProcessingRate());
  }

  public Integer getMajor() {
    return major;
  }

  public String getHostname() {
    return hostname;
  }

  public BigInteger getNumRecords() {
    return numRecords;
  }

  public BigInteger getProcessNanos() {
    return processNanos;
  }

  public BigInteger getNumThreads() {
    return numThreads;
  }

  public BigDecimal computeProcessingRate() {
    return HostProcessingRateUtil.computeRecordProcessingRate(numRecords, processNanos);
  }
}
