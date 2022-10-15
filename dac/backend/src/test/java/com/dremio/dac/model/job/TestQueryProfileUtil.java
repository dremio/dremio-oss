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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.util.QueryProfileUtil;
import com.dremio.service.jobAnalysis.proto.ThreadData;

/*
  * All the test case are based on current logic of skewness in function "checkOperatorSkewness".
  * Whenever above function changes these testcase need to be reviewed.
 */
public class TestQueryProfileUtil {
  private List<ThreadData> threadDataList;

  @Before
  public void setUp() throws Exception {
    threadDataList= getThreadDataList();
  }

  private List<ThreadData> getThreadDataList(){
    List<ThreadData> localThreadDataList = new ArrayList<>();
    for(int i=0;i<10;i++){
      localThreadDataList.add(new ThreadData());
      localThreadDataList.get(i).setRecordsProcessed(1L);
      localThreadDataList.get(i).setProcessingTime(1L);
      localThreadDataList.get(i).setPeakMemory(1L);
    }
    return localThreadDataList;
  }

  @Test
  public void testOneThreadSkewed(){
    threadDataList.get(0).setRecordsProcessed(100L);
    JobProfileOperatorHealth jpOperatorHealth = new JobProfileOperatorHealth();
    QueryProfileUtil.isOperatorSkewedAcrossThreads(threadDataList, jpOperatorHealth);
    assertEquals(true, jpOperatorHealth.getIsSkewedOnRecordsProcessed());
  }

  @Test
  public void testTwoThreadsSkewed(){
    threadDataList.get(0).setRecordsProcessed(100L);
    threadDataList.get(1).setRecordsProcessed(100L);
    JobProfileOperatorHealth jpOperatorHealth = new JobProfileOperatorHealth();
    QueryProfileUtil.isOperatorSkewedAcrossThreads(threadDataList, jpOperatorHealth);
    assertEquals(true, jpOperatorHealth.getIsSkewedOnRecordsProcessed());
  }

  @Test
  public void testThreeThreadsSkewed(){
    threadDataList.get(0).setRecordsProcessed(100L);
    threadDataList.get(1).setRecordsProcessed(100L);
    threadDataList.get(2).setRecordsProcessed(100L);
    JobProfileOperatorHealth jpOperatorHealth = new JobProfileOperatorHealth();
    QueryProfileUtil.isOperatorSkewedAcrossThreads(threadDataList, jpOperatorHealth);
    assertEquals(false, jpOperatorHealth.getIsSkewedOnRecordsProcessed());
  }

  private void setLinearData(){
    for(int i=0;i<threadDataList.size();i++){
      threadDataList.get(i).setRecordsProcessed((i+1)*100L);
    }
  }

  @Test
  public void testThreadsWithLinearData(){
    setLinearData();
    JobProfileOperatorHealth jpOperatorHealth = new JobProfileOperatorHealth();
    QueryProfileUtil.isOperatorSkewedAcrossThreads(threadDataList, jpOperatorHealth);
    assertEquals(false, jpOperatorHealth.getIsSkewedOnRecordsProcessed());
  }

  @Test
  public void testThreadsWithUniformData(){
    //the default threadDataList is created with uniform data
    JobProfileOperatorHealth jpOperatorHealth = new JobProfileOperatorHealth();
    QueryProfileUtil.isOperatorSkewedAcrossThreads(threadDataList, jpOperatorHealth);
    assertEquals(false, jpOperatorHealth.getIsSkewedOnRecordsProcessed());
  }

  private void setHalfThreadsWithHighData(){
    for(int i=0;i<threadDataList.size();i+=2){
      threadDataList.get(i).setRecordsProcessed((i+1)*100L);
    }
  }

  @Test
  public void testHalfThreadsWithHighData() {
    setHalfThreadsWithHighData();
    JobProfileOperatorHealth jpOperatorHealth = new JobProfileOperatorHealth();
    QueryProfileUtil.isOperatorSkewedAcrossThreads(threadDataList, jpOperatorHealth);
    assertEquals(false, jpOperatorHealth.getIsSkewedOnRecordsProcessed());
  }

  private void setOneThreadsWithLowData(){
    for(int i=1;i<threadDataList.size();i++){
      threadDataList.get(i).setRecordsProcessed((i+1)*100L);
    }
  }

  @Test
  public void testOneThreadWithLowData() {
    setOneThreadsWithLowData();
    JobProfileOperatorHealth jpOperatorHealth = new JobProfileOperatorHealth();
    QueryProfileUtil.isOperatorSkewedAcrossThreads(threadDataList, jpOperatorHealth);
    assertEquals(false, jpOperatorHealth.getIsSkewedOnRecordsProcessed());
  }

}
