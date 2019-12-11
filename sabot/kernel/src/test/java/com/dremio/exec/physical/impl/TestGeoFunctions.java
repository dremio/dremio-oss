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
package com.dremio.exec.physical.impl;

import org.junit.Test;

import com.dremio.sabot.BaseTestFunction;

/**
 * Test for Geo Functions
 */
public class TestGeoFunctions extends BaseTestFunction {

  @Test
  public void distance() {
    testFunctions(new Object[][]{
      {"geo_distance(c0,c1,c2,c3)", 0f,0f,0f,1f, 111194.9266445d},
    });
  }

  @Test
  public void nearby(){
    testFunctions(new Object[][]{
      {"geo_nearby(c0,c1,c2,c3,112000d)", 0f,0f,0f,1f, true},
      {"geo_nearby(c0,c1,c2,c3,110000d)", 0f,0f,0f,1f, false},
      {"geo_nearby(c0,c1,c2,c3,10000d)", 90f,0f,90f,1f, true},
    });
  }

  @Test
  public void beyond(){
    testFunctions(new Object[][]{
      {"geo_beyond(c0,c1,c2,c3,110000d)", 0f,0f,0f,1f, true},
      {"geo_beyond(c0,c1,c2,c3,112000d)", 0f,0f,0f,1f, false},
      {"geo_beyond(c0,c1,c2,c3,10000d)", 90f,0f,90f,1f, false},
    });
  }
}
