/*
 * Copyright (C) 2017 Dremio Corporation
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

public class TestCastFunctions extends BaseTestFunction {

  @Test
  public void bigInt(){
    testFunctions(new Object[][]{
      {"cast(c0 as bigint)", 14.0f, 14l},
      {"cast(c0 as bigint)", 14.0d, 14l},
      {"cast(c0 as bigint)", 14, 14l},
      {"cast(c0 as bigint)", "14", 14l},
    });
  }

  @Test
  public void integer(){
    testFunctions(new Object[][]{
      {"cast(c0 as int)", 14.0f, 14},
      {"cast(c0 as int)", 14.0d, 14},
      {"cast(c0 as int)", 14l, 14},
      {"cast(c0 as int)", "14", 14},
    });
  }

  @Test
  public void floating(){
    testFunctions(new Object[][]{
      {"cast(c0 as float4)", 14l, 14f},
      {"cast(c0 as float4)", 14.3d, 14.3f},
      {"cast(c0 as float4)", 14, 14f},
      {"cast(c0 as float4)", "14.3", 14.3f},
    });
  }

  @Test
  public void doublePrecision(){
    testFunctions(new Object[][]{
      {"cast(c0 as float8)", 14l, 14d},
      {"cast(c0 as float8)", 14f, 14d},
      {"cast(c0 as float8)", 14, 14d},
      {"cast(c0 as float8)", "14.3", 14.3d},
    });
  }


  @Test
  public void varchar(){
    testFunctions(new Object[][]{
      {"cast(c0 as varchar(30))", 14l, "14"},
      {"cast(c0 as varchar(30))", 14.3f, "14.3"},
      {"cast(c0 as varchar(30))", 14, "14"},
      {"cast(c0 as varchar(30))", 14.3d, "14.3"},
    });
  }
}
