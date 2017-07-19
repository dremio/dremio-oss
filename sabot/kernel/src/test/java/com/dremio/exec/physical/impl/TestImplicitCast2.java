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
import com.dremio.sabot.Fixtures;

public class TestImplicitCast2 extends BaseTestFunction {

  @Test
  public void go(){
    testFunctions(tests);
  }


  Object[][] tests = {
      { "10+20.1", 30.1f },
      { "20.1+10", 30.1f },
      { "20.1 + '10'", 30.1f },
      { "cast('10' as int) + 20.1", 30.1f },
      { "cast('10' as int) + cast('20' as bigint)", 30l },
      { "cast('10' as bigint) + cast('20' as int)", 30l },
      { "cast('10' as int) + cast('20.1' as float8)", 30.1d },
      { "cast('20.1' as float8) + cast('10' as int) ", 30.1d },
      { "cast('10' as int) + cast('20.1' as float4)", 30.1f },
      { "cast('10' as bigint) + cast('20.1' as float4)", 30.1f },
      { "cast('10' as bigint) + cast('20.1' as float8)", 30.1d },
      { "cast('10' as float4) + cast('20.1' as float8)", 30.1d },
      { "'10' + cast('20.1' as float4)", 30.1f },
      { "'10' + cast('20.1' as float8)", 30.1d },
      { "cast('10' as float4) + '20.1' ", 30.1f },
      { "cast('10' as float8) + '20.1' ", 30.1d },
      { "10 < 20.1", true },
      { "'10' < 20.1", true },
      { "20.1 > '10' ", true },
      { " 20.1  + 10 > '10' ", true },
      { " 20.1  + 10 > '10'  + 15.1", true },

      { "c0 + c1", 0, 1, 1},
      { "c0 + c1", 0, 1, 1},
      { "c0 + c1", 0, 1f, 1f},
      { "c0 + c1", 0f, 1, 1f},
      { "c0 + c1", 0d, 1l, 1d},

      { "isnull(c0)", Fixtures.NULL_BIGINT, true},
      { "isnull(c0)", 0d, false},
      { "isnotnull(c0)", 0d, true},
      { "isnotnull(c0)", Fixtures.NULL_BIGINT, false}


   };
}