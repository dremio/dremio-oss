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


/*
 * This class is automatically generated from AggrTypeFunctions2.tdd using FreeMarker.
 */

package com.dremio.exec.expr.fn.impl;

import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;

@SuppressWarnings("unused")
public class BooleanAggrFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BooleanAggrFunctions.class);


@FunctionTemplate(name = "bool_or", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class NullableBitBooleanOr implements AggrFunction{

  @Param NullableBitHolder in;
  @Workspace BitHolder inter;
  @Output NullableBitHolder out;

  public void setup() {
  inter = new BitHolder();

    // Initialize the workspace variables
    inter.value = 0;
  }

  @Override
  public void add() {
    sout: {
    if (in.isSet == 0) {
     // processing nullable input and the value is null, so don't do anything...
     break sout;
    }

    inter.value = inter.value | in.value;
    } // end of sout block
  }


  @Override
  public void output() {
    out.isSet = 1;
    out.value = inter.value;
  }

  @Override
  public void reset() {
    inter.value = 0;
  }
}

@FunctionTemplate(name = "bool_and", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class NullableBitBooleanAnd implements AggrFunction{

  @Param NullableBitHolder in;
  @Workspace BitHolder inter;
  @Output NullableBitHolder out;

  public void setup() {
  inter = new BitHolder();

    // Initialize the workspace variables
    inter.value = Integer.MAX_VALUE;
  }

  @Override
  public void add() {
    sout: {
    if (in.isSet == 0) {
     // processing nullable input and the value is null, so don't do anything...
     break sout;
    }

    inter.value = inter.value & in.value;

    } // end of sout block
  }


  @Override
  public void output() {
    out.isSet = 1;
    out.value = inter.value;
  }

  @Override
  public void reset() {
    inter.value = Integer.MAX_VALUE;
  }
}

}
