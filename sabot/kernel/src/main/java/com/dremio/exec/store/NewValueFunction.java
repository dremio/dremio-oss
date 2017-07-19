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
package com.dremio.exec.store;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;

import javax.inject.Inject;

/**
 *  The functions are similar to those created through FreeMarker template for fixed types. There is not much benefit to
 *  using code generation for generating the functions for variable length types, so simply doing them by hand.
 */
public class NewValueFunction {

  @FunctionTemplate(
      name = "newPartitionValue",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL)
  public static class NewValueVarCharNullable implements SimpleFunction {

    @Param NullableVarCharHolder in;
    @Workspace NullableVarCharHolder previous;
    @Workspace Boolean initialized;
    @Output NullableBitHolder out;
    @Inject ArrowBuf buf;

    public void setup() {
      initialized = false;
      previous.buffer = buf;
      previous.start = 0;
    }

    public void eval() {
      out.isSet = 1;

      if (initialized && // it's not the first value
        previous.isSet == in.isSet && // nullability didn't change
        (previous.isSet == 0 || // it's either a null partition or the partition value is the same
          org.apache.arrow.vector.util.ByteFunctionHelpers.compare(previous.buffer, 0, previous.end, in.buffer, in.start, in.end) == 0)
        ) {
        out.value = 0; // it's the same partition
      } else {
        out.value = 1; // it's a new partition
        if (in.isSet == 1) { // copy the partition's value in previous holder
          previous.buffer = buf.reallocIfNeeded(in.end - in.start);
          previous.buffer.setBytes(0, in.buffer, in.start, in.end - in.start);
          previous.end = in.end - in.start;
        }
        previous.isSet = in.isSet;
        initialized = true;
      }
    }
  }

  @FunctionTemplate(
      name = "newPartitionValue",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL)
  public static class NewValueVarBinaryNullable implements SimpleFunction {

    @Param NullableVarBinaryHolder in;
    @Workspace NullableVarBinaryHolder previous;
    @Workspace Boolean initialized;
    @Output NullableBitHolder out;
    @Inject ArrowBuf buf;

    public void setup() {
      initialized = false;
      previous.buffer = buf;
      previous.start = 0;
    }

    public void eval() {
      out.isSet = 1;

      if (initialized && // it's not the first value
        previous.isSet == in.isSet && // nullability didn't change
        (previous.isSet == 0 || // it's either a null partition or the partition value is the same
          org.apache.arrow.vector.util.ByteFunctionHelpers.compare(previous.buffer, 0, previous.end, in.buffer, in.start, in.end) == 0)
        ) {
        out.value = 0; // it's the same partition
      } else {
        out.value = 1; // it's a new partition
        if (in.isSet == 1) { // copy the partition's value in previous holder
          previous.buffer = buf.reallocIfNeeded(in.end - in.start);
          previous.buffer.setBytes(0, in.buffer, in.start, in.end - in.start);
          previous.end = in.end - in.start;
        }
        previous.isSet = in.isSet;
        initialized = true;
      }
    }
  }
}
