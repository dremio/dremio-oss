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
package com.dremio.exec.expr.fn.impl.array;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import javax.inject.Inject;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.UnionHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

public class ArrayToStringFunction {

  /** turns a list into a delimited string */
  @FunctionTemplate(
      names = {"list_to_delimited_string", "array_to_string"},
      scope = FunctionScope.SIMPLE,
      nulls = NullHandling.NULL_IF_NULL)
  public static class ListToStringWithDelimiter implements SimpleFunction {

    @Param private FieldReader input;
    @Param private VarCharHolder delimiter;
    @Output private VarCharHolder out;
    @Inject private ArrowBuf buffer;
    @Inject private FunctionErrorContext errCtx;
    @Workspace private byte[] delimiterArray;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      int length = delimiter.end - delimiter.start;
      delimiterArray = new byte[length];
      delimiter.buffer.getBytes(delimiter.start, delimiterArray, 0, length);
      byte[] bytes =
          com.dremio.exec.expr.fn.impl.array.ArrayToStringHelper.arrayToDelimitedStringByteArray(
              input, delimiterArray, errCtx);
      out.start = 0;
      buffer = buffer.reallocIfNeeded(bytes.length);
      out.buffer = buffer;
      out.buffer.setBytes(0, bytes);
      out.end = bytes.length;
    }
  }

  @FunctionTemplate(
      names = {"list_to_delimited_string", "array_to_string"},
      scope = FunctionScope.SIMPLE,
      nulls = NullHandling.NULL_IF_NULL)
  public static class UnionListToStringWithDelimiter implements SimpleFunction {

    @Param private UnionHolder input;
    @Param private VarCharHolder delimiter;
    @Output private VarCharHolder out;
    @Inject private ArrowBuf buffer;
    @Inject private FunctionErrorContext errCtx;
    @Workspace private byte[] delimiterArray;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      int length = delimiter.end - delimiter.start;
      delimiterArray = new byte[length];
      delimiter.buffer.getBytes(delimiter.start, delimiterArray, 0, length);
      org.apache.arrow.vector.complex.impl.UnionReader unionReader =
          (org.apache.arrow.vector.complex.impl.UnionReader) input.reader;
      byte[] bytes =
          com.dremio.exec.expr.fn.impl.array.ArrayToStringHelper.arrayToDelimitedStringByteArray(
              unionReader, delimiterArray, errCtx);
      out.start = 0;
      buffer = buffer.reallocIfNeeded(bytes.length);
      out.buffer = buffer;
      out.buffer.setBytes(0, bytes);
      out.end = bytes.length;
    }
  }
}
