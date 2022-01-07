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
package com.dremio.exec.expr.fn.impl;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.DateMilliHolder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;

public class MaskingFunctions {
  @FunctionTemplate(name = "mask_internal", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class MaskInternalInt implements SimpleFunction {
    @Param
    IntHolder value;
    @Param(constant = true)
    VarCharHolder mode;
    @Param(constant = true)
    IntHolder charCount;
    @Param(constant = true)
    VarCharHolder upperChar;
    @Param(constant = true)
    VarCharHolder lowerChar;
    @Param(constant = true)
    VarCharHolder digitChar;
    @Param(constant = true)
    VarCharHolder otherChar;
    @Param(constant = true)
    IntHolder numberValue;

    @Workspace
    com.dremio.exec.expr.fn.impl.MaskTransformer transformer;

    @Output
    IntHolder out;

    @Override
    public void setup() {
      String m = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(mode.start, mode.end, mode.buffer);
      String upper = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(upperChar.start, upperChar.end, upperChar.buffer);
      String lower = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(lowerChar.start, lowerChar.end, lowerChar.buffer);
      String digit = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(digitChar.start, digitChar.end, digitChar.buffer);
      String other = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(otherChar.start, otherChar.end, otherChar.buffer);
      transformer = com.dremio.exec.expr.fn.impl.MaskTransformers.newTransformer(m, charCount.value, upper, lower, digit, other, numberValue.value, -1, -1, -1);
    }

    @Override
    public void eval() {
      int newVal = transformer.transform(value.value);
      out.value = newVal;
    } // end of eval
  }
  @FunctionTemplate(name = "mask_internal", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class MaskInternalBigInt implements SimpleFunction {
    @Param
    BigIntHolder value;
    @Param(constant = true)
    VarCharHolder mode;
    @Param(constant = true)
    IntHolder charCount;
    @Param(constant = true)
    VarCharHolder upperChar;
    @Param(constant = true)
    VarCharHolder lowerChar;
    @Param(constant = true)
    VarCharHolder digitChar;
    @Param(constant = true)
    VarCharHolder otherChar;
    @Param(constant = true)
    IntHolder numberValue;

    @Workspace
    com.dremio.exec.expr.fn.impl.MaskTransformer transformer;

    @Output
    BigIntHolder out;

    @Override
    public void setup() {
      String m = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(mode.start, mode.end, mode.buffer);
      String upper = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(upperChar.start, upperChar.end, upperChar.buffer);
      String lower = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(lowerChar.start, lowerChar.end, lowerChar.buffer);
      String digit = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(digitChar.start, digitChar.end, digitChar.buffer);
      String other = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(otherChar.start, otherChar.end, otherChar.buffer);
      transformer = com.dremio.exec.expr.fn.impl.MaskTransformers.newTransformer(m, charCount.value, upper, lower, digit, other, numberValue.value, -1, -1, -1);
    }

    @Override
    public void eval() {
      long newVal = transformer.transform(value.value);
      out.value = newVal;
    } // end of eval
  }
  @FunctionTemplate(name = "mask_internal", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class MaskInternalDate implements SimpleFunction {
    @Param
    DateMilliHolder value;
    @Param(constant = true)
    VarCharHolder mode;
    @Param(constant = true)
    IntHolder charCount;
    @Param(constant = true)
    VarCharHolder upperChar;
    @Param(constant = true)
    VarCharHolder lowerChar;
    @Param(constant = true)
    VarCharHolder digitChar;
    @Param(constant = true)
    VarCharHolder otherChar;
    @Param(constant = true)
    IntHolder numberValue;
    @Param(constant = true)
    IntHolder dayValue;
    @Param(constant = true)
    IntHolder monthValue;
    @Param(constant = true)
    IntHolder yearValue;

    @Workspace
    com.dremio.exec.expr.fn.impl.MaskTransformer transformer;
    @Workspace
    org.joda.time.MutableDateTime temp;

    @Output
    DateMilliHolder out;

    @Override
    public void setup() {
      temp = new org.joda.time.MutableDateTime().toMutableDateTime(org.joda.time.DateTimeZone.UTC);
      String m = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(mode.start, mode.end, mode.buffer);
      String upper = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(upperChar.start, upperChar.end, upperChar.buffer);
      String lower = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(lowerChar.start, lowerChar.end, lowerChar.buffer);
      String digit = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(digitChar.start, digitChar.end, digitChar.buffer);
      String other = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(otherChar.start, otherChar.end, otherChar.buffer);
      transformer = com.dremio.exec.expr.fn.impl.MaskTransformers.newTransformer(m, charCount.value, upper, lower, digit, other, numberValue.value, dayValue.value, monthValue.value, yearValue.value);
      transformer.init(upper, lower, digit, other, numberValue.value, dayValue.value, monthValue.value, yearValue.value);
    }

    @Override
    public void eval() {
      temp.setMillis(value.value);
      transformer.transform(temp);
      out.value = temp.getMillis();
    } // end of eval
  }

  @FunctionTemplate(name = "mask_internal", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class MaskInternal implements SimpleFunction {
    @Param
    VarCharHolder text;
    @Param(constant = true)
    VarCharHolder mode;
    @Param(constant = true)
    IntHolder charCount;
    @Param(constant = true)
    VarCharHolder upperChar;
    @Param(constant = true)
    VarCharHolder lowerChar;
    @Param(constant = true)
    VarCharHolder digitChar;
    @Param(constant = true)
    VarCharHolder otherChar;

    @Inject
    ArrowBuf buffer;

    @Workspace
    com.dremio.exec.expr.fn.impl.MaskTransformer transformer;

    @Workspace
    com.dremio.exec.expr.fn.impl.CharSequenceWrapper charSequenceWrapper;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      charSequenceWrapper = new com.dremio.exec.expr.fn.impl.CharSequenceWrapper();
      String m = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(mode.start, mode.end, mode.buffer);
      String upper = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(upperChar.start, upperChar.end, upperChar.buffer);
      String lower = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(lowerChar.start, lowerChar.end, lowerChar.buffer);
      String digit = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(digitChar.start, digitChar.end, digitChar.buffer);
      String other = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(otherChar.start, otherChar.end, otherChar.buffer);
      transformer = com.dremio.exec.expr.fn.impl.MaskTransformers.newTransformer(m, charCount.value, upper, lower, digit, other, -1, -1, -1, -1);
    }

    @Override
    public void eval() {
      charSequenceWrapper.setBuffer(text.start, text.end, text.buffer);
      byte[] b = transformer.transform(charSequenceWrapper).getBytes(java.nio.charset.Charset.forName("UTF-8"));
      buffer = buffer.reallocIfNeeded(b.length);
      buffer.setBytes(0, b);
      out.buffer = buffer;
      out.start = 0;
      out.end = b.length;
    } // end of eval
  }
}
