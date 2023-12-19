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
<@pp.dropOutputFile />


<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/GMapFunctions.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.holders.*;

public class GMapFunctions {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GMapFunctions.class);

  private GMapFunctions(){}

  <#list MapFunctions.types as holder>

  @FunctionTemplate(names = {MapFunctions.LAST_MATCHING_ENTRY_FUNC}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL, derivation = MapFunctions.KeyValueOutputLastMatching.class)
  public static class GetLastMatchingMapEntryFor${holder}Key implements SimpleFunction {

    @Param FieldReader input;
    @Param ${holder} inputKey;
    @Output BaseWriter.ComplexWriter out;

    public void setup() {
    }

    public void eval() {
     org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter structWriter = out.rootAsStruct();
     if (input.isSet() && inputKey.isSet == 1) {
       org.apache.arrow.vector.complex.impl.UnionMapReader mapReader = (org.apache.arrow.vector.complex.impl.UnionMapReader) input;
       int iteratorIdx = 0;
       int lastMatchedIdx = -1;
       while (mapReader.next()) {
         ${holder} keyHolder = new ${holder}();
         mapReader.key().read(keyHolder);
           <#switch holder>
             <#case "NullableVarCharHolder">
                boolean isEqual = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.equalsIgnoreCase(keyHolder.buffer, keyHolder.start, keyHolder.end, inputKey.buffer, inputKey.start, inputKey.end);
             <#break>
             <#case "NullableDecimalHolder">
                long index = (keyHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
                java.math.BigDecimal left = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(keyHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), keyHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

                index = (inputKey.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
                java.math.BigDecimal right = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(inputKey.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), inputKey.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

                boolean isEqual = left.compareTo(right) == 0;
             <#break>
             <#case "NullableVarBinaryHolder">
                boolean isEqual = org.apache.arrow.memory.util.ByteFunctionHelpers.equal(keyHolder.buffer, keyHolder.start, keyHolder.end, inputKey.buffer, inputKey.start, inputKey.end) == 1;
             <#break>
             <#default>
                boolean isEqual = keyHolder.value == inputKey.value;
           </#switch>
         if (isEqual) {
           lastMatchedIdx = iteratorIdx;
         }
         iteratorIdx++;
       }
       if (lastMatchedIdx != -1) {
         org.apache.arrow.vector.holders.UnionHolder mapEntryHolder = new org.apache.arrow.vector.holders.UnionHolder();
         mapReader.read(lastMatchedIdx, mapEntryHolder);
         org.apache.arrow.vector.complex.impl.ComplexCopier.copy(mapEntryHolder.reader, (org.apache.arrow.vector.complex.writer.FieldWriter) structWriter);
       }
     }
    }
  }

  </#list>
}
