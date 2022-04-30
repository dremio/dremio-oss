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
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/GUnionFunctions.java" />


<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

<#include "/@includes/vv_imports.ftl" />
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.complex.impl.*;
import javax.inject.Inject;
        import org.apache.arrow.memory.ArrowBuf;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

@SuppressWarnings("unused")
/**
 * Additional functions can be found in the class UnionFunctions
 */
public class GUnionFunctions {

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>
  <#assign typeMapping = TypeMappings[minor.class]!{}>
  <#assign dremioMinorType = typeMapping.minor_type!minor.class?upper_case>
  <#assign supported = typeMapping.supported!true>
  <#if supported>
  <#if !minor.class?starts_with("Decimal") && !minor.class?starts_with("UInt") && !minor.class?starts_with("SmallInt") && !minor.class?starts_with("TinyInt") && !minor.class?starts_with("IntervalMonthDayNano") >

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "IS_${dremioMinorType}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
  public static class UnionIs${name} implements SimpleFunction {

    @Param UnionHolder in;
    @Output NullableBitHolder out;

    public void setup() {}

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 1) {
        out.value = in.getMinorType() == org.apache.arrow.vector.types.Types.MinorType.${name?upper_case} ? 1 : 0;
      } else {
        out.value = 0;
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "ASSERT_${dremioMinorType}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
  public static class CastUnion${name} implements SimpleFunction {

    @Param UnionHolder in;
    @Output Nullable${name}Holder out;

    public void setup() {}

    public void eval() {
      if (in.isSet == 1) {
        in.reader.read(out);
      } else {
        out.isSet = 0;
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "castUNION", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
  public static class CastToUnion${name} implements SimpleFunction {

    @Param Nullable${name}Holder in;
    @Output UnionHolder out;

    public void setup() {}

    public void eval() {
      if (in.isSet == 1) {
        org.apache.arrow.vector.complex.impl.Nullable${name}HolderReaderImpl holderReaderImpl = new org.apache.arrow.vector.complex.impl.Nullable${name}HolderReaderImpl(in);
        out.reader = holderReaderImpl;
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }
  }

  </#if>
  </#if>
  </#list></#list>

}
