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
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/GHashFunctions.java" />

<#include "/@includes/license.ftl" />

  package com.dremio.exec.expr.fn.impl;

<#include "/@includes/vv_imports.ftl" />

  import com.dremio.exec.expr.SimpleFunction;
  import com.dremio.exec.expr.annotations.FunctionTemplate;
  import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
  import com.dremio.exec.expr.annotations.Output;
  import com.dremio.exec.expr.annotations.Workspace;
  import com.dremio.exec.expr.annotations.Param;
  import com.dremio.exec.expr.fn.FunctionErrorContext;
  import org.apache.arrow.vector.holders.*;

  import io.netty.buffer.ByteBuf;
  import org.apache.arrow.memory.ArrowBuf;
  import org.apache.commons.codec.digest.DigestUtils;
  import javax.inject.Inject;
  import java.nio.charset.Charset;

/*
 * This class is automatically generated from HashFunctions.java using FreeMarker.
 */

public class GHashFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GHashFunctions.class);
  <#list hashTypes.hashFunctionsTypes as hashType>
  <#list hashType.types as type>

  @FunctionTemplate(names = {"${hashType.funcName}", "${hashType.aliasName}"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ${type}${hashType.className} implements SimpleFunction{
    @Param  ${type}Holder in;
    @Inject ArrowBuf buffer;
    @Output VarCharHolder out;
    @Workspace Charset charset;
    @Inject FunctionErrorContext errCtx;

    @Override
    public void setup() {
      charset = java.nio.charset.StandardCharsets.UTF_8;
    }

    @Override
    public void eval() {
<#if type == "Int" || type == "TimeMilli" ||  type == "Bit">
      byte[] buf = ByteBuffer.allocate(in.WIDTH).putInt(in.value).array();
<#elseif type == "BigInt" || type == "DateMilli" || type == "TimeStampMilli">
      byte[] buf = ByteBuffer.allocate(in.WIDTH).putLong(in.value).array();
<#elseif type == "Float4">
      byte[] buf = ByteBuffer.allocate(in.WIDTH).putFloat(in.value).array();
<#elseif type == "Float8">
      byte[] buf = ByteBuffer.allocate(in.WIDTH).putDouble(in.value).array();
<#elseif type == "VarBinary" || type == "VarChar">
      byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf);
<#elseif type == "Decimal">
      byte[] buf = com.dremio.common.util.DremioStringUtils.toBinaryStringNoFormat(io.netty.buffer.NettyArrowBuf.unwrapBuffer(in.buffer), (int) in
        .start, 16).getBytes();
<#else>
      byte[] buf = ${type}.toBinaryString(in.value).getBytes();
</#if>
<#if hashType.aliasName == "sha1">
      byte[] hash_buf= org.apache.commons.codec.digest.DigestUtils.shaHex(buf).getBytes();
<#else>
      byte[] hash_buf= org.apache.commons.codec.digest.DigestUtils.${hashType.aliasName}Hex(buf).getBytes();
</#if>
      buffer.setBytes(0, hash_buf);
      out.start = 0;
      out.end = hash_buf.length;
      out.buffer = buffer;
    }
  }
  </#list>
  </#list>
}




