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

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionSyntax;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.sabot.exec.context.ContextInformation;
import javax.inject.Inject;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

@SuppressWarnings("unused")
public class ContextFunctions {

  /**
   * Implement "user", "session_user" or "system_user" function. Returns the username of the user
   * connected to SabotNode.
   */
  @FunctionTemplate(
      names = {"user", "session_user", "system_user"},
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      syntax = FunctionSyntax.FUNCTION_ID,
      isDynamic = true)
  public static class User implements SimpleFunction {
    @Output NullableVarCharHolder out;
    @Inject ContextInformation contextInfo;
    @Inject ArrowBuf buffer;
    @Workspace int queryUserBytesLength;

    @Override
    public void setup() {
      final byte[] queryUserNameBytes =
          contextInfo.getQueryUser().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      buffer = buffer.reallocIfNeeded(queryUserNameBytes.length);
      queryUserBytesLength = queryUserNameBytes.length;
      buffer.setBytes(0, queryUserNameBytes);
    }

    @Override
    public void eval() {
      out.isSet = 1;
      out.start = 0;
      out.end = queryUserBytesLength;
      out.buffer = buffer;
    }
  }

  /**
   * Function that returns the name of the user currently executing the query.
   *
   * <p>Difference between {@link User} and this function is, the latter can be constant folded and
   * the former cannot.
   */
  @FunctionTemplate(
      names = {"query_user"},
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      isDynamic = true)
  public static class QueryUserFunction implements SimpleFunction {
    @Output NullableVarCharHolder out;
    @Inject ContextInformation contextInfo;
    @Inject ArrowBuf buffer;
    @Workspace int queryUserBytesLength;

    @Override
    public void setup() {
      final byte[] queryUserNameBytes =
          contextInfo.getQueryUser().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      buffer = buffer.reallocIfNeeded(queryUserNameBytes.length);
      queryUserBytesLength = queryUserNameBytes.length;
      buffer.setBytes(0, queryUserNameBytes);
    }

    @Override
    public void eval() {
      out.isSet = 1;
      out.start = 0;
      out.end = queryUserBytesLength;
      out.buffer = buffer;
    }
  }

  /** Implement "current_schema" function. Returns the default schema in current session. */
  @FunctionTemplate(
      name = "current_schema",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      syntax = FunctionSyntax.FUNCTION_ID,
      isDynamic = true)
  public static class CurrentSchema implements SimpleFunction {
    @Output NullableVarCharHolder out;
    @Inject ContextInformation contextInfo;
    @Inject ArrowBuf buffer;
    @Workspace int currentSchemaBytesLength;

    @Override
    public void setup() {
      final byte[] currentSchemaBytes =
          contextInfo.getCurrentDefaultSchema().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      buffer = buffer.reallocIfNeeded(currentSchemaBytes.length);
      currentSchemaBytesLength = currentSchemaBytes.length;
      buffer.setBytes(0, currentSchemaBytes);
    }

    @Override
    public void eval() {
      out.isSet = 1;
      out.start = 0;
      out.end = currentSchemaBytesLength;
      out.buffer = buffer;
    }
  }

  /** Implement "last_query_id" function. Returns the last query id for the user. */
  @FunctionTemplate(
      name = "last_query_id",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      syntax = FunctionSyntax.FUNCTION_ID,
      isDynamic = true)
  public static class LastQueryId implements SimpleFunction {
    @Output NullableVarCharHolder out;
    @Inject ContextInformation contextInfo;
    @Inject ArrowBuf buffer;
    @Workspace int lastQueryIdLen = 0;

    @Override
    public void setup() {
      final com.dremio.exec.proto.UserBitShared.QueryId lastQueryId = contextInfo.getLastQueryId();
      if (lastQueryId != null) {
        final String queryID = com.dremio.common.utils.protos.QueryIdHelper.getQueryId(lastQueryId);
        lastQueryIdLen = queryID.length();
        buffer = buffer.reallocIfNeeded(lastQueryIdLen);
        buffer.setBytes(0, queryID.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      }
    }

    @Override
    public void eval() {
      if (lastQueryIdLen > 0) {
        out.isSet = 1;
        out.start = 0;
        out.end = lastQueryIdLen;
        out.buffer = buffer;
      }
    }
  }
}
