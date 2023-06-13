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
package com.dremio.exec.store.sys.udf;

import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

import com.dremio.common.expression.CompleteType;
import com.dremio.service.namespace.function.proto.FunctionArg;
import com.dremio.service.namespace.function.proto.FunctionBody;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.function.proto.FunctionDefinition;
import com.dremio.service.namespace.function.proto.ReturnType;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

public final class UserDefinedFunctionSerde {
  private UserDefinedFunctionSerde() {}

  public static UserDefinedFunction fromProto(FunctionConfig functionConfig) {
    FunctionDefinition functionDefinition = functionConfig.getFunctionDefinitionsList().get(0);
    List<UserDefinedFunction.FunctionArg> functionArgList =
      functionDefinition.getFunctionArgList() == null
        ? ImmutableList.of()
        : functionDefinition
          .getFunctionArgList()
          .stream()
          .map(functionArg ->
            new UserDefinedFunction.FunctionArg(
              functionArg.getName(),
              CompleteType.deserialize(functionArg.getRawDataType().toByteArray()),
              functionArg.getDefaultExpression()))
          .collect(ImmutableList.toImmutableList());
    return new UserDefinedFunction(
      functionConfig.getName(),
      functionDefinition.getFunctionBody().getRawBody(),
      CompleteType.deserialize(functionConfig.getReturnType().getRawDataType().toByteArray()),
      functionArgList,
      functionConfig.getFullPathList(),
      functionDefinition.getFunctionBody().getSerializedPlan().toByteArray(),
      (functionConfig.getCreatedAt() != null) ? new Timestamp(functionConfig.getCreatedAt()) : null,
      (functionConfig.getLastModified() != null) ? new Timestamp(functionConfig.getLastModified()):null);
  }

  public static FunctionConfig toProto(UserDefinedFunction userDefinedFunction) {
    return new FunctionConfig()
      .setName(userDefinedFunction.getName())
        .setFunctionDefinitionsList(ImmutableList.of(
          new FunctionDefinition().setFunctionBody(
            new FunctionBody()
              .setRawBody(userDefinedFunction.getFunctionSql())
              .setSerializedPlan(ByteString.copyFrom(userDefinedFunction.getSerializedFunctionPlan())))
        .setFunctionArgList(userDefinedFunction.getFunctionArgsList().stream()
          .map(functionArg ->
            new FunctionArg()
              .setName(functionArg.getName())
              .setRawDataType(ByteString.copyFrom(functionArg.getDataType().serialize()))
              .setDefaultExpression(functionArg.getDefaultExpression()))
          .collect(Collectors.toList()))
      ))
      .setFullPathList(userDefinedFunction.getFullPath())
      .setReturnType(new ReturnType().setRawDataType(ByteString.copyFrom(userDefinedFunction.getReturnType().serialize())));
  }
}
