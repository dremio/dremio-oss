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

import com.dremio.exec.proto.FunctionRPC;
import com.dremio.service.Service;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.function.proto.FunctionDefinition;
import java.sql.Timestamp;

/** Exposes the Udf store to the execution engine */
public interface UserDefinedFunctionService extends Service {

  Iterable<FunctionInfo> functionInfos();

  Iterable<FunctionInfo> getFunctions();

  class FunctionInfo {
    public final String argList;
    public final String name;
    public final String sql;
    public final String returnType;
    public final Timestamp createdAt;
    public final Timestamp modifiedAt;
    public String owner;

    public FunctionInfo(
        String name,
        String sql,
        String argList,
        String returnType,
        Timestamp createdAt,
        Timestamp modifiedAt,
        String owner) {
      this.argList = argList;
      this.name = name;
      this.sql = sql;
      this.createdAt = createdAt;
      this.returnType = returnType;
      this.modifiedAt = modifiedAt;
      this.owner = owner;
    }

    public void setOwner(String owner) {
      this.owner = owner;
    }

    public boolean isValid() {
      return name != null;
    }

    public static FunctionInfo fromProto(FunctionRPC.FunctionInfo functionInfo) {

      return new FunctionInfo(
          functionInfo.getName(),
          functionInfo.getSql(),
          functionInfo.getArgList(),
          functionInfo.getReturnType(),
          new Timestamp(functionInfo.getCreatedAt()),
          new Timestamp(functionInfo.getLastModifiedAt()),
          functionInfo.getOwner());
    }

    public FunctionRPC.FunctionInfo toProto() {
      return FunctionRPC.FunctionInfo.newBuilder()
          .setName((name == null) ? "null" : name)
          .setSql((sql == null) ? "null" : sql)
          .setArgList((argList == null) ? "null" : argList)
          .setReturnType((returnType == null) ? "null" : returnType)
          .setCreatedAt(createdAt.getTime())
          .setOwner((owner == null) ? "null" : owner)
          .build();
    }
  }

  static FunctionInfo getFunctionInfoFromConfig(FunctionConfig functionConfig) {
    final FunctionDefinition functionDefinition =
        functionConfig.getFunctionDefinitionsList() == null
            ? FunctionDefinition.getDefaultInstance()
            : functionConfig.getFunctionDefinitionsList().isEmpty()
                ? FunctionDefinition.getDefaultInstance()
                : functionConfig.getFunctionDefinitionsList().get(0);
    return new FunctionInfo(
        (functionConfig.getName() != null) ? functionConfig.getName() : null,
        (functionDefinition.getFunctionBody() != null
                && functionDefinition.getFunctionBody().getRawBody() != null)
            ? functionDefinition.getFunctionBody().getRawBody()
            : null,
        (functionDefinition.getFunctionArgList() != null)
            ? functionDefinition.getFunctionArgList().toString()
            : null,
        (functionConfig.getReturnType() != null) ? functionConfig.getReturnType().toString() : null,
        (functionConfig.getCreatedAt() != null)
            ? new Timestamp(functionConfig.getCreatedAt())
            : null,
        (functionConfig.getLastModified() != null)
            ? new Timestamp(functionConfig.getLastModified())
            : null,
        null);
  }
}
