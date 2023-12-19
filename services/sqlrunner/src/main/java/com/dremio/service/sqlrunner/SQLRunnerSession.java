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
package com.dremio.service.sqlrunner;

import java.util.List;

import com.dremio.service.sqlrunner.proto.SQLRunnerSessionProto;

/**
 * SQLRunnerSession POJO that wraps the protobuf version
 */
public class SQLRunnerSession {
  private SQLRunnerSessionProto.SQLRunnerSession delegate;

  public SQLRunnerSession() {
    this.delegate = SQLRunnerSessionProto.SQLRunnerSession.getDefaultInstance();
  }

  public SQLRunnerSession(SQLRunnerSessionProto.SQLRunnerSession delegate) {
    this.delegate = delegate;
  }

  public SQLRunnerSession(String userId, List<String> scriptIds, String currentScriptId) {
    this.delegate = SQLRunnerSessionProto.SQLRunnerSession.newBuilder()
      .setUserId(userId)
      .addAllScriptIds(scriptIds)
      .setCurrentScriptId(currentScriptId)
      .build();
  }

  public String getUserId() {
    return delegate.getUserId();
  }

  public List<String> getScriptIds() {
    return delegate.getScriptIdsList();
  }

  public void setScriptIds(List<String> scriptIds) {
    delegate = delegate.toBuilder()
      .clearScriptIds()
      .addAllScriptIds(scriptIds)
      .build();
  }

  public String getCurrentScriptId() {
    return delegate.getCurrentScriptId();
  }

  public void setCurrentScriptId(String currentScriptId) {
    delegate = delegate.toBuilder().setCurrentScriptId(currentScriptId).build();
  }

  public SQLRunnerSessionProto.SQLRunnerSession toProtobuf() {
    return delegate;
  }

}
