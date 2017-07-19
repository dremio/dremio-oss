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
package com.dremio.common.exceptions;

import java.util.List;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.DremioPBError;

/**
 * Wraps a DremioPBError object so we don't need to rebuilt it multiple times when sending it to the client. It also
 * gives access to the original exception className and message.
 */
public class UserRemoteException extends UserException {

  private final DremioPBError error;

  public UserRemoteException(DremioPBError error) {
    super(error.getErrorType(), "Dremio Remote Exception", null);
    this.error = error;
  }

  @Override
  public String getMessage() {
    return error.getMessage();
  }

  @Override
  public String getOriginalMessage() {
    return error.getOriginalMessage();
  }

  @Override
  public String getVerboseMessage() {
    return getVerboseMessage(true);
  }

  @Override
  public String getVerboseMessage(boolean includeErrorIdAndIdentity) {
    StringBuilder sb = new StringBuilder();
    sb.append(error.getMessage());

    if (error.getContextCount() > 0) {
      sb.append("\n\n");
      for (String context : error.getContextList()) {
        sb.append(context).append("\n");
      }
    }

    if (error.hasException()) {
      sb.append("\n");
      for (UserBitShared.StackTraceElementWrapper stackLine : error.getException().getStackTraceList()) {
        sb.append(stackLine.getClassName())
          .append("(")
          .append(stackLine.getFileName())
          .append(":")
          .append(stackLine.getLineNumber())
          .append(")\n");
      }
    }
    return sb.toString();
  }

  @Override
  public DremioPBError getOrCreatePBError(boolean verbose) {
    return error;
  }

  @Override
  public List<String> getContextStrings() {
    return error.getContextList();
  }
}
