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
package com.dremio.common.exceptions;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a DremioPBError object so we don't need to rebuilt it multiple times when sending it to the
 * client. It also gives access to the original exception className and message.
 */
public final class UserRemoteException extends UserException {

  private static final Logger logger = LoggerFactory.getLogger(UserRemoteException.class);
  private final DremioPBError error;

  private UserRemoteException(DremioPBError error) {
    super(error.getErrorType(), "Dremio Remote Exception", null, error.getTypeSpecificContext());
    super.addErrorOrigin(error.getErrorOrigin());
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
    if (error.getErrorOrigin() != null) {
      sb.append("\nErrorOrigin: ");
      sb.append(error.getErrorOrigin());
      sb.append("\n");
    }
    if (error.hasTypeSpecificContext()) {
      if (error.getErrorType() == DremioPBError.ErrorType.OUT_OF_MEMORY) {
        try {
          OutOfMemoryOrResourceExceptionContext oomExceptionContext =
              OutOfMemoryOrResourceExceptionContext.fromUserException(this);
          sb.append("OOM Type: ");
          sb.append(oomExceptionContext.getMemoryType());
          sb.append("\n");
          sb.append("OOM Details: ");
          sb.append(oomExceptionContext.getAdditionalInfo());
        } catch (Exception e) {
          logger.error("Exception during parsing OutOfMemoryExceptionContext, but ignored ", e);
        }
      }
    }

    if (error.hasException()) {
      sb.append("\n");
      for (UserBitShared.StackTraceElementWrapper stackLine :
          error.getException().getStackTraceList()) {
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

  /**
   * Creates (or deserializes) a {@link UserRemoteException} from a {@link DremioPBError}.
   *
   * @param error serialized error message
   * @return user remote exception
   */
  public static UserRemoteException create(DremioPBError error) {
    return new UserRemoteException(error);
  }
}
