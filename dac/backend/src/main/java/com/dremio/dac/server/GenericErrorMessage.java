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
package com.dremio.dac.server;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Error message representation for unexpected/unhandled exceptions.<br>
 * We should always use UserExceptions to report errors to the user, anything that makes it past UserExceptionMapper
 * will be treated as a system exception and a generic message will be displayed.
 */
public class GenericErrorMessage {

  public static final String GENERIC_ERROR_MSG = "Something went wrong";
  public static final String NO_USER_MSG = "No User Available";

  private final String errorMessage;
  private final String moreInfo;
  private final String[] stackTrace;

  public GenericErrorMessage(String errorMessage) {
    this(errorMessage, "", new String[0]);
  }

  public GenericErrorMessage(String moreInfo, String[] stackTrace) {
    this(GENERIC_ERROR_MSG, moreInfo, stackTrace);
  }

  @JsonCreator
  public GenericErrorMessage(
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("moreInfo") String moreInfo,
      @JsonProperty("stackTrace") String[] stackTrace) {
    this.errorMessage = errorMessage;
    this.moreInfo = moreInfo;
    this.stackTrace = stackTrace;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getMoreInfo() {
    return moreInfo;
  }

  public String[] getStackTrace() {
    return stackTrace;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }

  public static String[] printStackTrace(Throwable throwable){
    String[] stackTrace;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (PrintWriter s = new PrintWriter(out)) {
      throwable.printStackTrace(s);
    }
    stackTrace = out.toString().split("\n");
    for (int i = 0; i < stackTrace.length; i++) {
      String ste = stackTrace[i];
      stackTrace[i] = ste.replaceAll("\t", "  ");
    }
    return stackTrace;
  }
}
