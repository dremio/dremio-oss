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

import java.util.regex.Pattern;

import com.dremio.exec.proto.UserBitShared.ExceptionWrapper;
import com.dremio.exec.proto.UserBitShared.StackTraceElementWrapper;

/**
 * Utility class that handles error message generation.
 */
public class ErrorHelper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ErrorHelper.class);

  private static final Pattern IGNORE = Pattern.compile("^(sun|com\\.sun|java).*");

  /**
   * Constructs the root error message in the form [root exception class name]: [root exception message]
   *
   * @param cause exception we want the root message for
   * @return root error message or empty string if none found
   */
  static String getRootMessage(final Throwable cause) {
    String message = "";

    Throwable ex = cause;
    while (ex != null) {
      message = ex.getClass().getSimpleName();
      if (ex.getMessage() != null) {
        message += ": " + ex.getMessage();
      }

      if (ex.getCause() != null && ex.getCause() != ex) {
        ex = ex.getCause();
      } else {
        break;
      }
    }

    return message;
  }


  static String buildCausesMessage(final Throwable t) {

    StringBuilder sb = new StringBuilder();
    Throwable ex = t;
    boolean cause = false;
    while(ex != null){

      sb.append("  ");

      if(cause){
        sb.append("Caused By ");
      }

      sb.append("(");
      sb.append(ex.getClass().getCanonicalName());
      sb.append(") ");
      sb.append(ex.getMessage());
      sb.append("\n");

      for(StackTraceElement st : ex.getStackTrace()){
        sb.append("    ");
        sb.append(st.getClassName());
        sb.append('.');
        sb.append(st.getMethodName());
        sb.append("():");
        sb.append(st.getLineNumber());
        sb.append("\n");
      }
      cause = true;

      if(ex.getCause() != null && ex.getCause() != ex){
        ex = ex.getCause();
      } else {
        ex = null;
      }
    }

    return sb.toString();
  }

  public static ExceptionWrapper getWrapper(Throwable ex) {
    return getWrapperBuilder(ex).build();
  }

  private static ExceptionWrapper.Builder getWrapperBuilder(Throwable ex) {
    return getWrapperBuilder(ex, false);
  }

  private static ExceptionWrapper.Builder getWrapperBuilder(Throwable ex, boolean includeAllStack) {
    ExceptionWrapper.Builder ew = ExceptionWrapper.newBuilder();
    if(ex.getMessage() != null) {
      ew.setMessage(ex.getMessage());
    }
    ew.setExceptionClass(ex.getClass().getName());
    boolean isHidden = false;
    StackTraceElement[] stackTrace = ex.getStackTrace();
    for(int i = 0; i < stackTrace.length; i++){
      StackTraceElement ele = ex.getStackTrace()[i];
      if(include(ele, includeAllStack)){
        if(isHidden){
          isHidden = false;
        }
        ew.addStackTrace(getSTWrapper(ele));
      }else{
        if(!isHidden){
          isHidden = true;
          ew.addStackTrace(getEmptyST());
        }
      }

    }

    if(ex.getCause() != null && ex.getCause() != ex){
      ew.setCause(getWrapper(ex.getCause()));
    }
    return ew;
  }

  private static boolean include(StackTraceElement ele, boolean includeAllStack) {
    return includeAllStack || !(IGNORE.matcher(ele.getClassName()).matches());
  }

  private static StackTraceElementWrapper.Builder getSTWrapper(StackTraceElement ele) {
    StackTraceElementWrapper.Builder w = StackTraceElementWrapper.newBuilder();
    w.setClassName(ele.getClassName());
    if(ele.getFileName() != null) {
      w.setFileName(ele.getFileName());
    }
    w.setIsNativeMethod(ele.isNativeMethod());
    w.setLineNumber(ele.getLineNumber());
    w.setMethodName(ele.getMethodName());
    return w;
  }

  private static StackTraceElementWrapper.Builder getEmptyST() {
    StackTraceElementWrapper.Builder w = StackTraceElementWrapper.newBuilder();
    w.setClassName("...");
    w.setIsNativeMethod(false);
    w.setLineNumber(0);
    w.setMethodName("...");
    return w;
  }

  /**
   * searches for an exception of type T wrapped inside the exception
   * @param ex exception
   * @return null if exception is null or no UserException was found
   */
  public static <T extends Throwable> T findWrappedCause(Throwable ex, Class<T> causeClass) {
    if (ex == null) {
      return null;
    }

    Throwable cause = ex;
    while (!causeClass.isInstance(cause)) {
      if (cause.getCause() != null && cause.getCause() != cause) {
        cause = cause.getCause();
      } else {
        return null;
      }
    }

    return (T) cause;
  }

}
