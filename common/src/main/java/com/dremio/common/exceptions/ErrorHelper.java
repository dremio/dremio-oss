/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.io.IOException;
import java.util.regex.Pattern;

import com.dremio.common.serde.ProtobufByteStringSerDe;
import com.dremio.exec.proto.UserBitShared.ExceptionWrapper;
import com.dremio.exec.proto.UserBitShared.StackTraceElementWrapper;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

/**
 * Utility class that handles error message generation from protobuf error objects.
 */
public class ErrorHelper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ErrorHelper.class);

  private final static Pattern IGNORE = Pattern.compile("^(sun|com\\.sun|java).*");

  private static final ObjectMapper additionalContextMapper = new ObjectMapper();

  static {
    // for backward compatibility
    additionalContextMapper.enable(JsonGenerator.Feature.IGNORE_UNKNOWN);

    // register subtypes
    // TODO: use classpath scanning to register subtypes; that way impls can import classes without bringing
    // dependencies to "common" module
    additionalContextMapper.registerSubtypes(InvalidMetadataErrorContext.class);
  }

  /**
   * Deserialize type specific additional context information.
   *
   * @param byteString serialized byte string
   * @return additional exception context
   */
  public static AdditionalExceptionContext deserializeAdditionalContext(final ByteString byteString) {
    try {
      if (byteString == null || byteString.isEmpty()) {
        return null;
      }
      return ProtobufByteStringSerDe.readValue(additionalContextMapper.readerFor(AdditionalExceptionContext.class),
          byteString, ProtobufByteStringSerDe.Codec.NONE, logger);
    } catch (IOException ignored) {
      logger.debug("unable to deserialize additional exception context", ignored);
      return null;
    }
  }

  /**
   * Serialize type specific additional context information.
   *
   * @param typeContext additional exception context
   * @return serialized bytes
   */
  public static ByteString serializeAdditionalContext(final AdditionalExceptionContext typeContext) {
    try {
      return ProtobufByteStringSerDe.writeValue(additionalContextMapper, typeContext,
          ProtobufByteStringSerDe.Codec.NONE);
    } catch (JsonProcessingException ignored) {
      logger.debug("unable to serialize additional exception context", ignored);
      return null;
    }
  }

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
