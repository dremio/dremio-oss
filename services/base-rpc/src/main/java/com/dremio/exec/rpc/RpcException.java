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
package com.dremio.exec.rpc;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.DremioStringUtils;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserBitShared.ExceptionWrapper;
import com.google.common.base.Throwables;

/**
 * Parent class for all rpc exceptions.
 */
public class RpcException extends IOException {
  private static final long serialVersionUID = -5964230316010502319L;
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RpcException.class);
  private final String status;
  private final String errorId;

  public RpcException() {
    super();
    this.status = null;
    this.errorId = null;
  }

  public RpcException(String message, Throwable cause) {
    this(message, null, null, cause);
  }

  public RpcException(String errMsg, String status, String errorId, Throwable cause) {
    super(format(errMsg), cause);
    this.status = status;
    this.errorId = errorId;
  }

  public RpcException(String errMsg, String status, String errorId) {
    super(format(errMsg));
    this.status = status;
    this.errorId = errorId;
  }

  private static String format(String message) {
    return DremioStringUtils.unescapeJava(message);
  }

  public RpcException(String message) {
    this(message, null, null);
  }

  public RpcException(Throwable cause) {
    super(cause);
    this.status = null;
    this.errorId = null;
  }

  public static RpcException mapException(Throwable t) {
    while (t instanceof ExecutionException) {
      t = t.getCause();
    }
    if (t instanceof RpcException) {
      return ((RpcException) t);
    }
    return new RpcException(t);
  }

  public static RpcException mapException(String message, Throwable t) {
    while (t instanceof ExecutionException) {
      t = t.getCause();
    }
    return new RpcException(message, t);
  }

  public boolean isRemote(){
    return false;
  }

  public DremioPBError getRemoteError(){
    throw new UnsupportedOperationException();
  }

  /**
   * Unwrap the provided exception, and throws the underlying "cause" if an instance of clazz.
   *
   * For {@code RpcException} wrapping {@code UserRemoteException}, try to unwrap the
   * {@code DremioPBError} instance, and skip over the {@code UserRpcException} instances to find
   * the root cause of the remote exception.
   *
   * If the method is able to build an instance of the exception by reflection, and if the exception is
   * of type {@code T}, or is a unchecked exception, throws the exception. Otherwise does nothing.
   *
   * @param e the exception to unwrap
   * @param clazz the exception class
   * @throws T if the underlying cause of e
   */
  public static <T extends Throwable> void propagateIfPossible(@Nullable RpcException e, Class<T> clazz) throws T {
    if (e == null) {
      return;
    }

    Throwable cause = e.getCause();
    if (!(cause instanceof UserRemoteException)) {
      return;
    }

    UserRemoteException ure = (UserRemoteException) e.getCause();
    DremioPBError remoteError = ure.getOrCreatePBError(false);

    ExceptionWrapper ew = remoteError.getException();
    Class<? extends Throwable> exceptionClazz;
    do {
      try {
        exceptionClazz = Class.forName(ew.getExceptionClass()).asSubclass(Throwable.class);
      } catch (ClassNotFoundException cnfe) {
        logger.info("Cannot deserialize exception " + ew.getExceptionClass(), cnfe);
        return;
      }

      // Skip over UserRpcException instances
      if (UserRpcException.class.isAssignableFrom(exceptionClazz)) {
        ew = ew.getCause();
        continue;
      }
      break;
    } while(true);

    Throwable t;
    try {
      Constructor<? extends Throwable> constructor = exceptionClazz.getConstructor(String.class, Throwable.class);
      t = constructor.newInstance(ew.getMessage(), e);
    } catch(ReflectiveOperationException nsme) {
      Constructor<? extends Throwable> constructor;
      try {
        constructor = exceptionClazz.getConstructor(String.class);
        t = constructor.newInstance(ew.getMessage()).initCause(e);
      } catch (ReflectiveOperationException rfe) {
        logger.info("Cannot deserialize exception " + ew.getExceptionClass(), rfe);
        return;
      }
    }
    Throwables.propagateIfPossible(t, clazz);
  }

  public String getStatus() {
    return status;
  }

  public String getErrorId() {
    return errorId;
  }
}
