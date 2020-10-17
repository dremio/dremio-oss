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
package com.dremio.service.flight.protector;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import javax.inject.Provider;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.work.protector.UserResponseHandler;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.flight.error.mapping.DremioFlightErrorMapper;

/**
 * A UserResponseHandler which can cancel the job. A cancellation from the request will automatically
 * cancel the job.
 *
 * @param <T> The response type.
 */
public abstract class CancellableUserResponseHandler<T> implements UserResponseHandler {
  private final CompletableFuture<T> future = new CompletableFuture<>();
  private final Supplier<Boolean> isRequestCancelled;
  private final UserBitShared.ExternalId externalId;
  private final UserSession userSession;
  private final Provider<UserWorker> workerProvider;

  public CancellableUserResponseHandler(UserBitShared.ExternalId externalId,
                                        UserSession userSession,
                                        Provider<UserWorker> workerProvider,
                                        Supplier<Boolean> isRequestCancelled) {
    this.externalId = externalId;
    this.userSession = userSession;
    this.workerProvider = workerProvider;
    this.isRequestCancelled = isRequestCancelled;
  }

  public T get() {
    while (true) {
      try {
        return future.get(100, TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        final Throwable cause = e.getCause();

        if (cause instanceof FlightRuntimeException) {
          throw (FlightRuntimeException) cause;
        } else if (cause instanceof UserException) {
          throw DremioFlightErrorMapper.toFlightRuntimeException((UserException) cause);
        } else {
          throw CallStatus.INTERNAL
            .withCause(cause)
            .withDescription(cause.getLocalizedMessage())
            .toRuntimeException();
        }
      } catch (InterruptedException e) {
        handleClientCancel(e);
        throw CallStatus.INTERNAL
          .withCause(UserException.parseError(e).buildSilently())
          .withDescription(e.getLocalizedMessage())
          .toRuntimeException();
      } catch (TimeoutException e) {
        handleClientCancel(e);
        // Fallthrough to continue.
      }
    }
  }

  private void handleClientCancel(Exception e) {
    if (isRequestCancelled.get()) {
      cancelJob();
      throw CallStatus.CANCELLED
        .withDescription("Call cancelled by client application.")
        .toRuntimeException();
    }
  }

  public void cancelJob() {
    if (!future.isDone()) {
      workerProvider.get().cancelQuery(externalId, userSession.getTargetUserName());
    }
  }

  protected CompletableFuture<T> getCompletableFuture() {
    return future;
  }
}
