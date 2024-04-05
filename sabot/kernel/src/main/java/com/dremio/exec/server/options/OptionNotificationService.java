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
package com.dremio.exec.server.options;

import com.dremio.options.OptionChangeNotification;
import com.dremio.options.OptionNotificationServiceGrpc;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import javax.inject.Provider;

/**
 * OptionNotificationService received a request from a coordinator after it performs a change on
 * system option and fetch the system option from kv store for cached option refresh.
 */
public class OptionNotificationService
    extends OptionNotificationServiceGrpc.OptionNotificationServiceImplBase {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(OptionNotificationService.class);

  private final Provider<SystemOptionManager> systemOptionManagerProvider;

  public OptionNotificationService(Provider<SystemOptionManager> optionManagerProvider) {
    this.systemOptionManagerProvider = optionManagerProvider;
  }

  @Override
  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  public void systemOptionFetch(
      OptionChangeNotification request, StreamObserver<Empty> responseObserver) {
    logger.debug("SystemOptionChangeNotificationRequest received: {}", request);
    try {
      (systemOptionManagerProvider.get()).populateCache();
    } catch (Exception e) {
      responseObserver.onError(e);
      return;
    }
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }
}
