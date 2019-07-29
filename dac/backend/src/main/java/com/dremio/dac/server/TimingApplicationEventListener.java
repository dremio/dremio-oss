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

import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.Ticker;

/**
 * To track how much time is spent in requests
 */
public class TimingApplicationEventListener implements ApplicationEventListener {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TimingApplicationEventListener.class);

  private static long nextReqId = 0;

  @Override
  public void onEvent(ApplicationEvent event) {
    logger.info("ApplicationEventListener.onEvent " + event.getType());
  }

  @Override
  public RequestEventListener onRequest(RequestEvent event) {
    if (Timer.enabled()) {
      return new TimingRequestEventListener(nextReqId ++);
    }
    return null;
  }

  /**
   * Tracks individual requests
   */
  public static final class TimingRequestEventListener implements RequestEventListener {
    private final Ticker ticker = Timer.longLivedTicker();
    private final int reqEId = ticker.nextId();
    private final long reqId;
    private int callId;

    private TimingRequestEventListener(long reqId) {
      this.reqId = reqId;
      ticker.tick("req", reqEId);
    }

    @Override
    public void onEvent(RequestEvent event) {
      switch (event.getType()) {
      case RESOURCE_METHOD_START:
        callId = ticker.nextId();
        ticker.tick("callRes", callId);
        break;
      case RESOURCE_METHOD_FINISHED:
        ticker.tock("callRes", callId);
        break;
      case FINISHED:
        ticker.tock("req", reqEId);
        ContainerRequest req = event.getContainerRequest();
        String endpoint = req.getMethod() + " " + req.getRequestUri().toString().substring(req.getBaseUri().toString().length());
        ticker.log(reqId, endpoint);
        Timer.release();
        break;
        default: // do nothing
      }
    }

  }

}
