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
package com.dremio.exec.work.foreman;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public interface TerminationListenerRegistry {

  public TerminationListenerRegistry NOOP = new NoOpTerminationListenerRegistry();

  /**
   * Add a listener that will be informed when this connection is terminated.
   * @param listener
   */
  void addTerminationListener(GenericFutureListener<? extends Future<? super Void>> listener);

  /**
   * Remove a previously registered listener.
   * @param listener Listener to remove.
   */
  void removeTerminationListener(GenericFutureListener<? extends Future<? super Void>> listener);



  public class NoOpTerminationListenerRegistry implements TerminationListenerRegistry{

    private NoOpTerminationListenerRegistry(){}

    @Override
    public void addTerminationListener(GenericFutureListener<? extends Future<? super Void>> listener) {
    }

    @Override
    public void removeTerminationListener(GenericFutureListener<? extends Future<? super Void>> listener) {
    }

  }
}
