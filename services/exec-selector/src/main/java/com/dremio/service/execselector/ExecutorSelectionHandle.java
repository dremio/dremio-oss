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
package com.dremio.service.execselector;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import java.util.Collection;

/**
 * Handle returned by the {@link ExecutorSelectionService}.
 *
 * <p>It is the means by which users of the {@link ExecutorSelectionService} obtain the execution
 * endpoints for a query
 */
public interface ExecutorSelectionHandle extends AutoCloseable {
  Collection<NodeEndpoint> getExecutors();

  /** Plan details for the executor selection, to be exposed in the profile */
  String getPlanDetails();
}
