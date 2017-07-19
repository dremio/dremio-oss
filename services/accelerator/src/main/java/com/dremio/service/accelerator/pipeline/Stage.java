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
package com.dremio.service.accelerator.pipeline;

/**
 * An abstraction that represents an individual stage in {@link Pipeline}
 */
public interface Stage {

  /**
   * Executes this stage with the given context.
   *
   * Invoking thread is blocked until stage commits.
   *
   * @param context  execution context within which stage will execute
   */
  void execute(StageContext context);

}
