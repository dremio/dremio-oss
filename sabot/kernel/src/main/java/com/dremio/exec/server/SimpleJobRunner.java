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
package com.dremio.exec.server;

/**
 * Able to run a query as a job on execution hosts. Available from the SabotContext.
 */
@FunctionalInterface
public interface SimpleJobRunner {
  /**
   * Run a query as a job. Will process synchronously and return when the job is
   * complete.
   * @param query query to run as a job
   * @param userName username of user to use for running the job
   * @throws Exception when the job failed. The exception caught is rethrown as is.
   * @throws IllegalStateException when the job is canceled.
   */
  void runQueryAsJob(String query, String userName, String queryType, String queryLabel) throws Exception;
}
