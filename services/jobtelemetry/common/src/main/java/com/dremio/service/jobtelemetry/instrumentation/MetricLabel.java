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
package com.dremio.service.jobtelemetry.instrumentation;

public class MetricLabel {
  public static final String JTS_METRIC_TAG_KEY_RPC = "rpc";
  public static final String JTS_METRIC_TAG_VALUE_RPC_PUT_QUERY_TAIL_PROFILE =
      "PUT_QUERY_TAIL_PROFILE";
  public static final String JTS_METRIC_TAG_VALUE_RPC_PUT_EXECUTOR_PROFILE = "PUT_EXECUTOR_PROFILE";
  public static final String JTS_METRIC_TAG_VALUE_RPC_SEND_TAIL_PROFILE = "SEND_TAIL_PROFILE";
  public static final String JTS_METRIC_TAG_KEY_ERROR_ORIGIN = "error_origin";
  public static final String JTS_METRIC_TAG_VALUE_NODE_QUERY_COMPLETE = "NODE_QUERY_COMPLETE";
  public static final String JTS_METRIC_TAG_VALUE_ATTEMPT_CLOSE = "ATTEMPT_CLOSE";
  public static final String JTS_METRIC_TAG_VALUE_NEW_ATTEMPT = "NEW_ATTEMPT";
  public static final String JTS_METRIC_TAG_VALUE_COORD_EXEC_NODE_QUERY_PROFILE =
      "COORD_EXEC_NODE_QUERY_PROFILE";
  public static final String JTS_METRIC_TAG_VALUE_START_JOB = "START_JOB";
}
