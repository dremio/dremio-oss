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
package com.dremio.dac.util;

public interface QueryProfileConstant {

  String CPU = "cpu";
  String PREL = "Prel";
  String NODE_NAME = "\"op\"";
  String INPUTS = "\"inputs\"";
  String BLANK_SPACE = "";
  String DOT = ".";
  String OPEN_BRACKET = "\\[";
  String CLOSED_BRACKET = "\\]";
  String COMMA_SPACE_SEPERATOR = ", ";
  String HYPHEN = "-";
  String DOUBLE_QUOTE = "\"";
  String MEMORY = "memory";
  String PROCESSINGTIME = "processingTime";
  String WAITTIME = "waitTime";
  String SETUPTIME = "setupTime";
  String ROWCOUNT = "totalRowCount";
  String BYTES_PROCESSED = "totalBytesProcessed";
  String BYTES = "BYTES_";
  String DEFAULT_NODEID = "00";
  String DEFAULT_NULL = "null";
  String PHASE = "Phase";
  int DEFAULT_INDEX = 0;
  long DEFAULT_LONG = -1L;
  Boolean DEFAULT_IND = false;
  String REFLECTION_PREFIX = "__accelerator";
  long ONE_SEC = (long) 1e9;
  int MINIMUM_THREADS_TO_CHECK_FOR_SKEW = 3;
}
