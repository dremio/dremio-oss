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
package com.dremio.exec.physical.base;

import com.dremio.exec.planner.fragment.MinorDataReader;
import com.dremio.exec.planner.fragment.MinorDataWriter;

/**
 * Operator that has attributes that are specific to the minor version.
 */
public interface OpWithMinorSpecificAttrs {
  /*
   * Collect attributes that are specific to a minor fragment.
   * @param writer
   */
  void collectMinorSpecificAttrs(MinorDataWriter writer) throws Exception;

  /*
   * Populate attributes that are specific to a minor fragment.
   * @param reader
   */
  void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception;
}
