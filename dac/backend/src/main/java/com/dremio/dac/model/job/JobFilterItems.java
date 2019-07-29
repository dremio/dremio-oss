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
package com.dremio.dac.model.job;


import java.util.ArrayList;
import java.util.List;

import com.dremio.dac.util.JSONUtil;

/**
 * JobFilterItems model
 */
public class JobFilterItems {
  private List<JobFilterItem> items;

  public JobFilterItems() {
    items = new ArrayList<>();
  }

  public void add(JobFilterItem item) {
    items.add(item);
  }

  public List<JobFilterItem> getItems() {
    return items;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }
}
