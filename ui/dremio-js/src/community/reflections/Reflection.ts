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

import type {
  ReflectionInterface,
  ReflectionProperties,
} from "../../interfaces/Reflection.js";

export class Reflection implements ReflectionInterface {
  createdAt: ReflectionInterface["createdAt"];
  dataset: ReflectionInterface["dataset"];
  id: ReflectionInterface["id"];
  isArrowCachingEnabled: ReflectionInterface["isArrowCachingEnabled"];
  isCanAlter: ReflectionInterface["isCanAlter"];
  isCanView: ReflectionInterface["isCanView"];
  metrics: ReflectionInterface["metrics"];
  name: ReflectionInterface["name"];
  status: ReflectionInterface["status"];
  type: ReflectionInterface["type"];
  updatedAt: ReflectionInterface["updatedAt"];
  constructor(properties: ReflectionProperties) {
    this.createdAt = properties.createdAt;
    this.dataset = properties.dataset;
    this.id = properties.id;
    this.isArrowCachingEnabled = properties.isArrowCachingEnabled;
    this.isCanAlter = properties.isCanAlter;
    this.isCanView = properties.isCanView;
    this.metrics = properties.metrics;
    this.name = properties.name;
    this.status = properties.status;
    this.type = properties.type;
    this.updatedAt = properties.updatedAt;
  }
}
