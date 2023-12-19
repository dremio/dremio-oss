
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

import { SmartResource } from "smart-resource";
import { BehaviorSubject } from "rxjs";

export const behaviorSubjectFromResource = <T>(resource: SmartResource<T>) => {
  const $behaviorSubject = new BehaviorSubject(resource.getResource().value);
  resource.subscribe(next => {
    if (next.status === "success") {
      $behaviorSubject.next(next.value)
    }
  })
  return $behaviorSubject;
}
