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

import { useEffect, useRef, useState } from "react";
import { SmartResource } from "smart-resource";
import { isEqual } from "lodash";

export const useResourceSnapshotDeep = <T>(resource: SmartResource<T>) => {
  const [snapshot, setSnapshot] = useState<Awaited<T> | null>(resource.value);
  const [error, setError] = useState<any>(resource.error);
  const currentSnapshot = useRef(snapshot);

  useEffect(() => {
    return resource.subscribe(
      (val) => {
        setError(null);
        if (!isEqual(val, currentSnapshot.current)) {
          setSnapshot(val);
          currentSnapshot.current = val;
        }
      },
      (err: any) => {
        setSnapshot(null);
        setError(err);
      },
    ).unsubscribe;
  }, [resource]);

  return [snapshot, error] as const;
};
