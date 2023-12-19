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
import { useCallback, useEffect } from "react";
import { useResourceSnapshot, useResourceStatus } from "smart-resource/react";
import { SonarContentsResource } from "../resources/SonarContentsResource";
import { useStore } from "react-redux";

export const useSonarContents = ({
  path,
  entityType,
  refType,
  refValue,
}: {
  path: string;
  entityType: string;
  refType?: string;
  refValue?: string;
}) => {
  const doFetch = useCallback(() => {
    if (path && refType && refValue) {
      SonarContentsResource.fetch({
        path,
        entityType,
        params: { refType, refValue },
      });
    }
  }, [path, entityType, refType, refValue]);

  useEffect(doFetch, [doFetch]);

  const store = useStore();
  useEffect(() => {
    const unsubscribe = store.subscribe(() => {
      const {
        action: { meta },
      } = store.getState() as any;

      if (meta?.invalidateViewIds?.includes("HomeContents")) {
        doFetch();
      }
    });
    return () => unsubscribe();
  }, [doFetch, store, path]);

  return [
    ...useResourceSnapshot(SonarContentsResource),
    useResourceStatus(SonarContentsResource),
  ] as const;
};
