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

import { dremio } from "#oss/dremio";
import { queryClient } from "#oss/queryClient";
import { queryOptions, useMutation } from "@tanstack/react-query";
import { addNotification } from "#oss/actions/notification";
import { MSG_CLEAR_DELAY_SEC } from "#oss/constants/Constants";
import { store } from "#oss/store/store";

export type StarredResources = {
  entities: { id: string; starredAt: Date }[];
  type: "STARRED";
};

export const getStarsQueryKey = (pid?: string) =>
  pid ? [pid, "stars", "list"] : ["stars", "list"];

export const starredResourcesQuery = (_pid?: string) =>
  queryOptions({
    queryKey: getStarsQueryKey(),
    queryFn: ({ signal }) =>
      dremio
        ._sonarV3Request("users/preferences/STARRED", { signal })
        .then((res) => res.json())
        .then(
          (starredResources: StarredResources) =>
            new Set(starredResources.entities.map((entity) => entity.id)),
        ),
    retry: false,
    staleTime: 300_000,
  });

export const useAddStarMutation = (_pid?: string) =>
  useMutation({
    mutationFn: (resourceId: string) =>
      dremio
        ._sonarV3Request(`users/preferences/STARRED/${resourceId}`, {
          keepalive: true,
          method: "PUT",
        })
        .then((res) => res.json())
        .then(
          (starredResources: StarredResources) =>
            new Set(starredResources.entities.map((entity) => entity.id)),
        ),
    onSuccess: (starredEntities) => {
      queryClient.setQueryData(getStarsQueryKey(), starredEntities);
    },
    onError: (e) => {
      store.dispatch(
        addNotification((e as any).body.detail, "error", MSG_CLEAR_DELAY_SEC),
      );
    },
  });

export const useRemoveStarMutation = (_pid?: string) =>
  useMutation({
    mutationFn: (resourceId: string) =>
      dremio
        ._sonarV3Request(`users/preferences/STARRED/${resourceId}`, {
          keepalive: true,
          method: "DELETE",
        })
        .then((res) => res.json())
        .then(
          (starredResources: StarredResources) =>
            new Set(starredResources.entities.map((entity) => entity.id)),
        ),
    onSuccess: (starredEntities) => {
      queryClient.setQueryData(getStarsQueryKey(), starredEntities);
    },
    onError: (e) => {
      store.dispatch(
        addNotification((e as any).body.detail, "error", MSG_CLEAR_DELAY_SEC),
      );
    },
  });
