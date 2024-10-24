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
import { CommunityScript } from "@dremio/dremio-js/interfaces";
import { queryOptions, useMutation } from "@tanstack/react-query";

export const scriptQuery = (pid?: string) => (id: string) =>
  queryOptions({
    queryKey: [pid, "scripts", id],
    queryFn: () => dremio.scripts.retrieve(id),
    retry: false,
    staleTime: 5_000,
  });

export const scriptsListQuery = (pid?: string) =>
  queryOptions({
    queryKey: [pid, "scripts", "list"],
    queryFn: async () => {
      const scripts = await Array.fromAsync(dremio.scripts.list().data());

      for (const script of scripts) {
        queryClient.setQueryData(["scripts", script.id], script);
      }

      return scripts;
    },
    staleTime: 60_000,
  });

export const useScriptSaveMutation = (
  pid: string | undefined,
  script: CommunityScript,
  {
    onSuccess,
    onError,
  }: { onSuccess?: () => void; onError?: (err: unknown) => void } = {},
) =>
  useMutation({
    mutationKey: ["script", script.id],
    mutationFn: (properties: Parameters<typeof script.save>[0]) =>
      script.save(properties).then((result) => {
        if (result.isErr()) {
          throw result.error;
        }
        return result;
      }),
    onSuccess: (data) => {
      queryClient.setQueryData([pid, "scripts", script.id], data);
      onSuccess?.();
    },
    onError,
  });
