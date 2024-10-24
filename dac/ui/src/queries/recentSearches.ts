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

import { queryClient } from "#oss/queryClient";
import { queryOptions, useMutation } from "@tanstack/react-query";

type RecentSearchStorage = {
  [pid: string]: string[];
};

const STORAGE_KEY = "rs";
const DEFAULT_PID = "DEFAULT";

const getStorage = (): RecentSearchStorage => {
  try {
    const storage = JSON.parse(
      window.atob(window.localStorage.getItem(STORAGE_KEY)!),
    );
    if (typeof storage !== "object" || Array.isArray(storage)) {
      throw new Error("Recent storage did not parse into the expected format");
    }
    return storage;
  } catch {
    setStorage({});
    return {};
  }
};

const setStorage = (recentSearches: RecentSearchStorage): void => {
  window.localStorage.setItem(
    STORAGE_KEY,
    window.btoa(JSON.stringify(recentSearches)),
  );
};

const getPidStorage = (pid: string = DEFAULT_PID) => getStorage()[pid] || [];

const setPidStorage = (pid: string = DEFAULT_PID, val: string[]) => {
  const storage = getStorage();
  storage[pid] = val;
  setStorage(storage);
};

export const addRecentSearch = (
  pid: string = DEFAULT_PID,
  val: string,
): string[] => {
  const data = getPidStorage(pid);
  // Don't add duplicates, including duplicates that differ only in casing
  const prev = data.filter((v) => v.toLowerCase() !== val.toLowerCase());

  // Don't allow the list to grow larger than 10 items
  const next = [val, ...prev].slice(0, 10);

  setPidStorage(pid, next);

  return next;
};

export const recentSearches = (pid: string = DEFAULT_PID) =>
  queryOptions({
    queryKey: [pid, "recent-searches"],
    queryFn: () => getPidStorage(pid),
    retry: false,
  });

export const useAddRecentSearchMutation = (pid: string = DEFAULT_PID) =>
  useMutation({
    mutationFn: ({ search }: { search: string }) =>
      Promise.resolve(addRecentSearch(pid, search)),
    onSuccess: (recentSearches) => {
      queryClient.setQueryData([pid, "recent-searches"], recentSearches);
    },
  });
