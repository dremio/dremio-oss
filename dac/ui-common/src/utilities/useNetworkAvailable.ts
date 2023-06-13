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

import { useEffect, useState } from "react";

export const isNetworkAvailable = () => window.navigator.onLine;

export const useNetworkAvailable = () => {
  const [networkAvailable, setNetworkAvailable] = useState(isNetworkAvailable);

  useEffect(() => {
    const handleOnline = () => {
      setNetworkAvailable(true);
    };

    const handleOffline = () => {
      setNetworkAvailable(false);
    };

    window.addEventListener("online", handleOnline);
    window.addEventListener("offline", handleOffline);

    return () => {
      window.removeEventListener("online", handleOnline);
      window.removeEventListener("offline", handleOffline);
    };
  }, []);

  return networkAvailable;
};

export const waitForNetworkAvailable = (): Promise<void> => {
  if (isNetworkAvailable()) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    window.addEventListener(
      "online",
      () => {
        resolve();
      },
      { once: true }
    );
  });
};
