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

export const useHasClipboardPermissions = (): boolean | null => {
  const [hasPermission, setHasPermission] = useState<boolean | null>(null);

  useEffect(() => {
    //@ts-ignore
    if (!navigator.permissions) {
      setHasPermission(true);
    } else {
      navigator.permissions
        //@ts-ignore //Works in Chrome
        .query({ name: "clipboard-write" })
        .then((result) => {
          if (result.state == "granted" || result.state == "prompt") {
            setHasPermission(true);
          } else {
            setHasPermission(false);
          }
        })
        .catch((e) => {
          setHasPermission(true);
        });
    }
  }, []);

  return hasPermission;
};
