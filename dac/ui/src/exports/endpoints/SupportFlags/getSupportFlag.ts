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
import moize from "moize";
import localStorageUtils from "@inject/utils/storageUtils/localStorageUtils";
import { type SupportFlagResponse } from "./SupportFlagResponse.type";
import { APIV2Call } from "#oss/core/APICall";
import { useEffect, useState } from "react";
import sentryUtil from "#oss/utils/sentryUtil";
import { store } from "#oss/store/store";
import { fetchSupportFlagsSuccess } from "#oss/actions/supportFlags";

export const getSupportFlagUrl = (supportKey: string) =>
  new APIV2Call().paths(`settings/${supportKey}`).toString();

export const getSupportFlag = moize(
  <T = any>(flagId: string): Promise<SupportFlagResponse<T>> => {
    const token = localStorageUtils!.getAuthToken();
    return fetch(getSupportFlagUrl(flagId), {
      headers: {
        ...(token && { Authorization: token }),
      },
    })
      .then((res) => res.json() as unknown as SupportFlagResponse<T>)
      .then((payload) => {
        store.dispatch(fetchSupportFlagsSuccess(payload));
        return payload;
      })
      .catch(() => false);
  },
  { isPromise: true, maxSize: Infinity },
);

export const clearCachedSupportFlag = (id: string) => {
  getSupportFlag.remove([id]);
};

export const clearCachedSupportFlags = () => {
  getSupportFlag.clear();
};

export const useSupportFlag = <T = boolean>(flag: string) => {
  const [value, setValue] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        const res = await getSupportFlag<T>(flag);
        setValue(res.value);
      } catch (e) {
        sentryUtil.logException(e);
      } finally {
        setLoading(false);
      }
    })();
  }, [flag]);

  return [value, loading] as const;
};
