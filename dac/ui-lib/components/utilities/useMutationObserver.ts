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
import { useEffect, type MutableRefObject, useRef } from "react";

export const useMutationObserver = (
  ref: MutableRefObject<Element>,
  cb: MutationCallback,
): void => {
  const cbRef = useRef(cb);
  cbRef.current = cb;
  useEffect(() => {
    const observer = new MutationObserver((...args) => cbRef.current(...args));

    observer.observe(ref.current, { subtree: true, childList: true });

    return () => {
      observer.disconnect();
    };
  }, [ref]);
};
