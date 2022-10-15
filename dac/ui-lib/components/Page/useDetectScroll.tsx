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

import {
  useCallback,
  useState,
  useRef,
  useLayoutEffect,
  type UIEventHandler,
} from "react";

export const elementIsScrolled = (el: HTMLElement): boolean =>
  el.scrollTop !== 0;

export const useDetectScroll = (): any => {
  const [isScrolled, setIsScrolled] = useState(false);
  const scrollContainerRef = useRef<HTMLElement>(null);

  const onScroll = useCallback<UIEventHandler<HTMLElement>>((e): void => {
    setIsScrolled(elementIsScrolled(e.target as HTMLElement));
  }, []);

  useLayoutEffect(() => {
    if (!scrollContainerRef.current) {
      throw new Error("useDetectScroll: provided ref is null");
    }
    setIsScrolled(elementIsScrolled(scrollContainerRef.current));
  }, []);

  return {
    isScrolled,
    scrollContainerProps: {
      onScroll,
      ref: scrollContainerRef,
    },
  };
};
