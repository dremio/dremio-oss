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

type Direction = "left" | "top" | "bottom";

export const elementIsScrolled = (
  direction: Direction,
  el: HTMLElement
): boolean => {
  switch (direction) {
    case "top":
      return el.scrollTop !== 0;
    case "left":
      return el.scrollLeft !== 0;
    case "bottom":
      return el.scrollHeight - el.scrollTop - el.clientHeight > 1;
    default:
      throw new Error(`Unknown direction ${direction}`);
  }
};

export const useDetectScroll = (directions: Direction[]): any => {
  const directionsRef = useRef(directions);
  directionsRef.current = directions;

  const [scrolledDirections, setScrolledDirections] = useState<Set<Direction>>(
    new Set()
  );
  const scrollContainerRef = useRef<HTMLElement>(null);

  const checkForScrolledDirectionsUpdates = useCallback(() => {
    let changed = false;

    setScrolledDirections((previous) => {
      const next = new Set<Direction>();

      directionsRef.current.forEach((direction) => {
        const inPrevious = previous.has(direction);
        if (elementIsScrolled(direction, scrollContainerRef.current!)) {
          next.add(direction);
          if (!inPrevious) {
            changed = true;
          }
        } else if (inPrevious) {
          changed = true;
        }
      });

      if (!changed) {
        return previous;
      }

      return next;
    });
  }, []);

  useLayoutEffect(() => {
    if (!scrollContainerRef.current) {
      throw new Error("useDetectScroll: provided ref is null");
    }

    const el = scrollContainerRef.current;

    const observer = new MutationObserver(() => {
      checkForScrolledDirectionsUpdates();
    });

    observer.observe(el, {
      childList: true,
      subtree: true,
    });

    el.addEventListener("scroll", checkForScrolledDirectionsUpdates, {
      passive: true,
    });

    return () => {
      observer.disconnect();
      el.removeEventListener("scroll", checkForScrolledDirectionsUpdates);
    };
  }, [checkForScrolledDirectionsUpdates]);

  return {
    scrolledDirections,
    scrollContainerRef,
  };
};
