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
import * as React from "react";
import clsx from "clsx";
import { forwardRef } from "react";
import { useDetectScroll } from "../utilities/useDetectScroll";

type PageProps = {
  children?: JSX.Element | JSX.Element[];
  className?: string;
  header?: JSX.Element;
  footer?: JSX.Element;
};

export const Page = forwardRef<HTMLDivElement, PageProps>((props, ref) => {
  const { children, className, footer, header, ...rest } = props;
  const { scrolledDirections, scrollContainerRef } = useDetectScroll([
    "top",
    "bottom",
  ]);
  return (
    <div ref={ref} {...rest} className={clsx("dremio-page", className)}>
      {header && (
        <header
          className={clsx(
            "dremio-page__header dremio-scroll-shadow dremio-scroll-shadow--top",
            {
              "--scrolled": scrolledDirections.has("top"),
            }
          )}
        >
          {header}
        </header>
      )}
      <div className="dremio-page__content" ref={scrollContainerRef}>
        {children}
      </div>
      {footer && (
        <footer
          className={clsx(
            "dremio-page__footer",
            "dremio-scroll-shadow dremio-scroll-shadow--bottom",
            {
              "--scrolled": scrolledDirections.has("bottom"),
            }
          )}
        >
          {footer}
        </footer>
      )}
    </div>
  );
});
