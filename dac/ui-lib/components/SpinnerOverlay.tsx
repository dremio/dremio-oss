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
import { CSSTransition } from "react-transition-group";
import clsx from "clsx";
import { Spinner } from "./Spinner/Spinner";

type SpinnerOverlayProps = {
  className?: string;
  in: boolean;
};

export const SpinnerOverlay = (props: SpinnerOverlayProps): JSX.Element => {
  return (
    <CSSTransition
      addEndListener={(node, done) => {
        node.addEventListener("transitionend", done, false);
      }}
      appear
      classNames="dremio-spinner-overlay"
      in={props.in}
      mountOnEnter
      unmountOnExit
    >
      <div
        aria-busy="true"
        aria-live="polite"
        className={clsx("dremio-spinner-overlay", props.className)}
      >
        <Spinner />
      </div>
    </CSSTransition>
  );
};
