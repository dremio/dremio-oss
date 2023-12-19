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

//@ts-nocheck
import React from "react";
import clsx from "clsx";
import {
  useFloating,
  flip,
  autoUpdate,
  size,
  offset,
} from "@floating-ui/react-dom";
import { CSSTransition } from "react-transition-group";
import { cloneElement } from "react";
import mergeRefs from "react-merge-refs";

export const FloatingContainer = (props: any) => {
  const { trigger, children, isOpen, placement } = props;
  const { x, y, strategy, refs } = useFloating({
    middleware: [
      offset(4),
      flip(),
      size({
        apply({ rects, elements }) {
          Object.assign(elements.floating.style, {
            width: `${rects.reference.width}px`,
          });
        },
      }),
    ],
    placement,
    strategy: "fixed",
    whileElementsMounted: autoUpdate,
  });

  return (
    <>
      {cloneElement(trigger, {
        ref: mergeRefs([trigger.ref, refs.setReference]),
      })}
      <CSSTransition
        in={isOpen}
        nodeRef={refs.floating}
        classNames="float-container"
        addEndListener={(done) =>
          refs.floating.current.addEventListener("transitionend", done, false)
        }
      >
        {cloneElement(children, {
          className: clsx("float-container", children.props.className),
          ref: mergeRefs([children.ref, refs.setFloating]),
          style: {
            ...children.props.style,
            ...(!isOpen && { display: "none" }),
            position: strategy,
            top: y ?? 0,
            left: x ?? 0,
          },
        })}
      </CSSTransition>
    </>
  );
};
