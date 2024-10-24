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
import { createPortal } from "react-dom";
import { CSSTransition } from "react-transition-group";

import {
  type Placement,
  useFloating,
  useInteractions,
  useHover,
  useFocus,
  useRole,
  flip,
  autoUpdate,
  offset,
  shift,
  arrow,
  safePolygon,
} from "@floating-ui/react";
import clsx from "clsx";
import mergeRefs from "react-merge-refs";

export type TooltipPlacement = Placement;

type TooltipProps = {
  /**
   * Render prop function allows you to customize where the tooltip content is rendered,
   * otherwise it defaults to a sibling of the hover target.
   */
  children: JSX.Element | ((tooltipContent: JSX.Element) => JSX.Element);
  content: JSX.Element | string;
  delay?: number;
  interactive?: boolean;
  placement?: Placement;
  style?: Record<string, any>;
  portal?: boolean;
  id?: string;
  /**
   *  Use for white tooltip
   */
  rich?: boolean;
  /**
   * Use when a disabled element does not show the tooltip
   */
  shouldWrapChildren?: boolean;
  /**
   * Called when the tooltip is closed
   */
  onClose?: () => void;

  /**
   * Called when the tooltip is opened
   */
  onOpen?: () => void;
};

export const Tooltip = (props: TooltipProps): JSX.Element => {
  const arrowElRef = React.useRef(null);
  const [open, setOpen] = React.useState(false);
  const {
    children,
    content,
    delay = 250,
    interactive = false,
    placement = "bottom",
    portal = false,
    shouldWrapChildren = false,
    onClose,
    onOpen,
    style,
    id,
    rich,
  } = props;

  const handleOpenChange = (isOpen: boolean) => {
    setOpen(isOpen);

    if (!isOpen) {
      onClose?.();
    }

    if (isOpen) {
      onOpen?.();
    }
  };

  const {
    x,
    y,
    context,
    refs,
    strategy,
    middlewareData,
    placement: calculatedPlacement,
  } = useFloating({
    middleware: [offset(8), flip(), shift(), arrow({ element: arrowElRef })],
    onOpenChange: handleOpenChange,
    open,
    placement,
    strategy: "absolute",
    whileElementsMounted: autoUpdate,
  });

  const { getReferenceProps, getFloatingProps } = useInteractions([
    useHover(context, {
      restMs: delay,
      handleClose: interactive
        ? safePolygon({
            buffer: 2,
          })
        : undefined,
    }),
    useFocus(context),
    useRole(context, { role: "tooltip" }),
  ]);

  const ref = React.useMemo(
    () => mergeRefs([refs.setReference, (children as any).ref]),
    [refs.setReference, children],
  );

  const staticSide = (
    {
      top: "bottom",
      right: "left",
      bottom: "top",
      left: "right",
    } as const
  )[calculatedPlacement.split("-")[0]];

  const tooltipContent = (
    <CSSTransition
      appear
      classNames="dremio-tooltip"
      in={open}
      timeout={10000}
      //@ts-ignore
      addEndListener={(node, done) =>
        node.addEventListener("transitionend", done, false) as any
      }
      mountOnEnter
      unmountOnExit
    >
      <div
        className={clsx(
          "dremio-tooltip",
          `dremio-tooltip--${staticSide}`,
          rich && "dremio-tooltip-rich",
        )}
        {...getFloatingProps({
          ref: refs.setFloating,
          style: {
            position: strategy,
            top: y ?? 0,
            left: x ?? 0,
            ...(style || {}),
          },
        })}
        id={id || ""}
      >
        {content}
        <div
          className="dremio-tooltip__arrow"
          ref={arrowElRef}
          style={{
            left: middlewareData.arrow?.x,
            top: middlewareData.arrow?.y,
            [staticSide as any]:
              "calc(var(--dremio-tooltip--arrow--size) * -0.5)",
          }}
        />
      </div>
    </CSSTransition>
  );

  if (typeof children === "function") {
    const childrenResult = children(tooltipContent);
    return React.cloneElement(
      children(tooltipContent),
      getReferenceProps({ ref, ...childrenResult.props }),
    );
  }

  return (
    <>
      {shouldWrapChildren ? (
        <span
          className="dremio-tooltip__child-wrap"
          {...getReferenceProps({ ref })}
        >
          {React.cloneElement(children, { ...children.props })}
        </span>
      ) : (
        React.cloneElement(
          children,
          getReferenceProps({ ref, ...children.props }),
        )
      )}
      {portal ? createPortal(tooltipContent, document.body!) : tooltipContent}
    </>
  );
};
