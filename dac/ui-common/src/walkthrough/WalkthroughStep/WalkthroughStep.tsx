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
import { useRef, type ReactNode } from "react";
import { useOutletId, useTutorialController } from "../TutorialController";
import { Button } from "dremio-ui-lib/components";
import {
  type Placement,
  useFloating,
  arrow,
  autoUpdate,
  shift,
  offset,
} from "@floating-ui/react-dom";
import clsx from "clsx";

type WalkthroughStepProps = {
  title: string;
  description?: string | ReactNode;
  steps?: ReactNode[];
  postSteps?: ReactNode | ReactNode[];
  actions?: ReactNode | ReactNode[];
  anchor?: () => Element | null;
  showRipple?: boolean;
  placement?: Placement;
  outletId?: string;
};
const RIPPLE_SIZE = 40;
const RIPPLE_PADDING = 8;

const defaultAnchor = () =>
  document.querySelector('[aria-label="User options"]') as HTMLElement;

const sides = {
  top: "bottom",
  right: "left",
  bottom: "top",
  left: "right",
} as const;

const getPlacementProperty = (
  placement: Placement,
): (typeof sides)[keyof typeof sides] => {
  return (sides as any)[placement.split("-")[0] as any];
};

const isDefaultAnchor = (anchorEl: HTMLElement): boolean =>
  anchorEl === defaultAnchor();

const getAnchorElement = (props: WalkthroughStepProps): HTMLElement => {
  if (props.anchor) {
    try {
      const el = props.anchor() as HTMLElement;
      // Ensure that the anchor element doesn't have a hidden parent
      if (el && el.offsetParent != null) {
        return el;
      }
    } catch (e) {
      //
    }
  }

  return defaultAnchor();
};

const rippleShouldRender = (props: WalkthroughStepProps): boolean => {
  // If showRipple wasn't set, then default it to true
  // Otherwise respect the explicit value of showRipple
  return typeof props.showRipple === "undefined" || !!props.showRipple;
};

export const WalkthroughStep = (props: WalkthroughStepProps) => {
  const { endTutorial } = useTutorialController();
  const arrowRef = useRef(null);
  const { outletId = null } = props;
  const anchorEl = getAnchorElement(props);
  const isDefaultEl = isDefaultAnchor(anchorEl);
  const showRipple = !isDefaultEl && rippleShouldRender(props);
  const placement = isDefaultEl ? "right" : props.placement || "top";
  const { refs, floatingStyles, middlewareData } = useFloating({
    middleware: [
      shift(),
      offset(showRipple ? RIPPLE_SIZE + RIPPLE_PADDING * 2 : 0),
      arrow({
        element: arrowRef,
      }),
    ],
    placement,
    elements: {
      reference: anchorEl,
    },
    whileElementsMounted: autoUpdate,
  });
  useOutletId(outletId);
  const hasOneStep = props.steps?.length === 1;
  return (
    <div
      className={clsx({ "p-1": !showRipple })}
      ref={refs.setFloating}
      style={{ ...floatingStyles, zIndex: "5000" }}
    >
      {showRipple && (
        <svg
          ref={arrowRef}
          width={RIPPLE_SIZE}
          height={RIPPLE_SIZE}
          viewBox="0 0 40 40"
          fill="none"
          xmlns="http://www.w3.org/2000/svg"
          style={{
            position: "absolute",
            left:
              middlewareData.arrow?.x != null
                ? `${middlewareData.arrow!.x}px`
                : "",
            top:
              middlewareData.arrow?.y != null
                ? `${middlewareData.arrow!.y}px`
                : "",
            [getPlacementProperty(placement)]: `-${
              RIPPLE_SIZE + RIPPLE_PADDING
            }px`,
          }}
        >
          <circle opacity="0.2" cx="20" cy="20" r="19" fill="#167A89" />
          <circle opacity="0.5" cx="20" cy="20" r="13" fill="#167A89" />
          <circle cx="20" cy="20" r="7" fill="#167A89" />
        </svg>
      )}
      <div
        className="bg-brand-500 rounded p-2 leading-normal"
        style={{
          width: "45ch",
          fontSize: "13px",
        }}
      >
        <p className="text-semibold mb-05 text-base">{props.title}</p>
        {props.description && <p>{props.description}</p>}
        {props.steps && (
          <ol
            className="rounded px-105 py-1 my-2 dremio-layout-stack"
            style={{
              listStyle: "decimal",
              //@ts-ignore
              "--space": "var(--scale-05)",
              backgroundColor: "#167A89",
            }}
          >
            {props.steps.map((step, i) => (
              <li
                key={i}
                className={hasOneStep ? "" : "ml-2"}
                style={hasOneStep ? { listStyle: "none" } : {}}
              >
                {step}
              </li>
            ))}
          </ol>
        )}
        {props.postSteps && <div>{props.postSteps}</div>}
        <div className="flex flex-row items-center">
          <Button
            variant="tertiary"
            className="text-sm"
            onClick={endTutorial}
            style={{
              marginInlineStart: "calc(var(--scale-2) * -1)",
              color: "#16E3EE",
            }}
          >
            End tutorial
          </Button>
          <div className="dremio-button-group ml-auto">{props.actions}</div>
        </div>
      </div>
    </div>
  );
};
