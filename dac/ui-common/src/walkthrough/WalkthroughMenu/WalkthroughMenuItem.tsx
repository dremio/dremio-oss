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
import clsx from "clsx";
import { css } from "@emotion/css";
import { Button } from "dremio-ui-lib/components";
const lockedSvg = (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    fill="none"
    viewBox="0 0 28 28"
    style={{
      position: "absolute",
      inset: 0,
      margin: "auto",
      blockSize: "24px",
      inlineSize: "24px",
    }}
  >
    <path
      fill="#FF981B"
      stroke="#FF981B"
      strokeWidth="2"
      d="M14 27c7.18 0 13-5.82 13-13S21.18 1 14 1 1 6.82 1 14s5.82 13 13 13Z"
    />
    <path
      fill="#fff"
      d="M17.86 10.357h-7.72c-1.233 0-2.232 1-2.232 2.232v7.266c0 1.232 1 2.232 2.232 2.232h7.72c1.232 0 2.232-1 2.232-2.232v-7.266c0-1.232-1-2.232-2.232-2.232Z"
    />
    <path
      fill="#fff"
      d="M17.86 22.788h-7.72a2.935 2.935 0 0 1-2.932-2.933V12.59a2.935 2.935 0 0 1 2.932-2.932h7.72a2.935 2.935 0 0 1 2.932 2.932v7.265a2.935 2.935 0 0 1-2.932 2.933Zm-7.72-11.73c-.844 0-1.532.687-1.532 1.532v7.265c0 .845.688 1.532 1.532 1.532h7.72c.845 0 1.532-.687 1.532-1.532V12.59c0-.845-.687-1.532-1.532-1.532h-7.72Z"
    />
    <path
      fill="#fff"
      d="M17.593 11.057h-7.06l.001-2.341a3.538 3.538 0 0 1 3.529-3.503 3.533 3.533 0 0 1 3.53 3.53v2.314Zm-5.66-1.4h4.26v-.921a2.132 2.132 0 0 0-2.13-2.123c-1.174 0-2.13.955-2.13 2.13v.914Z"
    />
    <path
      fill="#FF981B"
      d="M15.43 13.812a1.367 1.367 0 1 0-2.125 1.138v1.92a.76.76 0 0 0 1.518 0v-1.922c.366-.245.607-.662.607-1.136Z"
    />
  </svg>
);

type WalkthroughMenuItemProps = {
  title: JSX.Element | string;
  description: JSX.Element | string;
  iconSrc: string;
  onSelect: () => void;
  disabled?: boolean;
  lockedMsg?: string;
};

export const WalkthroughMenuItem = (props: WalkthroughMenuItemProps) => {
  return (
    <li>
      <button
        disabled={props.disabled}
        className={clsx(
          "position-relative flex flex-row gap-1 text-left bg-none border-none rounded-md w-full leading-normal hover",
          {
            "color-neutral-300": props.disabled,
          },
          css`
            .walkthrough-menu-item__restart {
              visibility: hidden;
            }
            &:hover {
              .walkthrough-menu-item__restart {
                visibility: visible;
              }
            }
          `,
        )}
        style={
          props.disabled
            ? {
                color: "var(--color--neutral--300)",
              }
            : {}
        }
        onClick={props.onSelect}
      >
        <div className="position-relative">
          {!!props.lockedMsg && lockedSvg}
          <img src={props.iconSrc} className="h-7 w-7 self-center" />
        </div>
        <div className="flex flex-col py-05">
          {!!props.lockedMsg && (
            <p className="text-sm" style={{ color: "#FF981B" }}>
              {props.lockedMsg}
            </p>
          )}
          <span className="text-semibold">{props.title}</span>
          <span
            className={clsx("text-sm", { "color-faded ": !props.disabled })}
          >
            {props.description}
          </span>
        </div>
        {/*@ts-ignore*/}
        <dremio-icon
          name="interface/right-chevron"
          alt=""
          class="self-center ml-auto"
        >
          {/*@ts-ignore*/}
        </dremio-icon>
        {props.disabled && !props.lockedMsg && (
          <Button
            className="position-absolute walkthrough-menu-item__restart"
            style={{ bottom: "var(--scale-1)", right: "var(--scale-1)" }}
            variant="secondary"
            onClick={props.onSelect}
          >
            Restart tutorial
          </Button>
        )}
      </button>
    </li>
  );
};
