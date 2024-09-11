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
import * as React from "react";
import { IconButton } from "./IconButton";
import { useScrollObserver } from "./utilities/useScrollObserver";
import { useMutationObserver } from "./utilities/useMutationObserver";
import {
  elementIsScrolledToLeftEnd,
  elementIsScrolledToRightEnd,
} from "./utilities/elementIsScrolled";
import { Tooltip } from "./Tooltip/Tooltip";
import { Popover } from "./Popover";

type TabListWrapperProps = {
  children: React.ReactNode;
  tabControls?: React.ReactNode;
};

const useScrollMonitor = (ref: React.MutableRefObject<HTMLElement>) => {
  const [isScrolledToLeft, setIsScrolledToLeft] = React.useState(true);
  const [isScrolledToRight, setIsScrolledToRight] = React.useState(true);
  useMutationObserver(ref, () => {
    setIsScrolledToLeft(elementIsScrolledToLeftEnd(ref.current));
    setIsScrolledToRight(elementIsScrolledToRightEnd(ref.current));
  });
  useScrollObserver(ref, () => {
    setIsScrolledToLeft(elementIsScrolledToLeftEnd(ref.current));
    setIsScrolledToRight(elementIsScrolledToRightEnd(ref.current));
  });

  return { isScrolledToLeft, isScrolledToRight };
};

export const TabListWrapper = (props: TabListWrapperProps) => {
  const tablistWrapperRef = React.useRef(null);
  const { isScrolledToLeft, isScrolledToRight } =
    useScrollMonitor(tablistWrapperRef);

  return (
    <div className="tab-list">
      {!isScrolledToLeft && (
        <IconButton
          tooltip="Scroll left"
          onClick={() => {
            tablistWrapperRef.current.scroll({
              left:
                tablistWrapperRef.current.scrollLeft -
                tablistWrapperRef.current.scrollWidth,

              behavior: "smooth",
            });
          }}
        >
          <dremio-icon
            name="interface/right-chevron"
            style={{ transform: "rotate(180deg)" }}
            alt=""
          ></dremio-icon>
        </IconButton>
      )}
      <div className="tab-list-tabs" role="tablist" ref={tablistWrapperRef}>
        {props.children}
      </div>
      {!isScrolledToRight && (
        <IconButton
          tooltip="Scroll right"
          onClick={() => {
            tablistWrapperRef.current.scroll({
              left:
                tablistWrapperRef.current.scrollLeft +
                tablistWrapperRef.current.scrollWidth,
              behavior: "smooth",
            });
          }}
        >
          <dremio-icon name="interface/right-chevron" alt=""></dremio-icon>
        </IconButton>
      )}
      {props.tabControls}
    </div>
  );
};

type TabMenuItem = {
  id: string;
  label: JSX.Element | string;
  handler: () => void;
  disabled?: boolean;
};

type TabListTabProps = {
  "aria-controls": string;
  "aria-selected": boolean;
  className?: string;
  children: JSX.Element[] | JSX.Element | string;
  id: string;
  disabledCloseTooltip: string;
  onClose?: (() => void) | null;
  onSelected: () => void;
  getMenuItems?: () => TabMenuItem[];
  isUnsaved?: boolean;
  unsavedLabel?: string | JSX.Element;
};

const renderMenuItems = (
  menuItems: TabMenuItem[],
  close: () => void,
): JSX.Element => {
  return (
    <ul
      className="float-container float-container-enter-done listbox no-select"
      style={{ cursor: "pointer", minInlineSize: "20ch" }}
    >
      {menuItems.map((menuItem) => {
        return (
          <li
            aria-disabled={menuItem.disabled}
            key={menuItem.id}
            className="listbox-item px-2"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              if (!menuItem.disabled) {
                menuItem.handler();
                close();
              }
            }}
          >
            {menuItem.label}
          </li>
        );
      })}
    </ul>
  );
};

export const TabListTab = (props: TabListTabProps) => {
  const { getMenuItems, onClose, onSelected, disabledCloseTooltip, ...rest } =
    props;
  const elRef = React.useRef<HTMLButtonElement>();
  const selected = props["aria-selected"];
  React.useLayoutEffect(() => {
    if (!selected || !elRef.current) {
      return;
    }

    elRef.current.scrollIntoView({ smooth: "true" });
  }, [selected]);

  const unsavedBubble = (
    <div
      style={{
        width: "6px",
        height: "6px",
        borderRadius: "9999px",
        background: "var(--fill--brand--bold)",
        flexShrink: 0,
      }}
    />
  );
  return (
    <button
      {...rest}
      ref={elRef}
      className="tab-list-tab"
      role="tab"
      type="button"
      onClick={(e) => {
        e.stopPropagation();
        onSelected();
      }}
    >
      {props.isUnsaved &&
        (props.unsavedLabel ? (
          <Tooltip content={props.unsavedLabel} portal>
            {unsavedBubble}
          </Tooltip>
        ) : (
          unsavedBubble
        ))}
      <Tooltip content={<span>{props.children}</span>} delay={750} portal>
        <span className="tab-list-tab__label">{props.children}</span>
      </Tooltip>
      {getMenuItems ? (
        <div
          className="tab-list__tab-menu"
          onClick={(e) => {
            e.stopPropagation();
          }}
        >
          <Popover
            dismissable
            mode="click"
            content={({ close }) => renderMenuItems(getMenuItems(), close)}
            placement="bottom"
            portal
          >
            <IconButton
              aria-label="Script options"
              style={{ marginRight: "-8px" }}
            >
              <dremio-icon
                name="interface/ellipses"
                alt=""
                style={{ width: "20px", height: "20px" }}
              ></dremio-icon>
            </IconButton>
          </Popover>
        </div>
      ) : null}
      <IconButton
        disabled={!onClose}
        tooltip={!onClose && disabledCloseTooltip}
        tooltipPortal
        tooltipPlacement="top"
        aria-label={onClose && "Close"}
        className="tab-list-tab__close-icon"
        onClick={(e) => {
          e.stopPropagation();
          onClose?.();
        }}
      >
        <dremio-icon name="interface/close-small" alt=""></dremio-icon>
      </IconButton>
    </button>
  );
};
