/* eslint-disable react/prop-types */
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

/* eslint-disable react/prop-types */
import * as React from "react";
import clsx from "clsx";
import { createElement, useState } from "react";

export const getControlledTabProps = ({
  id,
  controls,
  currentTab,
}: {
  id: string;
  controls: string;
  currentTab: string;
}) => {
  const isSelected = currentTab === id;
  return {
    id,
    "aria-controls": controls,
    "aria-selected": isSelected,
    tabIndex: 0,
  };
};

export const getControlledTabPanelProps = ({
  id,
  labelledBy,
  currentTab,
}: {
  id: string;
  labelledBy: string;
  currentTab: string;
}) => {
  const isSelected = currentTab === labelledBy;
  return {
    id,
    "aria-labelledby": labelledBy,
    hidden: !isSelected,
  };
};

export const useTabList = (initialTab: string) => {
  const [tab, setTab] = useState<string>(initialTab);

  const getTabProps = ({ id, controls }: { id: string; controls: string }) => {
    return {
      ...getControlledTabProps({ id, controls, currentTab: tab }),
      onClick: () => {
        setTab(id);
      },
    };
  };

  const getTabPanelProps = ({
    id,
    labelledBy,
  }: {
    id: string;
    labelledBy: string;
  }) => {
    return getControlledTabPanelProps({ id, labelledBy, currentTab: tab });
  };

  return {
    tab,
    setTab,
    getTabProps,
    getTabPanelProps,
  };
};

export const TabPanel = (props: {
  children: React.ReactNode;
  hidden?: boolean;
  className?: string;
}) => {
  const { children, className, ...rest } = props;
  return (
    <div role="tabpanel" className={clsx("tabpanel", className)} {...rest}>
      {rest.hidden ? null : children}
    </div>
  );
};

export const TabList = (props: {
  children: React.ReactNode;
  "aria-label": string;
  className?: string;
}) => (
  <div
    {...props}
    role="tablist"
    className={clsx("tabpanel-tablist", props.className)}
  />
);

type TabProps = {
  as?: any;
  className?: string;
  children: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
};

export const Tab = (props: TabProps) => {
  const { as: asProp = "button", className = "", ...rest } = props;
  const defaultTypeProp = props.as === "button" && { type: "button" };

  return createElement(asProp, {
    ...defaultTypeProp,
    ...rest,
    className: clsx("tabpanel-tab", className),
    role: "tab",
  });
};
