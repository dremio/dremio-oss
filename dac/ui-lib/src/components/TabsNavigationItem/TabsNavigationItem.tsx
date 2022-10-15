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
import * as React from "react";

import "./TabsNavigationItem.scss";

type TabsNavigationItemProps = {
  name: string;
  activeTab?: string;
  onClick?: () => void;
  style?: any;
  children?: any;
  className?: string;
};

const TabsNavigationItem = (props: TabsNavigationItemProps) => {
  const {
    activeTab,
    style: styleParam = {},
    onClick,
    name: tabName,
    children,
    className,
  } = props;
  const key = `tab-${tabName}`;
  const isActive = activeTab === tabName ? "-active" : "";
  const tabClasses = `tab-link tab ${isActive}`;

  return (
    <div
      className={clsx(tabClasses, className)}
      style={styleParam}
      key={key}
      onClick={onClick}
      data-qa={key}
    >
      {children}
    </div>
  );
};

export default TabsNavigationItem;
