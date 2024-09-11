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

// @ts-ignore
import { Link } from "react-router";
// @ts-ignore
import { Tooltip } from "dremio-ui-lib/components";
import React from "react";
import { intl } from "@app/utils/intl";

type TopActionProps = {
  active?: string;
  url: string;
  icon: string;
  alt: string;
  dataqa?: string;
  logo?: boolean;
  socketIsOpen?: boolean;
  tooltipProps?: Record<string, unknown>;
  tooltip?: boolean;
  className?: string;
  iconClassName?: string;
};

export const TopAction = (props: TopActionProps) => {
  const {
    active = "",
    url,
    icon,
    alt,
    dataqa = "data-qa",
    logo = false,
    socketIsOpen = true,
    tooltipProps = {},
    tooltip = true,
    className = "",
    iconClassName = "",
  } = props;

  const id = React.useId();
  const shouldHover = logo ? "" : "item__hover";
  const isSocketOpen = socketIsOpen ? "" : "socket__notOpen";
  const renderIcon = () => {
    return (
      <div className={logo ? "sideNav-item__logo" : "sideNav-item__icon"}>
        <dremio-icon
          name={icon}
          alt={alt}
          data-qa={icon}
          class={iconClassName}
        />
      </div>
    );
  };

  const renderLink = () => {
    return (
      <Link
        className="sideNav-item-a"
        to={url}
        data-qa={dataqa}
        aria-labelledby={id}
      >
        <div className={`sideNav-item__link ${active}`}>{renderIcon()}</div>
      </Link>
    );
  };

  return (
    <div className={`${isSocketOpen} sideNav-item ${shouldHover} ${className}`}>
      {tooltip ? (
        <Tooltip
          {...tooltipProps}
          content={intl.formatMessage({ id: alt })}
          id={id}
        >
          <div>{renderLink()}</div>
        </Tooltip>
      ) : (
        renderLink()
      )}
    </div>
  );
};
