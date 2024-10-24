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
import { PureComponent } from "react";
import classnames from "clsx";
import PropTypes from "prop-types";
import { Tooltip } from "dremio-ui-lib";

import { formDefault } from "uiTheme/radium/typography";
import DefaultMenuItem from "#oss/components/Menus/MenuItem";

import "./MenuItem.less";

export default class MenuItem extends PureComponent {
  static propTypes = {
    className: PropTypes.string,
    children: PropTypes.node,
    onClick: PropTypes.func,
    href: PropTypes.string,
    title: PropTypes.string,
    disabled: PropTypes.bool,
    tooltipPlacement: PropTypes.string,
    showTooltip: PropTypes.bool,
  };

  renderDiv = (className, style) => {
    const { onClick, children, disabled, title, showTooltip } = this.props;
    return (
      <div
        className={classnames("dropdown-menu-item", className, {
          "--disabled": disabled,
        })}
        title={!showTooltip && title}
        onClick={disabled ? undefined : onClick}
      >
        <DefaultMenuItem disabled={disabled} style={style}>
          {children}
        </DefaultMenuItem>
      </div>
    );
  };

  renderLink = (className, style) => {
    const { href, children, onClick, disabled, title, showTooltip } =
      this.props;
    return (
      <a
        href={href}
        title={!showTooltip && title}
        onClick={onClick}
        className={classnames("menu-item-link", className, {
          "--disabled": disabled,
        })}
      >
        <DefaultMenuItem disabled={disabled} style={style}>
          {children}
        </DefaultMenuItem>
      </a>
    );
  };

  render() {
    const { className, href, disabled, title, tooltipPlacement, showTooltip } =
      this.props;
    const style = { ...styles.base, ...(disabled && styles.disabled) };
    return showTooltip ? (
      <Tooltip
        placement={tooltipPlacement || "left"}
        title={title}
        enterDelay={500}
        enterNextDelay={500}
      >
        {href && !disabled
          ? this.renderLink(className, style)
          : this.renderDiv(className, style)}
      </Tooltip>
    ) : href && !disabled ? (
      this.renderLink(className, style)
    ) : (
      this.renderDiv(className, style)
    );
  }
}

const styles = {
  base: {
    display: "flex",
    alignItems: "center",
    paddingLeft: 8,
    paddingRight: 8,
    cursor: "pointer",
    height: 32,
    ...formDefault,
    fontSize: 14,
  },
  disabled: {
    cursor: "default",
    color: "#DDDDDD",
  },
};
