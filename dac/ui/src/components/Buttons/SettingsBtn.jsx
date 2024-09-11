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
import { cloneElement, createRef, PureComponent } from "react";
import classNames from "clsx";
import PropTypes from "prop-types";
import Popover from "@mui/material/Popover";
import { IconButton } from "dremio-ui-lib/components";

import "./SettingsBtn.less";

class SettingsBtn extends PureComponent {
  static propTypes = {
    classStr: PropTypes.string,
    dataQa: PropTypes.string,
    menu: PropTypes.object.isRequired,
    handleSettingsClose: PropTypes.func,
    handleSettingsOpen: PropTypes.func,
    hasDropdown: PropTypes.bool,
    position: PropTypes.any,
    hideArrowIcon: PropTypes.bool,
    children: PropTypes.node,
    stopPropagation: PropTypes.bool,
    disabled: PropTypes.bool,
    tooltip: PropTypes.string,
    "aria-label": PropTypes.string,
    disablePortal: PropTypes.bool,
  };

  static defaultProps = {
    hasDropdown: true,
    children: <dremio-icon name="interface/settings" class="settings-icon" />,
    classStr: "main-settings-btn min-btn",
  };

  constructor(props) {
    super(props);
    this.handleRequestClose = this.handleRequestClose.bind(this);
    this.handleTouchTap = this.handleTouchTap.bind(this);
    this.settingsWrapRef = createRef();
    this.state = {
      open: false,
      subDropRight: true,
    };
  }

  handleRequestClose(event) {
    this.props.stopPropagation && event && event.stopPropagation();
    if (this.props.handleSettingsClose) {
      this.props.handleSettingsClose(this.settingsWrapRef.current);
    }
    this.setState({
      open: false,
    });
  }

  handleTouchTap(event) {
    event.preventDefault();
    this.props.stopPropagation && event.stopPropagation();
    if (this.props.handleSettingsOpen) {
      this.props.handleSettingsOpen(this.settingsWrapRef.current);
    }
    this.setState({
      open: true,
      anchorEl: event.currentTarget,
    });
  }

  render() {
    const {
      hasDropdown,
      classStr,
      hideArrowIcon,
      children,
      position,
      disabled,
      tooltip,
      disablePortal,
    } = this.props;
    const wrapClasses = classNames(classStr, { active: this.state.open });

    return (
      <span className={wrapClasses} ref={this.settingsWrapRef}>
        <IconButton
          className="settings-button"
          data-qa={this.props.dataQa || "settings-button"}
          onClick={this.handleTouchTap}
          tooltip={tooltip ?? "More"}
          tooltipPortal
        >
          {children}
          {hasDropdown && !disabled && !hideArrowIcon && (
            <dremio-icon
              style={{
                blockSize: 20,
                inlineSize: 20,
              }}
              name="interface/caretDown"
            ></dremio-icon>
          )}
        </IconButton>

        {hasDropdown && !disabled && (
          <Popover
            open={this.state.open}
            anchorEl={this.state.anchorEl}
            anchorOrigin={{
              horizontal: position ? position : "left",
              vertical: "bottom",
            }}
            transformOrigin={{
              horizontal: position ? position : "left",
              vertical: "top",
            }}
            onClose={this.handleRequestClose}
            container={
              disablePortal ? this.settingsWrapRef.current : document.body
            }
            className="settings-popover"
          >
            {cloneElement(this.props.menu, {
              closeMenu: this.handleRequestClose,
            })}
          </Popover>
        )}
      </span>
    );
  }
}

export default SettingsBtn;
