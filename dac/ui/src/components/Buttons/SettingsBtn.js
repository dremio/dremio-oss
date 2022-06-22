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
import React, { createRef, PureComponent } from "react";
import classNames from "classnames";
import PropTypes from "prop-types";
import Popover from "@material-ui/core/Popover";
import Radium from "radium";

import FontIcon from "../Icon/FontIcon";
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
    style: PropTypes.object,
    stopPropagation: PropTypes.bool,
    disabled: PropTypes.bool,
  };

  static defaultProps = {
    hasDropdown: true,
    children: <FontIcon type="Settings" />,
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
      style,
      hideArrowIcon,
      children,
      position,
      disabled,
    } = this.props;
    const wrapClasses = classNames(classStr, { active: this.state.open });
    return (
      <span className={wrapClasses} ref={this.settingsWrapRef}>
        <button
          className="settings-button"
          data-qa={this.props.dataQa || "settings-button"}
          onClick={this.handleTouchTap}
          style={[styles.button, style]}
        >
          {children}
          {hasDropdown && !disabled && !hideArrowIcon && (
            <FontIcon
              type="Arrow-Down-Small"
              theme={{ Icon: { width: 12, backgroundPosition: "-7px 2px" } }}
            />
          )}
        </button>
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
          >
            {React.cloneElement(this.props.menu, {
              closeMenu: this.handleRequestClose,
            })}
          </Popover>
        )}
      </span>
    );
  }
}

const styles = {
  popover: {
    width: "",
    margin: 0,
  },
  button: {
    display: "flex",
  },
};
export default Radium(SettingsBtn);
