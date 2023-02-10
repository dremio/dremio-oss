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
import { Component } from "react";
import PropTypes from "prop-types";
import invariant from "invariant";
import clsx from "clsx";

import FontIcon from "components/Icon/FontIcon";

import * as buttonStyles from "uiTheme/radium/buttons";
import * as classes from "@app/uiTheme/radium/replacingRadiumPseudoClasses.module.less";

class SimpleButton extends Component {
  static propTypes = {
    onClick: PropTypes.func,
    disabled: PropTypes.bool,
    submitting: PropTypes.bool,
    buttonStyle: PropTypes.string,
    style: PropTypes.object,
    children: PropTypes.node,
    className: PropTypes.string,
    showSpinnerAndText: PropTypes.bool,
  };

  static defaultProps = {
    disabled: false,
    submitting: false,
    buttonStyle: "secondary",
  };

  renderSpinner() {
    const { buttonStyle, disabled, submitting } = this.props;
    if (!submitting) {
      return null;
    }
    let loaderType = "LoaderWhite spinner";
    if (disabled || submitting || buttonStyle !== "primary") {
      loaderType = "Loader spinner";
    }
    return <FontIcon type={loaderType} theme={styles.spinner} />;
  }

  render() {
    const {
      className,
      buttonStyle,
      disabled,
      submitting,
      style,
      children,
      showSpinnerAndText = true,
      ...props
    } = this.props;
    invariant(
      buttonStyles[buttonStyle] !== undefined,
      `Unknown button style: "${buttonStyle}"`
    );

    const combinedStyle = {
      ...styles.base,
      ...buttonStyles[buttonStyle],
      ...(disabled ? buttonStyles.disabled : {}),
      ...(submitting ? buttonStyles.submitting[buttonStyle] : {}),
      ...(style || {}),
    };

    return (
      <button
        className={clsx(className, {
          [classes[`${buttonStyle}ButtonPsuedoClasses`]]:
            !submitting && !disabled,
          [classes["buttonPsuedoClasses"]]: !submitting && !disabled,
        })}
        disabled={submitting || disabled}
        {...props}
        // DX-34369: need to fix how we use classname and style
        style={combinedStyle}
      >
        {showSpinnerAndText ? (
          <div className="flex --alignCenter">
            {this.renderSpinner()}
            {children}
          </div>
        ) : (
          this.renderSpinner() || children
        )}
      </button>
    );
  }
}

const styles = {
  base: {
    lineHeight: "27px",
    textDecoration: "none",
  },
  spinner: {
    Container: {
      display: "block",
      height: 24,
    },
  },
};
export default SimpleButton;
