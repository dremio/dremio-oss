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
import PropTypes from "prop-types";
import clsx from "clsx";

import { Tooltip } from "@mui/material";

export default class CopyButtonIcon extends PureComponent {
  static propTypes = {
    title: PropTypes.string,
    style: PropTypes.object,
    buttonStyle: PropTypes.object,
    onClick: PropTypes.func,
    disabled: PropTypes.bool,
    isLoading: PropTypes.bool,
    className: PropTypes.string,
  };

  render() {
    const {
      title,
      style,
      buttonStyle,
      onClick,
      disabled,
      isLoading,
      className,
    } = this.props;

    const clickHandler = disabled ? undefined : onClick;

    // Directly use Tooltip instead of IconButton to work around bug where tooltip is overflowing the page content
    return (
      <span
        onMouseOver={(e) => e.stopPropagation()}
        style={{ ...styles.wrap, ...(disabled && styles.disabled), ...style }}
        className={className}
      >
        <Tooltip
          title={title}
          placement="bottom-start"
          arrow
          enterDelay={500}
          enterNextDelay={500}
        >
          <button
            onClick={clickHandler}
            disabled={disabled}
            className={clsx("dremio-icon-button", "copy-button")}
            data-qa="copy-icon"
            style={buttonStyle}
          >
            <dremio-icon
              name={`${isLoading ? "job-state/loading" : "interface/copy"}`}
              class={`copy-button__icon ${isLoading && "spinner"}`}
            />
          </button>
        </Tooltip>
      </span>
    );
  }
}

const styles = {
  wrap: {
    display: "inline-block",
    color: "var(--icon--primary)",
  },
  disabled: {
    cursor: "not-allowed",
  },
};
