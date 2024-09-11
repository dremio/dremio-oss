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
import { Component, createRef } from "react";
import PropTypes from "prop-types";
import Keys from "@app/constants/Keys.json";
import classNames from "clsx";
import { base, searchInput } from "./SearchField.less";
import { IconButton, Spinner } from "dremio-ui-lib/components";

class SearchField extends Component {
  static propTypes = {
    placeholder: PropTypes.string,
    onChange: PropTypes.func,
    value: PropTypes.string,
    style: PropTypes.object,
    inputStyle: PropTypes.object,
    showCloseIcon: PropTypes.bool,
    closeIconTheme: PropTypes.object,
    inputClassName: PropTypes.string,
    dataQa: PropTypes.string,
    className: PropTypes.string,
    onClick: PropTypes.func,
    onBlur: PropTypes.func,
    showIcon: PropTypes.bool,
    disabled: PropTypes.bool,
    loading: PropTypes.bool,
    autoFocus: PropTypes.bool,
  };

  static defaultProps = {
    showIcon: false,
    inputStyle: {},
  };

  constructor(props) {
    super(props);
    this.inputRef = createRef();
    this.state = {
      value: props.value,
    };
  }

  onChange = (value) => {
    this.setState({
      value,
    });
    this.props.onChange(value);
  };

  handleKeyDown = (evt) => {
    if (evt.keyCode === Keys.ESCAPE) {
      this.clearFilter();
    }
  };

  clearFilter = () => {
    this.onChange("");
    this.focus();
  };

  focus() {
    this.inputRef.current?.focus();
  }

  render() {
    const { disabled } = this.props;
    const val = this.props.value != null ? this.props.value : this.state.value;
    const showCloseIcon = this.props.showCloseIcon && this.state.value;
    return (
      <div
        className={classNames(["field", base, this.props.className], {
          disabled,
        })}
        style={{ ...this.props.style, position: "relative" }}
      >
        {this.props.showIcon && (
          <div className="position-absolute ml-1">
            {this.props.loading ? (
              <Spinner />
            ) : (
              <dremio-icon name="interface/search" />
            )}
          </div>
        )}
        <input
          disabled={disabled}
          data-qa={this.props.dataQa}
          className={classNames([this.props.inputClassName || "", searchInput])}
          type="text"
          ref={this.inputRef}
          placeholder={this.props.placeholder}
          style={{
            ...this.props.inputStyle,
            ...(this.props.showIcon && { paddingLeft: 32 }),
            ...(this.props.showCloseIcon && { paddingRight: 24 }),
          }}
          value={val}
          onChange={(e) => this.onChange(e.target.value)}
          onKeyDown={this.handleKeyDown}
          onClick={this.props.onClick}
          onBlur={this.props.onBlur}
          autoFocus={this.props.autoFocus}
        />
        {showCloseIcon && (
          <div
            style={{
              position: "absolute",
              right: "var(--dremio--spacing--2)",
              top: 0,
            }}
          >
            <IconButton onClick={this.clearFilter} aria-label="Clear">
              <dremio-icon name="interface/close-small"></dremio-icon>
            </IconButton>
          </div>
        )}
      </div>
    );
  }
}

export default SearchField;
