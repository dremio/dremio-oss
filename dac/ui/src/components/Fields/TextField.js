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

import { IconButton } from "dremio-ui-lib";

import PropTypes from "prop-types";
import classNames from "clsx";
import {
  textInput,
  numberInput,
  numberInputDisabled,
  numberInputButtons,
  numberInputButtonIcon,
  numberInputWrapper,
  numberInputWrapperError,
  numberInputWrapperFocused,
  numberInputWrapperDisabled,
  inputWrapperAdvanced,
} from "@app/uiTheme/less/forms.less";

import forms from "uiTheme/radium/forms";
import { intl } from "@app/utils/intl";

import * as classes from "./TextField.module.less";

const initializeValue = (initialValue) => {
  // If the value is not passed or if it is an empty string,
  // assigning 0 as default value for number input fields
  if (typeof numberValue === "string") {
    initialValue = initialValue.trim();
  }
  return Number.parseInt(initialValue || 0);
};

class TextField extends Component {
  static propTypes = {
    initialFocus: PropTypes.bool,
    error: PropTypes.string,
    onChange: PropTypes.func,
    touched: PropTypes.bool,
    disabled: PropTypes.bool, // todo: add a #readonly/readOnly(?) and switch existing uses of #disabled as appropriate)
    default: PropTypes.string,
    type: PropTypes.string,
    style: PropTypes.object,
    className: PropTypes.string,
    placeholder: PropTypes.string, // only shown if the field is not disabled
    value: PropTypes.any,
    initialValue: PropTypes.any,
    autofill: PropTypes.any,
    onUpdate: PropTypes.any,
    valid: PropTypes.any,
    invalid: PropTypes.any,
    dirty: PropTypes.any,
    pristine: PropTypes.any,
    active: PropTypes.any,
    visited: PropTypes.any,
    autofilled: PropTypes.any,
    step: PropTypes.number,
    maxValue: PropTypes.number,
    minValue: PropTypes.number,
    onFocus: PropTypes.func,
    onBlur: PropTypes.func,
    wrapperClassName: PropTypes.string,
    numberInputWrapperStyles: PropTypes.any,
    disableCommas: PropTypes.bool,
  };

  static defaultProps = {
    type: "text",
    autoComplete: "new-password",
  };

  constructor(props) {
    super(props);
    this.inputRef = createRef();
    this.state = { isFocused: false };
  }

  componentDidMount() {
    if (this.props.initialFocus && this.inputRef.current) {
      // Timeout makes this work in modals. Maybe field is not visible initially due to modal animation.
      setTimeout(() => {
        this.focus();
      }, 0);
    }
  }

  focus() {
    if (!this.props.disabled && this.inputRef.current) {
      this.inputRef.current.focus();
    }
  }

  render() {
    // remove "initialFocus" from rendered input properties to avoid react warning
    const {
      className,
      onChange,
      error,
      touched,
      placeholder,
      disabled,
      type,
      step = 1,
      maxValue,
      minValue,
      wrapperClassName,
      numberInputWrapperStyles,
      disableCommas,
      prefix,
      ...props
    } = this.props;

    const { isFocused } = this.state;

    const initialValue = initializeValue(this.props.value);

    const numberInputWrapperClass = classNames(
      numberInputWrapper,
      { [numberInputWrapperError]: error && touched },
      { [numberInputWrapperDisabled]: disabled },
      { [numberInputWrapperFocused]: isFocused },
      wrapperClassName === "input-wrapper-advanced"
        ? inputWrapperAdvanced
        : null,
      className
    );

    const numberInputClass = classNames(
      ["field", textInput, numberInput, className],
      { [numberInputDisabled]: disabled },
      wrapperClassName === "input-wrapper-advanced"
        ? inputWrapperAdvanced
        : null
    );

    const handleBlur = (e) => {
      this.setState({ isFocused: false });
      this.props.onBlur?.();
    };

    const handleFocus = () => {
      this.setState({ isFocused: true });
      this.props.onFocus?.();
    };

    const handleChange = (event) => {
      typeof onChange === "function" &&
        onChange(
          Number(initializeValue(event.target.value.replaceAll(/\D/g, "")))
        );
    };

    const handleIncrement = (e) => {
      e.preventDefault();
      typeof onChange === "function" &&
        onChange(
          maxValue != null
            ? Math.min(maxValue, initialValue + step)
            : initialValue + step
        );
    };

    const handleDecrement = (e) => {
      e.preventDefault();
      typeof onChange === "function" &&
        onChange(
          minValue != null
            ? Math.max(minValue, initialValue - step)
            : initialValue - step
        );
    };

    const composedStyles = {
      ...(this.props.style || {}),
      ...(this.props.error && this.props.touched && forms.textInputError),
      ...(this.props.disabled && forms.textInputDisabled),
    };

    const getTextField = () => (
      <input
        ref={this.inputRef}
        {...props}
        type={type}
        onChange={onChange}
        disabled={disabled}
        placeholder={props.disabled ? "" : placeholder}
        defaultValue={this.props.default}
        className={classNames(["field", textInput, className])}
        style={composedStyles}
      />
    );

    let inputElement = getTextField();
    if (prefix) {
      inputElement = (
        <div className={classes["container"]}>
          {prefix && <span className={classes["prefix"]}>{prefix}</span>}
          {getTextField()}
        </div>
      );
    }

    return type !== "number" ? (
      inputElement
    ) : (
      <div
        className={numberInputWrapperClass}
        style={numberInputWrapperStyles || {}}
      >
        <input
          ref={this.inputRef}
          {...props}
          type="text"
          disabled={disabled}
          defaultValue={props.default}
          placeholder={disabled ? "" : placeholder}
          onBlur={handleBlur}
          onFocus={handleFocus}
          onChange={handleChange}
          className={numberInputClass}
          value={
            disableCommas
              ? Number(props.value)
              : Number(props.value).toLocaleString()
          }
          style={composedStyles}
        />
        <div className={numberInputButtons}>
          <IconButton
            aria-label={intl.formatMessage({ id: "Common.Increment" })}
            className={numberInputButtonIcon}
            onClick={handleIncrement}
            type="button"
          >
            <dremio-icon name="interface/caretUp" />
          </IconButton>
          <IconButton
            aria-label={intl.formatMessage({ id: "Common.Decrement" })}
            className={numberInputButtonIcon}
            onClick={handleDecrement}
            type="button"
          >
            <dremio-icon name="interface/caretDown" />
          </IconButton>
        </div>
      </div>
    );
  }
}
TextField.displayName = "TextField"; //Used in SettingsMicroForm-spec.js, remove later
export default TextField;
