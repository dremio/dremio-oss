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
import React, { useState } from 'react';
import PropTypes from 'prop-types';
import clsx from 'clsx';
import get from 'lodash.get';

import FormValidationMessage from '../FormValidationMessage';
import Label from '../Label';
import CopyToClipboard from '../CopyToClipboard';
import ToggleFieldVisibility from '../ToggleFieldVisibility';

import { ReactComponent as ArrowUpIcon } from '../../art/InputArrowUp.svg';
import { ReactComponent as ArrowDownIcon } from '../../art/InputArrowDown.svg';

import './input.scss';

const Input = (props) => {

  const {
    label,
    labelStyle,
    classes = {},
    disabled,
    enableCopy,
    name,
    onChange,
    onCopy,
    onFocus,
    onView,
    onBlur,
    form: {
      values,
      errors,
      touched,
      setFieldValue,
      handleChange: formikHandleChange
    },
    hideError,
    prefix,
    toggleField,
    type,
    value,
    helpText,
    step,
    maxValue,
    minValue,
    ...otherProps
  } = props;

  // This is needed because we have a wrapper div which has the border to handle prefix.
  const [ focused, setFocused ] = useState(false);

  const hasError = get(touched, name) && get(errors, name);
  const showError = !hideError && hasError;

  const rootClass = clsx('input-root', { [classes.root]: classes.root });
  const labelClass = clsx('input-root__label', { [classes.label]: classes.label });
  const labelInnerClass = clsx({ [classes.labelInner]: classes.labelInner });
  const containerClass = clsx(
    'input__container',
    { [classes.container]: classes.container },
    { '--disabled': disabled },
    { '--error': hasError },
    { '--focused': focused }
  );
  const inputClass = clsx({
    '--prefixed': prefix,
    [classes.input]: classes.input
  });
  const inputNumberButtonsClass = clsx(
    'input__numberButtons',
    { '--disabled': disabled },
    { '--error': hasError },
    { '--focused': focused }
  );

  const handleFocus = (...args) => {
    setFocused(true);
    if (onFocus && typeof onFocus === 'function') {
      onFocus(...args);
    }
  };

  const handleBlur = (...args) => {
    setFocused(false);
    if (type === 'number') {
      const commaSeparatedValue = Number(values[name].replaceAll(',', '')).toLocaleString();
      setFieldValue(name, commaSeparatedValue);
    }
    if (onBlur && typeof onBlur === 'function') {
      onBlur(...args);
    }
  };

  const handleChange = (event) => {
    const commaSeparatedValue = Number(event.target.value.replaceAll(/\D/g, '')).toLocaleString();
    event.target.value = commaSeparatedValue;
    formikHandleChange && typeof onChange === 'function' && formikHandleChange(event);
    onChange && typeof onChange === 'function' && onChange(event);
  };

  const handleIncrement = () => {
    if (value && setFieldValue && typeof setFieldValue === 'function') {
      const numericalValue =
        maxValue !== undefined
          ? Math.min(maxValue, Number(value.replaceAll(',', '')) + step)
          : Number(value.replaceAll(',', '')) + step;
      const commaSeparatedValue = numericalValue.toLocaleString();
      setFieldValue(name, commaSeparatedValue);
    }
  };

  const handleDecrement = () => {
    if (value && setFieldValue && typeof setFieldValue === 'function') {
      const numericalValue =
        minValue !== undefined
          ? Math.max(minValue, Number(value.replaceAll(',', '')) - step)
          : Number(value.replaceAll(',', '')) - step;
      const commaSeparatedValue = numericalValue.toLocaleString();
      setFieldValue(name, commaSeparatedValue);
    }
  };

  return (
    <div className={rootClass}>
      {label && (
        <div className='input-root__labelContainer'>
          <Label
            value={label}
            className={labelClass}
            labelInnerClass={labelInnerClass}
            style={labelStyle}
            id={`input-label-${name}`}
            helpText={helpText}
          />
          {enableCopy && <CopyToClipboard value={value} onCopy={onCopy}/>}
          {toggleField && <ToggleFieldVisibility onView={onView}/>}
        </div>
      )}
      <div className={containerClass}>
        {prefix && <span className='input__prefix'>{prefix}</span>}
        <input
          className={inputClass}
          aria-labelledby={`input-label-${name}`}
          disabled={disabled}
          value={value}
          name={name}
          onChange={type === 'number' ? handleChange : onChange}
          onFocus={handleFocus}
          onBlur={handleBlur}
          type={type === 'number' ? 'text' : type}
          {...otherProps}
        />
        {type === 'number' && (
          <div className={inputNumberButtonsClass}>
            <ArrowUpIcon className='icon' onClick={handleIncrement}/>
            <ArrowDownIcon className='icon' onClick={handleDecrement}/>
          </div>
        )}
      </div>
      {showError && (
        <FormValidationMessage className='input__validationError' id={`input-error-${name}`}>
          {get(errors, name)}
        </FormValidationMessage>
      )}
    </div>
  );
};

Input.propTypes = {
  disabled: PropTypes.bool,
  label: PropTypes.string,
  labelStyle: PropTypes.object,
  classes: PropTypes.shape({
    root: PropTypes.string,
    input: PropTypes.string,
    container: PropTypes.string
  }),
  name: PropTypes.string,
  onFocus: PropTypes.func,
  onBlur: PropTypes.func,
  onChange: PropTypes.func,
  type: PropTypes.string,
  form: PropTypes.object.isRequired,
  hideError: PropTypes.bool,
  prefix: PropTypes.string,
  value: PropTypes.any,
  helpText: PropTypes.string,
  enableCopy: PropTypes.bool,
  toggleField: PropTypes.bool,
  onCopy: PropTypes.func,
  onView: PropTypes.func,
  step: PropTypes.number,
  maxValue: PropTypes.number,
  minValue: PropTypes.number
};

Input.defaultProps = {
  disabled: false,
  label: null,
  labelStyle: {},
  classes: {},
  hideError: false,
  step: 1
};

export default Input;
