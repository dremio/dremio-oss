/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { Component } from 'react';

import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import classNames from 'classnames';
import {
  base,
  labelContent,
  disabled as disabledCls,
  dummy
} from './Checkbox.less';

export const checkboxPropTypes = {
  label: PropTypes.node,
  dataQa: PropTypes.string,
  labelBefore: PropTypes.bool,
  inputType: PropTypes.string,
  checked: PropTypes.bool,
  disabled: PropTypes.bool,
  inverted: PropTypes.bool,
  renderDummyInput: PropTypes.func,
  dummyInputStyle: PropTypes.object,
  style: PropTypes.object,
  className: PropTypes.string,
  initialValue: PropTypes.any,
  autofill: PropTypes.any,
  onUpdate: PropTypes.any,
  valid: PropTypes.any,
  invalid: PropTypes.any,
  dirty: PropTypes.any,
  pristine: PropTypes.any,
  error: PropTypes.any,
  active: PropTypes.any,
  touched: PropTypes.any,
  visited: PropTypes.any,
  autofilled: PropTypes.any
};

@pureRender
export default class Checkbox extends Component {

  static propTypes = checkboxPropTypes;

  static defaultProps = {
    inputType: 'checkbox'
  };

  renderDummyCheckbox(isChecked, style) {
    return <div className={classNames(dummy, isChecked && 'checked')} style={style}
      data-qa={this.props.dataQa || 'dummyCheckbox'}>
      {isChecked ? '✔' : '\u00A0'}
    </div>;
  }

  render() {
    const {
      style, label, dummyInputStyle,
      inputType, labelBefore,
      className, inverted, renderDummyInput,
      initialValue, autofill, onUpdate, valid, invalid, dirty, pristine, error, active, touched, visited, autofilled,
      ...props
    } = this.props;
    const labelSpan = <span className={labelContent}>{label}</span>;
    const dummyCheckState = (inverted) ? !props.checked : props.checked;

    // <input .../> should be before dummy input to '~' css selector work
    return (
      <label className={classNames(['field', base, this.props.disabled && disabledCls, className])} key='container' style={style}>
        {labelBefore && labelSpan}
        <input type={inputType} style={{position: 'absolute', left: -10000}} {...props}/>
        {renderDummyInput ?
          renderDummyInput(props.checked, dummyInputStyle) :
          this.renderDummyCheckbox(dummyCheckState, dummyInputStyle)
        }
        {!labelBefore && labelSpan}
      </label>
    );
  }
}
