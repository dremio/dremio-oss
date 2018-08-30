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
import classNames from 'classnames';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import Checkbox, { checkboxPropTypes } from './Checkbox';
import { dot, dummy } from './Radio.less';

@pureRender
export default class Radio extends Component {

  static propTypes = {
    ...checkboxPropTypes,
    // radio is checked based on props.checked if it is defined, or
    // if props.value (of the field) matches props.radioValue (of this radio).
    radioValue: PropTypes.any
  }

  renderDummyRadio = (isChecked, style) => {
    return <div
      data-qa={this.props.radioValue}
      className={classNames(dummy, isChecked && 'checked')}
      style={style}>
      {isChecked ? <div className={dot} /> : ''}
    </div>;
  }

  render() {
    const {checked, value, radioValue, ...props} = this.props;
    const finalChecked = checked === undefined ? radioValue === value : checked;
    return <Checkbox {...props} value={radioValue} checked={finalChecked}
      inputType='radio' renderDummyInput={this.renderDummyRadio}/>;
  }
}
