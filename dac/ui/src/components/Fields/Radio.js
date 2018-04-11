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
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import { BLUE } from 'uiTheme/radium/colors';
import Checkbox, { checkboxPropTypes } from './Checkbox';

@Radium
@pureRender
export default class Radio extends Component {

  static propTypes = {
    ...checkboxPropTypes,
    // radio is checked based on props.checked if it is defined, or
    // if props.value (of the field) matches props.radioValue (of this radio).
    radioValue: PropTypes.any
  }

  renderDummyRadio = (isChecked, style) => {
    return <div data-qa={this.props.radioValue}
      style={[styles.dummy, style, isChecked ? styles.checked : null]}>{isChecked ? <div style={styles.dot}/> : ''}
    </div>;
  }

  render() {
    const {checked, value, radioValue, ...props} = this.props;
    const finalChecked = checked === undefined ? radioValue === value : checked;
    return <Checkbox {...props} value={radioValue} checked={finalChecked}
      inputType='radio' renderDummyInput={this.renderDummyRadio}/>;
  }
}

const styles = {
  dummy: {
    flexShrink: 0,
    width: 13,
    height: 13,
    marginRight: 5,
    padding: '5px 2px',
    border: '1px solid #bbb',
    borderRadius: '50%',
    background: '#fff'
  },
  dot: {
    background: BLUE,
    width: 7,
    height: 7,
    borderRadius: '50%',
    position: 'relative',
    top: -3
  },
  checked: {
    border: `1px solid ${BLUE}`,
    color: BLUE
  }
};
