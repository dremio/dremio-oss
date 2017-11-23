/*
 * Copyright (C) 2017 Dremio Corporation
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

import PropTypes from 'prop-types';

import forms from 'uiTheme/radium/forms';

@Radium
export default class TextField extends Component {

  static propTypes = {
    initialFocus: PropTypes.bool,
    error: PropTypes.string,
    onChange: PropTypes.func,
    touched: PropTypes.bool,
    disabled: PropTypes.bool, // todo: add a #readonly/readOnly(?) and switch existing uses of #disabled as appropriate)
    default: PropTypes.string,
    type: PropTypes.string,
    style: PropTypes.object
  };

  static defaultProps = {
    type: 'text',
    autoComplete: 'off'
  };

  componentDidMount() {
    if (this.props.initialFocus && this.refs.input) {
      // Timeout makes this work in modals. Maybe field is not visible initially due to modal animation.
      setTimeout(() => {
        this.focus();
      }, 0);
    }
  }

  focus() {
    !this.props.disabled && this.refs.input && this.refs.input.focus();
  }

  render() {
    return (
      <input
        ref='input'
        {...this.props}
        defaultValue={this.props.default}
        style={[
          forms.textInput,
          this.props.style,
          this.props.error && this.props.touched && forms.textInputError,
          this.props.disabled && forms.textInputDisabled
        ]}/>
    );
  }
}
