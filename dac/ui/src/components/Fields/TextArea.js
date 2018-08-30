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

import forms from 'uiTheme/radium/forms';

@Radium
@pureRender
export default class TextArea extends Component {

  static propTypes = {
    error: PropTypes.string,
    disabled: PropTypes.bool,
    style: PropTypes.object,
    initialValue: PropTypes.any,
    autofill: PropTypes.any,
    onUpdate: PropTypes.any,
    valid: PropTypes.any,
    invalid: PropTypes.any,
    dirty: PropTypes.any,
    pristine: PropTypes.any,
    active: PropTypes.any,
    touched: PropTypes.any,
    visited: PropTypes.any,
    autofilled: PropTypes.any
  };

  constructor(props) {
    super(props);
  }

  render() {
    const {
      initialValue, autofill, onUpdate, valid, invalid, dirty, pristine, error, active, touched, visited, autofilled,
      ...props
    } = this.props;
    return (
      <textarea {...props} className='field'
        style={[
          forms.textArea,
          this.props.error && forms.textInputError,
          this.props.disabled && forms.textInputDisabled,
          this.props.style && {...this.props.style}
        ]}
      />
    );
  }
}
