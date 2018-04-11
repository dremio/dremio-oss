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
import PropTypes from 'prop-types';
import Keys from 'constants/Keys.json';
import TextField from './TextField';

export default class PrevalidatedTextField extends Component {
  static propTypes = {
    onChange: PropTypes.func,
    validate: PropTypes.func,
    value: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.number
    ])
  }

  constructor(props) {
    super(props);

    this.state = {
      internalValue: props.value
    };
  }

  componentWillReceiveProps(nextProps) {
    if (this.state.internalValue === '' || this.props.value !== nextProps.value) {
      this.setState({ internalValue: nextProps.value });
    }
  }

  handleTextFieldChange = (e) => {
    const { value } = e.target;
    this.setState({ internalValue: value });
  }

  handleUpdateTextField = () => {
    const { validate, onChange, value } = this.props;
    if (!validate || validate(this.state.internalValue)) {
      onChange(this.state.internalValue);
    } else {
      this.setState({ internalValue: value });
    }
  }

  handleTextFieldBlur = () => {
    this.handleUpdateTextField();
  }

  handleTextFieldKeyDown = (e) => {
    if (e.keyCode === Keys.ENTER) {
      e.stopPropagation();
      this.handleUpdateTextField();
    }
  }

  render() {
    return (
      <TextField
        {...this.props}
        value={this.state.internalValue}
        onChange={this.handleTextFieldChange}
        onBlur={this.handleTextFieldBlur}
        onKeyDown={this.handleTextFieldKeyDown}
      />
    );
  }
}
