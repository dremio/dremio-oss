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

import { FieldWithError, TextField, Select } from './';

// todo: loc

@pureRender
export default class FormatField extends Component {

  static propTypes = {
    label: PropTypes.string,
    onChange: PropTypes.func,
    touched: PropTypes.bool,
    error: PropTypes.string,
    options: PropTypes.array
  };

  onSelectChange = (value) => {
    this.props.onChange(value || '');
  }

  getSelectedItemValueForSelect(value) {
    const result = this.props.options.find((item) => item.option === value);
    if (result) {
      return value;
    }
    return ''; // it's custom
  }

  render() {
    const {options, ...props} = this.props;
    const items = options.concat([{label: 'Custom…', option: ''}]);

    return (
      <FieldWithError {...props} errorPlacement='top'>
        <div style={styles.wrapper}>
          <Select
            disabled={props.disabled}
            value={this.getSelectedItemValueForSelect(props.value)}
            onChange={this.onSelectChange}
            items={items}
            style={styles.menu}/>
          <TextField {...props} style={styles.textField}/>
        </div>
      </FieldWithError>
    );
  }
}

const styles = {
  wrapper: {
    display: 'flex'
  },
  menu: {
    marginLeft: 0,
    flex: 1
  },
  textField: {
    marginLeft: 10,
    width: 50
  }
};
