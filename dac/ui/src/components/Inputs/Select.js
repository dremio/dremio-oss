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
import { Component, PropTypes } from 'react';
import Immutable from 'immutable';
import ReactSelect from 'react-select';
import Radium from 'radium';

import './Select.less'; // For now it's nessesary because react-select use css

@Radium
export default class Select extends Component {
  static propTypes = {
    items: PropTypes.instanceOf(Immutable.List), // each item should be {name: 'name', label: 'label'}
    clearable: PropTypes.bool,
    editable: PropTypes.bool,
    disabled: PropTypes.bool,
    defaultValue: PropTypes.instanceOf(Immutable.Map),
    onChange: PropTypes.func,
    name: PropTypes.string,
    style: PropTypes.object,
    defaultItem: PropTypes.object
  }

  constructor(props) {
    super(props);
    this.setCurrentValue = this.setCurrentValue.bind(this);
  }

  setCurrentValue(value) {
    if (this.props.onChange) {
      this.props.onChange(value);
    }
  }

  mapForSelect() {
    return this.props.items.toJS();
  }

  render() {
    const style = {
      width: 230,
      height: 30
    };

    return (
      <div style={[style, this.props.style]} className='react-select-styles'>
        <ReactSelect
          clearable={this.props.clearable || false}
          searchable={this.props.editable || false}
          name={this.props.name || 'defaultSelect'}
          value={{label: this.props.defaultValue || this.props.defaultItem || ''}}
          options={this.mapForSelect()}
          onChange={this.setCurrentValue}/>
      </div>
    );
  }
}
