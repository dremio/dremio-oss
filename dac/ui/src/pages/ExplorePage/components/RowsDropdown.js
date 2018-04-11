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

import './RowsDropdown.less';
const STEP = 100;

@pureRender
class RowsDropdown extends Component {
  static propTypes = {
    setMaxNumberOfRows: PropTypes.func.isRequired,
    tableLength: PropTypes.oneOfType([PropTypes.string, PropTypes.number])
  };

  constructor(props) {
    super(props);
    this.toogleDropdown = this.toogleDropdown.bind(this);
    this.getItems = this.getItems.bind(this);
    this.getDropDown = this.getDropDown.bind(this);
    let maxValue = (props.tableLength + STEP) / STEP;
    maxValue = (Math.round(maxValue) * STEP);

    this.state = {
      currentValue: props.tableLength ? STEP : 0,
      dropdownState: false,
      min: STEP,
      max: maxValue
    };
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.tableLength !== this.props.tableLength) {
      let maxValue = (nextProps.tableLength + STEP) / STEP;
      maxValue = (Math.round(maxValue) * STEP);

      this.state = {
        currentValue: nextProps.tableLength ? STEP : 0,
        dropdownState: false,
        min: STEP,
        max: maxValue
      };
    }
  }

  getDropDown() {
    return <div className='items-container'>
      {this.getItems()}
    </div>;
  }

  getItems() {
    const numberOfRows = (this.state.max - this.state.min) / STEP + 1;
    const items = [];
    for (let i = 0; i < numberOfRows; i++) {
      items.push(
        <div
          className='row-item'
          key={i}
          onClick={this.setCurrentValue.bind(this, i * STEP)}>
          {i * STEP} rows
        </div>
      );
    }
    return items;
  }

  setCurrentValue(value) {
    this.setState({
      currentValue: value
    });
    if (this.props.setMaxNumberOfRows) {
      this.props.setMaxNumberOfRows(value);
    }
  }

  toogleDropdown() {
    this.setState({
      dropdownState: !this.state.dropdownState
    });
  }

  render() {
    const dropdown = this.state.dropdownState
      ? this.getDropDown()
      : null;
    return (
      <div className='rows-dropdown' onClick={this.toogleDropdown}>
        <span className='text'>First {this.state.currentValue} rows</span>
        <i className='fa fa-angle-down'></i>
        {dropdown}
      </div>
    );
  }
}

export default RowsDropdown;
