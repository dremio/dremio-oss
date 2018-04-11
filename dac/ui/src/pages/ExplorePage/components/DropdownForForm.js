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
import $ from 'jquery';
import classNames from 'classnames';

import './DropdownForForm.less';

export default class DropdownForForm extends Component {
  static propTypes = {
    name: PropTypes.string.isRequired,
    onChange: PropTypes.func.isRequired,
    items: PropTypes.array.isRequired,
    currentValue: PropTypes.string.isRequired
  }

  constructor(props) {
    super(props);
    this.getItems = this.getItems.bind(this);
    this.setValue = this.setValue.bind(this);
    this.showList = this.showList.bind(this);
    this.state = {
      active: false
    };
  }

  componentDidMount() {
    this.clickHandler = (e) => {
      if (!e.target.className.match('dropdown')) {
        this.setState({
          active: false
        });
      }
    };
    $(document).on('click', this.clickHandler);
  }

  componentWillUnmount() {
    $(document).off('click', this.clickHandler);
  }

  getItems() {
    return this.props.items.map((item) => {
      const setValue = this.setValue.bind(this, item);
      return <li onClick={setValue}>{item}</li>;
    });
  }

  setValue(value) {
    this.props.onChange(value, this.props.name);
  }

  showList() {
    this.setState({
      active: !this.state.active
    });
  }

  render() {
    const classes = classNames('wrapper-dropdown', {'active': this.state.active});
    return (
      <div id={this.props.name} onClick={this.showList} className={classes}>
        <span className='dropdown-text'>{this.props.currentValue}</span>
        <ul className='dropdown'>
          {this.getItems()}
        </ul>
      </div>
    );
  }
}
