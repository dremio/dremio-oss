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
import Divider from '@material-ui/core/Divider';

import { AUTO_TYPES } from 'constants/columnTypeGroups';
import { JSONTYPE } from 'constants/DataTypes';

import ColumnMenuItem from './../ColumnMenus/ColumnMenuItem';
import MenuItem from './../MenuItem';

@Radium
@pureRender
export default class AutoGroup extends Component {
  static propTypes = {
    makeTransform: PropTypes.func.isRequired,
    columnType: PropTypes.string
  }

  constructor(props) {
    super(props);
    this.setVisibility = this.setVisibility.bind(this);
    this.state = {
      visibility: true
    };
  }

  componentDidMount() {
    if (!this.refs.root || !this.refs.root.children) {
      return null;
    }

    const divs = [...this.refs.root.children].filter(child => child.nodeName === 'DIV');
    if (divs.length === 1) {
      this.setVisibility(false);
    } else {
      this.setVisibility(true);
    }
  }

  setVisibility(visibility) {
    this.setState({
      visibility
    });
  }

  render() {
    return (
      <div ref='root' style={{display: this.state.visibility ? 'block' : 'none'}}>
        <Divider style={{marginTop: 5, marginBottom: 5}}/>
        <MenuItem disabled>AUTO-DETECT</MenuItem>
        <ColumnMenuItem
          actionType={JSONTYPE}
          columnType={this.props.columnType}
          title='JSON'
          availableTypes={AUTO_TYPES}
          onClick={this.props.makeTransform}/>
      </div>
    );
  }
}
