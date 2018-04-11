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
import Immutable  from 'immutable';

import './DropDownHints.less';

@pureRender
class DropDownHints extends Component {
  static propTypes = {
    dialog: PropTypes.instanceOf(Immutable.Map).isRequired,
    insertIntoCode: PropTypes.func.isRequired,
    line: PropTypes.number.isRequired,
    char: PropTypes.number.isRequired,
    items: PropTypes.object,
    info: PropTypes.instanceOf(Immutable.Map)
  }

  constructor(props) {
    super(props);
  }

  renderHints() {
    return this.props.items.map((item) => {
      return (
        <div
          className='hint'
          key={item.get('text')}
          onClick={this.props.insertIntoCode.bind(this, item.get('text'))}>
          <div className='hint-title' >
            {item.get('text')}
          </div>
        </div>);
    }).toList();
  }

  render() {
    const {line, char} = this.props;
    const top = 65 + line * 20;
    const left = 25 + char * 8.43;
    const style = {
      left: `${left}px`,
      top: `${top}px`
    };
    const hintList = this.renderHints();
    const block = this.props.info.get('header')
      ? <div>
        <div className='name-header'>
          {this.props.info && this.props.info.get('header')}
        </div>
        <div className='name-type'>
          {this.props.info && this.props.info.get('type')}
        </div>
        <div className='name-description'>
          {this.props.info && this.props.info.get('description')}
        </div>
        <div className='name-text'>
          {this.props.info && this.props.info.get('text')}
        </div>
      </div>
      : null;
    return (
      <div className='drop-down-hints' style={style}>
        {block}
        {hintList}
      </div>);
  }

}

export default DropDownHints;
