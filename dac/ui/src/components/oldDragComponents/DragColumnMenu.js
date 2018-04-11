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
import Radium from 'radium';
import Immutable from 'immutable';

import ColumnMenuItem from './ColumnMenuItem';

export const NOT_SUPPORTED_TYPES = ['MAP', 'LIST'];

@pureRender
@Radium
export default class DragColumnMenu extends Component {
  static propTypes = {
    items: PropTypes.instanceOf(Immutable.List).isRequired,
    namesOfColumnsInDragArea: PropTypes.instanceOf(Immutable.List),
    handleDragStart: PropTypes.func,
    onDragEnd: PropTypes.func,
    dragAreaColumnSize: PropTypes.number,
    name: PropTypes.string.isRequired,
    dragType: PropTypes.string.isRequired,
    style: PropTypes.object,
    type: PropTypes.string
  }

  static formatColumnItem(column, namesOfColumnsInDragArea) {
    const isAddedToDragArea = namesOfColumnsInDragArea &&
      namesOfColumnsInDragArea.find(item => item === column.get('name'));
    const isNotSupported = NOT_SUPPORTED_TYPES.indexOf(column.get('type')) !== -1;

    if (isAddedToDragArea || isNotSupported) {
      return column.set('disabled', true);
    }
    return column;
  }

  static decorateColumns(columns, namesOfColumnsInDragArea) {
    return columns.map(column => {
      return DragColumnMenu.formatColumnItem(column, namesOfColumnsInDragArea);
    }).sort(a => (a.get('disabled')));
  }

  constructor(props) {
    super(props);
    this.getItems = this.getItems.bind(this);
  }

  getItems() {
    const namesOfColumnsInDragArea = this.props.namesOfColumnsInDragArea || [];
    return DragColumnMenu.decorateColumns(this.props.items, namesOfColumnsInDragArea).map(item => (
      <ColumnMenuItem
        item={item}
        type={this.props.type}
        handleDragStart={this.props.handleDragStart}
        onDragEnd={this.props.onDragEnd}
        key={item.get('name')}
        dragType={this.props.dragType}/>
      )
    );
  }

  render() {
    return (
      <div className='inner-join-left-menu' style={[styles.base, this.props.style]}>
        <div style={[styles.content]}>
          <div style={[styles.items]}>
            {this.getItems()}
          </div>
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    width: 275,
    height: 180,
    borderRight: '1px solid rgba(0,0,0,0.10)',
    paddingRight: 5,
    paddingLeft: 5
  },
  content: {
    display: 'flex',
    flexDirection: 'column',
    width: '100%',
    overflowY: 'hidden'
  },
  items: {
    display: 'flex',
    flexDirection: 'column',
    overflowY: 'auto',
    maxHeight: 180,
    minHeight: 150
  },
  title: {
    paddingLeft: 5,
    paddingTop: 5,
    color: '#000',
    fontWeight: 600
  }
};
