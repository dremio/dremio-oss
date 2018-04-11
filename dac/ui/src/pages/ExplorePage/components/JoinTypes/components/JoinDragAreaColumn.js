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

import DragAreaColumn from 'components/oldDragComponents/DragAreaColumn';

@pureRender
@Radium
class JoinDragAreaColumn extends Component {
  static propTypes = {
    dragColumntableType: PropTypes.string,
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    leftColumns: PropTypes.instanceOf(Immutable.List).isRequired,
    items: PropTypes.instanceOf(Immutable.List).isRequired,
    rightColumns: PropTypes.instanceOf(Immutable.List).isRequired,
    dragType: PropTypes.string.isRequired,
    type: PropTypes.string.isRequired,
    onDragStart: PropTypes.func,
    isDragInProgress: PropTypes.bool,
    moveColumn: PropTypes.func,
    removeColumn: PropTypes.func,
    index: PropTypes.number,
    showColumnDropdown: PropTypes.func,
    showSortDropdown: PropTypes.func,
    handleMeasureChange: PropTypes.func,
    onDragEnd: PropTypes.func,
    addColumn: PropTypes.func
  };

  constructor(props) {
    super(props);
    this.filterLeftColumns = this.filterLeftColumns.bind(this);
  }

  filterLeftColumns() {
    return this.props.leftColumns.filter(col => {
      const isFound = this.props.items &&
                      this.props.items.find(item => item.get('default').get('name') === col.get('name'));
      return !isFound;
    });
  }

  filterByType(columns, linkedItem) {
    return columns.filter(column => (linkedItem.get('empty') || column.get('type') === linkedItem.get('type')));
  }

  render() {
    const { item, index } = this.props;
    return (
      <div className='drag-measure-column' style={[styles.base]}>
        <DragAreaColumn
          dragColumntableType={this.props.dragColumntableType}
          ownDragColumntableType='default'
          isDragInProgress={this.props.isDragInProgress}
          addColumn={this.props.addColumn}
          columns={this.filterByType(this.filterLeftColumns(), item.get('custom'))}
          key={item.get('id')}
          index={index}
          icon={<div style={styles.equal}>=</div>}
          type='default'
          removeColumn={this.props.removeColumn}
          moveColumn={this.props.moveColumn}
          dragType={this.props.dragType}
          id={item.get('id')}
          item={item.get('default')}
        />
        <DragAreaColumn
          dragColumntableType={this.props.dragColumntableType}
          ownDragColumntableType='custom'
          isDragInProgress={this.props.isDragInProgress}
          addColumn={this.props.addColumn}
          columns={this.filterByType(this.props.rightColumns, item.get('default'))}
          key={item.get('id') + 'custom'}
          index={index}
          type='custom'
          removeColumn={this.props.removeColumn.bind(this, item.get('id'))}
          moveColumn={this.props.moveColumn}
          dragType={this.props.dragType}
          id={item.get('id')}
          item={item.get('custom')}
        />
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    flexWrap: 'nowrap',
    alignItems: 'center'
  },
  equal: {
    marginLeft: 7
  },
  iconStyle: {
    top: 0
  },
  customLabelStyle: {
    top: 13
  },
  select: {
    width: 400,
    height: 25,
    marginLeft: 5,
    marginTop: 4
  }
};

export default JoinDragAreaColumn;
