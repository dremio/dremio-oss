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
import pureRender from 'pure-render-decorator';
import Radium from 'radium';
import Immutable from 'immutable';
import { AutoSizer, List } from 'react-virtualized';

import { SearchField } from 'components/Fields';

import ColumnMenuItem from './ColumnMenuItem';

export const NOT_SUPPORTED_TYPES = new Set(['MAP', 'LIST']);

@pureRender
@Radium
export default class DragColumnMenu extends Component {
  static propTypes = {
    items: PropTypes.instanceOf(Immutable.List).isRequired,
    disabledColumnNames: PropTypes.instanceOf(Immutable.Set).isRequired,
    namesOfColumnsInDragArea: PropTypes.array,
    handleDragStart: PropTypes.func,
    onDragEnd: PropTypes.func,
    name: PropTypes.string.isRequired,
    dragType: PropTypes.string.isRequired,
    style: PropTypes.object,
    type: PropTypes.string,
    fieldType: PropTypes.string
  }

  static sortColumns(columns, disabledColumnNames) {
    return columns.sortBy((column) =>
      (disabledColumnNames.has(column.get('name')) ? columns.size : 0) + column.get('index')
    );
  }

  filteredSortedColumns = undefined

  constructor(props) {
    super(props);
    this.state = {
      filter: ''
    };
    this.updateColumns(this.state.filter, props.disabledColumnNames);
  }

  onFilterChange = (filter) => {
    this.setState({
      filter
    });
  }

  componentWillUpdate(nextProps, nextState) {
    if (nextProps.items !== this.props.items ||
      nextProps.disabledColumnNames !== this.props.disabledColumnNames ||
      nextState.filter !== this.state.filter) {
      this.updateColumns(nextState.filter, nextProps.disabledColumnNames);
    }
  }

  updateColumns(filter, disabledColumnNames) {
    this.filteredSortedColumns = DragColumnMenu.sortColumns(
      this.filterColumns(filter, this.props.items),
      disabledColumnNames
    );
    if (this.virtualList) {
      this.virtualList.forceUpdateGrid();
    }
  }

  filterColumns(filter, allColumns) {
    return allColumns.filter((column) => column.get('name').toLowerCase().includes(filter.trim().toLowerCase()));
  }

  renderColumn = ({ index, key, style }) => {
    const column = this.filteredSortedColumns.get(index);
    return <div key={key} style={style}>
      <ColumnMenuItem
        item={column}
        disabled={this.props.disabledColumnNames.has(column.get('name'))}
        type={this.props.type}
        fieldType={this.props.fieldType}
        handleDragStart={this.props.handleDragStart}
        onDragEnd={this.props.onDragEnd}
        name={this.props.name}
        dragType={this.props.dragType}
      />
    </div>;
  }

  render() {
    return (
      <div className='inner-join-left-menu' style={[styles.base, this.props.style]}>
        <SearchField
          dataQa={'search-field-' + this.props.fieldType}
          showCloseIcon
          placeholder={la('Search fields...')}
          value={this.state.filter}
          onChange={this.onFilterChange}
          style={styles.searchStyle}
        />
        <div style={{flexGrow: 1}}>
          <AutoSizer>
            {({ height, width }) => (
              <List
                ref={(ref) => this.virtualList = ref}
                rowRenderer={this.renderColumn}
                rowCount={this.filteredSortedColumns.size}
                rowHeight={29}
                height={height}
                width={width}
              />
            )}
          </AutoSizer>
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    flexDirection: 'column',
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
    minHeight: 150
  },
  title: {
    paddingLeft: 5,
    paddingTop: 5,
    color: '#000',
    fontWeight: 600
  },
  searchStyle: {
    width: 230,
    height: 30,
    display: 'block'
  }
};
