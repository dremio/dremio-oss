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
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import Select from 'components/Fields/Select';
import DragAreaColumn from 'components/DragComponents/DragAreaColumn';

@Radium
class DragSortColumn extends Component {
  static propTypes = {
    isDragInProgress: PropTypes.bool,
    field: PropTypes.object.isRequired,
    allColumns: PropTypes.instanceOf(Immutable.List).isRequired,
    dragType: PropTypes.string.isRequired,
    onRemoveColumn: PropTypes.func,
    index: PropTypes.number,
    onDragEnd: PropTypes.func
  };

  constructor(props) {
    super(props);

    this.options = [
      {
        label: 'Descending',
        option: 'DESC'
      },
      {
        label: 'Ascending',
        option: 'ASC'
      }
    ];
  }

  render() {
    const { field, index } = this.props;
    return (
      <div className='drag-sort-column' style={[styles.base]}>
        <Select
          {...field.direction}
          dataQa='sortDirection'
          style={styles.select}
          items={this.options}
          iconStyle={styles.iconStyle}
          customLabelStyle={styles.customLabelStyle}
        />
        <DragAreaColumn
          field={field.name}
          isDragInProgress={this.props.isDragInProgress}
          allColumns={this.props.allColumns}
          index={index}
          dragOrigin='sort'
          onRemoveColumn={this.props.onRemoveColumn}
          dragType={this.props.dragType}
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

export default DragSortColumn;
