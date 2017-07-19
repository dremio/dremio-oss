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
import Radium from 'radium';
import Immutable from 'immutable';

import DragColumnMenu from 'components/DragComponents/DragColumnMenu';
import FontIcon from 'components/Icon/FontIcon';

import { body } from 'uiTheme/radium/typography';

import SortDragArea from './components/SortDragArea';

@Radium
class SortMultiply extends Component {
  static propTypes = {
    columnsField: PropTypes.array,
    dataset: PropTypes.instanceOf(Immutable.Map),
    isDragInProgress: PropTypes.bool,
    columns: PropTypes.instanceOf(Immutable.List).isRequired,
    path: PropTypes.string,
    handleDrop: PropTypes.func.isRequired,
    handleDragStart: PropTypes.func,
    handleDragStop: PropTypes.func,
    dragType: PropTypes.string.isRequired,
    addAnother: PropTypes.func
  };

  getNamesOfColumnsInDragArea() {
    return Immutable.Set(this.props.columnsField.map(item => item.name.value));
  }

  render() {
    return (
      <div className='inner-join' style={[styles.base]} onMouseUp={this.props.handleDragStop}>
        <div style={[styles.header]}>
          <div style={[styles.name, body]}>
            {this.props.dataset.getIn(['displayFullPath', -1])}  Fields:
          </div>
          <div style={[styles.name, body, {borderRight: 'none'}]}>
            Sort Fields:
          </div>
        </div>
        <div style={[styles.inner]}>
          <DragColumnMenu
            items={this.props.columns || Immutable.List()}
            disabledColumnNames={this.getNamesOfColumnsInDragArea()}
            type='column'
            onDragEnd={this.props.handleDragStop}
            handleDragStart={this.props.handleDragStart}
            dragType={this.props.dragType}
            style={[styles.dragNameStyle]}
            name={this.props.path + ' (Current)'}/>
          <SortDragArea
            columnsField={this.props.columnsField}
            allColumns={this.props.columns}
            onDrop={this.props.handleDrop}
            dragType={this.props.dragType}
            isDragInProgress={this.props.isDragInProgress}
          />
        </div>
        <span
          style={[styles.addJoinStyle, body]}
          onClick={this.props.addAnother}>
          <FontIcon type='Add' hoverType='AddHover' theme={{Container: {display: 'flex', alignItems: 'center'}}}/>
          Add Another Sort Field
        </span>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    height: '100%',
    flexWrap: 'wrap'
  },
  name: {
    display: 'flex',
    alignItems: 'center',
    paddingLeft: 10,
    height: 26,
    width: 220,
    borderRight: '1px solid rgba(0,0,0,0.10)'
  },
  header: {
    backgroundColor: '#f3f3f3',
    height: 26,
    display: 'flex',
    alignItems: 'center',
    width: '100%',
    margin: '10px 10px 0 10px'
  },
  inner: {
    width: '100%',
    backgroundColor: '#fff',
    justifyContent: 'space-between',
    border: '1px solid rgba(0,0,0,0.10)',
    display: 'flex',
    margin: '0 10px',
    maxHeight: 180,
    minHeight: 180
  },
  rightMenu: {
    borderLeft: '1px solid rgba(0,0,0,0.10)'
  },
  dragWrapStyles: {
    display: 'flex',
    width: '100%',
    justifyContent: 'space-between'
  },
  dragOneWrapStyles: {
    display: 'flex',
    width: '100%',
    overflowY: 'auto',
    justifyContent: 'space-between'
  },
  addJoinStyle: {
    color: '#0096FF',
    position: 'relative',
    marginLeft: 230,
    fontSize: 13,
    cursor: 'pointer',
    alignItems: 'center',
    height: 35,
    display: 'inline-flex'
  },
  dragArea: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    width: '100%',
    height: 180
  },
  border: {
    borderLeft: '1px solid rgba(0,0,0,0.10)'
  },
  dragAreaText: {
    width: 180,
    color: 'gray',
    fontSize: 12,
    textAlign: 'center',
    display: 'inline-block'
  },
  dragNameStyle: {
    width: 219,
    minWidth: 219
  }
};

export default SortMultiply;
