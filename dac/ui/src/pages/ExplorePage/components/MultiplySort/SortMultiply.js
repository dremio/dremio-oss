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
import classNames from 'classnames';

import DragColumnMenu from 'components/DragComponents/DragColumnMenu';
import FontIcon from 'components/Icon/FontIcon';
import EllipsedText from 'components/EllipsedText';

import SortDragArea from './components/SortDragArea';
import { header, base, name, inner, addJoin } from './SortMultiply.less';

// TODO: DRY and fix with Join/Group BY

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
    return ( // todo: loc
      <div className={classNames(['inner-join', base])} onMouseUp={this.props.handleDragStop}>
        <div className={header}>
          <div className={name}>
            <EllipsedText text={`“${this.props.dataset.getIn(['displayFullPath', -1])}” fields:`}/>
          </div>
          <div className={name} style={{borderRight: 'none'}}>
            {la('Sort fields:')}
          </div>
        </div>
        <div className={inner}>
          <DragColumnMenu
            items={this.props.columns || Immutable.List()}
            disabledColumnNames={this.getNamesOfColumnsInDragArea()}
            type='column'
            onDragEnd={this.props.handleDragStop}
            handleDragStart={this.props.handleDragStart}
            dragType={this.props.dragType}
            style={styles.dragNameStyle}
            name={this.props.path + ' <current>'}/>
          <SortDragArea
            columnsField={this.props.columnsField}
            allColumns={this.props.columns}
            onDrop={this.props.handleDrop}
            dragType={this.props.dragType}
            isDragInProgress={this.props.isDragInProgress}
          />
        </div>
        <span
          className={addJoin}
          onClick={this.props.addAnother}> {/* todo: ax, consistency: button */}
          <FontIcon type='Add' hoverType='AddHover' theme={{Container: {display: 'flex', alignItems: 'center'}}}/>
          {la('Add a Sort Field')}
        </span>
      </div>
    );
  }
}

const styles = {
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
    width: 240, // -1 from styles.name
    minWidth: 240 // -1 from styles.name
  }
};

export default SortMultiply;
