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

import FontIcon from 'components/Icon/FontIcon';
import SelectWithPopover from 'components/Fields/SelectWithPopover';

import { body, bodySmall, formDefault } from 'uiTheme/radium/typography';
import { PALE_BLUE, PALE_GREY } from 'uiTheme/radium/colors';
import { INLINE_NOWRAP_ROW_FLEX_START } from 'uiTheme/radium/flexStyle';

import JoinColumnMenu from './components/JoinColumnMenu';
import JoinDragArea from './components/JoinDragArea';

const DEFAULT_WIDTH = 200;

@Radium
export class InnerJoin extends Component {
  static propTypes = {
    dragColumntableType: PropTypes.string,
    leftColumns: PropTypes.instanceOf(Immutable.List).isRequired,
    rightColumns: PropTypes.instanceOf(Immutable.List).isRequired,
    removeColumn: PropTypes.func.isRequired,
    addColumnToInnerJoin: PropTypes.func.isRequired,
    stopDrag: PropTypes.func.isRequired,
    onDragStart: PropTypes.func.isRequired,
    handleDrop: PropTypes.func.isRequired,
    addEmptyColumnToInnerJoin: PropTypes.func.isRequired,
    dragType: PropTypes.string.isRequired,
    type: PropTypes.string,
    fields: PropTypes.object,
    defaultPath: PropTypes.string,
    customPath: PropTypes.string,
    isDragInProgress: PropTypes.bool,
    columnDragName: PropTypes.string,
    columnsInDragArea: PropTypes.instanceOf(Immutable.List)
  };

  leftDisabledColumnNames = undefined;
  rightDisabledColumnNames = undefined;

  constructor(props) {
    super(props);

    this.renderCurItem = this.renderCurItem.bind(this);

    this.items = [
      {
        label: 'Inner',
        value: 'Inner',
        des: 'Only matching records',
        icon: 'JoinInner'
      },
      {
        label: 'Left Outer',
        value: 'LeftOuter',
        des: 'All records from left, matching records from right',
        icon: 'JoinLeft'
      },
      {
        label: 'Right Outer',
        value: 'RightOuter',
        des: 'All records from right, matching records from left',
        icon: 'JoinRight'
      },
      {
        label: 'Full Outer',
        value: 'FullOuter',
        des: 'All records from both',
        icon: 'JoinFull'
      }
    ];
    this.receiveProps(props, {});
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  receiveProps(nextProps, oldProps) {
    // disabledColumnNames is wholly derived from these props, so only recalculate it when one of them has changed
    if (nextProps.columnsInDragArea !== oldProps.columnsInDragArea) {
      this.leftDisabledColumnNames = Immutable.Set(
        nextProps.columnsInDragArea.map((col) =>  col.getIn(['default', 'name']))
      );
      this.rightDisabledColumnNames = Immutable.Set(
        nextProps.columnsInDragArea.map((col) =>  col.getIn(['custom', 'name']))
      );
    }
  }

  getDragPart() {
    const props = {
      addColumn: this.props.addColumnToInnerJoin,
      leftColumns: this.props.leftColumns,
      rightColumns: this.props.rightColumns,
      removeColumn: this.props.removeColumn,
      onDragStart: this.props.onDragStart,
      handleDrop: this.props.handleDrop,
      columnDragName: this.props.columnDragName,
      dragType: this.props.dragType,
      isDragInProgress: this.props.isDragInProgress,
      type: this.props.dragType,
      items: this.props.columnsInDragArea
    };
    if (this.props.columnsInDragArea.size) {
      return (
        <div style={[styles.dragOneWrapStyles]}>
          <JoinDragArea
            dragColumntableType={this.props.dragColumntableType}
            {...props} />
        </div>
      );
    }
    return (
      <div style={[styles.dragWrapStyles]} >
        <JoinDragArea {...props} />
        <JoinDragArea {...props} />
      </div>
    );
  }

  renderCurItem() {
    const { value } = this.props.fields.joinType;
    const curIcon = this.items.find(item => item.value === value || item.label === value).icon || '';
    return (
      <div style={styles.label}>
        <div style={INLINE_NOWRAP_ROW_FLEX_START}>
          <FontIcon type={curIcon}/>
          {value}
        </div>
        <FontIcon type='ArrowDownSmall'/>
      </div>
    );
  }

  render() {
    return (
      <div className='inner-join' style={[styles.base]} onMouseUp={this.props.stopDrag}>
        <div style={styles.wrap}>
          <div style={[styles.item]}>
            <span style={[styles.font, body]}>Type: </span>
            <SelectWithPopover
              dataQa='selectedJoinType'
              items={this.items}
              curItem={this.renderCurItem()}
              onChange={this.props.fields.joinType.onChange}
              styleWrap={{marginTop: 3}}/>
          </div>
        </div>
        <div style={[styles.inner]}>
          <JoinColumnMenu
            type='default'
            columns={this.props.leftColumns}
            disabledColumnNames={this.leftDisabledColumnNames}
            onDragEnd={this.props.stopDrag}
            handleDragStart={this.props.onDragStart}
            dragType={this.props.dragType}
            path={this.props.defaultPath}/>
          {this.getDragPart()}
          <JoinColumnMenu
            type='custom'
            columns={this.props.rightColumns}
            disabledColumnNames={this.rightDisabledColumnNames}
            onDragEnd={this.props.stopDrag}
            handleDragStart={this.props.onDragStart}
            dragType={this.props.dragType}
            path={this.props.customPath}/>
        </div>
        <div style={styles.center}>
          <div style={[styles.add]} onClick={this.props.addEmptyColumnToInnerJoin}>
            <FontIcon type='Add' hoverType='AddHover'/>
            <span style={{marginBottom: 3}}>Add Another Join Condition</span>
          </div>
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    flex: 1,
    display: 'flex',
    minHeight: 180,
    flexWrap: 'wrap',
    justifyContent: 'center',
    backgroundColor: PALE_BLUE
  },
  wrap: {
    width: '100%',
    display: 'flex',
    paddingBottom: 5,
    height: 38,
    backgroundColor: PALE_BLUE
  },
  inner: {
    width: '100%',
    backgroundColor: '#fff',
    justifyContent: 'space-between',
    display: 'flex',
    margin: '3px 10px',
    maxHeight: 180,
    minHeight: 180
  },
  center: {
    width: '100%',
    height: 30,
    display: 'flex',
    margin: '0 10px',
    alignItems: 'center',
    borderBottom: `2px solid ${PALE_GREY}`,
    justifyContent: 'center',
    backgroundColor: PALE_BLUE,
    padding: '0 10px',
    ...body
  },
  add: {
    display: 'flex',
    alignItems: 'center',
    cursor: 'pointer'
  },
  rightMenu: {
    borderLeft: `2px solid ${PALE_GREY}`
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
  item: {
    maxWidth: 235,
    width: 235,
    alignItems: 'center',
    marginLeft: 20,
    fontWeight: 400,
    position: 'relative',
    marginTop: 5,
    display: 'flex',
    flexDirection: 'row',
    justifyContent: 'flex-start'
  },
  font: {
    margin: '0 10px 0 -5px'
  },
  select: {
    padding: 0,
    width: DEFAULT_WIDTH,
    height: 28,
    marginTop: 2,
    marginLeft: 0,
    ...bodySmall
  },
  addJoinStyle: {
    color: '#0096FF',
    fontSize: 13,
    marginBottom: 3,
    marginTop: -5,
    cursor: 'pointer'
  },
  dragArea: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    width: '100%',
    height: 180
  },
  border: {
    borderLeft: '1px solid #ccc'
  },
  dragAreaText: {
    width: 180,
    color: 'gray',
    fontSize: 12,
    textAlign: 'center',
    display: 'inline-block'
  },
  label: {
    display: 'flex',
    flexDirection: 'row',
    flexWrap: 'nowrap',
    justifyContent: 'space-between',
    padding: '0 5px',
    width: '100%',
    ...formDefault
  }
};

export default InnerJoin;
