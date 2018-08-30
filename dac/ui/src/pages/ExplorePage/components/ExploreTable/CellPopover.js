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
import Immutable from 'immutable';
import $ from 'jquery';
import classNames from 'classnames';
import result from 'lodash/result';

import JSONTree from 'react-json-tree';
import { FLEX_COL_START } from 'uiTheme/radium/flexStyle';
import { LIST, MAP } from 'constants/DataTypes';
import exploreUtils from 'utils/explore/exploreUtils';
import SelectedTextPopover from './SelectedTextPopover';
import getTheme from './themeTreeMap';

import './CellPopover.less';

@pureRender
@Radium
export default class CellPopover extends Component {
  static propTypes = {
    availibleActions: PropTypes.array,
    cellPopover: PropTypes.instanceOf(Immutable.Map),
    data: PropTypes.string,
    location: PropTypes.object,
    hideCellPopover: PropTypes.func,
    treeHeight: PropTypes.number,
    openPopover: PropTypes.bool,
    isDumbTable: PropTypes.bool,
    hide: PropTypes.func,
    onCurrentPathChange: PropTypes.func,
    onSelectMenuVisibleChange: PropTypes.func
  };

  static contextTypes = {
    router: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
    this.hideDrop = this.hideDrop.bind(this);
    this.selectItem = this.selectItem.bind(this);
    this.renderLabel = this.renderLabel.bind(this);
    this.data = JSON.parse(props.data);
  }

  componentDidMount() {
    this.clickHandler = (e) => {
      const $element = $(e.target);
      if ($element.closest('.cell-popover-wrap').length > 0) {
        return null;
      }

      if (!$element.hasClass('table-control-wrap')) {
        this.props.hideCellPopover();
      }
    };
    // TODO: change on better desion
    $('#grid-page').on('click.popover', this.clickHandler);
  }

  getMapModel(keyPath) {
    const mapPathList = keyPath.slice().reverse();
    // we need concat item of list with parent like "list[indexOfList]"
    const path = mapPathList.reduce((prev, cur, curIndex) => {
      if (typeof cur === 'number') {
        prev[prev.length - 1] = prev[prev.length - 1] + `[${cur}]`;
      } else {
        prev.push(cur);
      }
      return prev;
    }, []);

    return Immutable.fromJS({
      columnName: this.props.cellPopover.get('columnName'),
      mapPathList: path
    });
  }

  getListModel(keyPath) {
    const index = keyPath[0];
    return Immutable.fromJS({
      cellText: this.props.data,
      columnName: this.props.cellPopover.get('columnName'),
      startIndex: index,
      endIndex: index + 1,
      listLength: this.data.length
    });
  }

  getMapValueFromSelection() {
    const { keyPath } = this.state;
    const key = keyPath[keyPath.length - 1];
    const label = $.isNumeric(key) ? '' : `${key}:`;
    return `${label}${JSON.stringify(result(this.data, keyPath.join('.')))}`;
  }

  hideDrop() {
    this.setState({anchor: null});
    if (this.props.onSelectMenuVisibleChange) {
      this.props.onSelectMenuVisibleChange(false);
    }
    if (this.props.hide) {
      this.props.hide();
    }
  }

  handleMouseOver = (e, keyPath) => {
    if (this.props.onCurrentPathChange && !this.props.isDumbTable) {
      this.props.onCurrentPathChange(keyPath && keyPath.slice().reverse().join('.'));
    }
  }

  selectItem(e, keyPath) {
    if (e.target.tagName === 'DIV' || this.props.isDumbTable) {
      return;
    }

    this.setState({ anchor: e.target, keyPath: keyPath.slice().reverse() });
    if (this.props.onSelectMenuVisibleChange) {
      this.props.onSelectMenuVisibleChange(true);
    }

    const { location, cellPopover } = this.props;
    const columnType = cellPopover.get('columnType');
    const model = columnType === LIST ? this.getListModel(keyPath) : this.getMapModel(keyPath);
    const query = {
      ...location.query,
      column: cellPopover.get('columnName'),
      columnType
    };
    const state = {
      ...location.state,
      selection: model
    };
    this.context.router.push({ pathname: this.props.location.pathname, query, state });
  }

  shouldToggleExpand(e) {
    return e.target.tagName === 'DIV';
  }

  shouldExpandNode() {
    return true;
  }

  copySelection = () => {
    const selection = this.getMapValueFromSelection();
    exploreUtils.copySelection(selection);
    this.hideDrop();
  }

  renderLabel(keyPath) {
    const {isDumbTable} = this.props;
    return (
      <span
        className='label_node'
        style={isDumbTable && {cursor: 'default'}}
      >
        {$.isNumeric(keyPath[0]) ? '' : `${keyPath[0]}:`}
      </span>
    );
  }

  render() {
    const { cellPopover, isDumbTable } = this.props;
    const className = classNames('large_overlay_tree',
      { 'overlay-array': cellPopover.get('columnType') === LIST },
      { 'overlay-object': cellPopover.get('columnType') === MAP }
    );
    return (
      <div  className='cell-popover-wrap' ref='cellPopover'>
        <div style={[styles.selectedDrop, { maxHeight: this.props.treeHeight || 300 }]}>
          <div style={FLEX_COL_START} className={className}>
            <JSONTree
              getItemString={() => <span/>}
              theme={getTheme(!isDumbTable)}
              invertTheme={false}
              data={this.data}
              labelRenderer={this.renderLabel}
              onNodeClick={this.selectItem}
              onMouseOver={this.handleMouseOver}
              shouldToggleExpand={this.shouldToggleExpand}
              shouldExpandNode={this.shouldExpandNode}
              maxClickableNodeDepth={Array.isArray(this.data) && 1 || null}
              hideRoot/>
          </div>
          <SelectedTextPopover
            copySelection={this.copySelection}
            visibleItems={this.props.availibleActions}
            anchor={this.state.anchor}
            hideDrop={this.hideDrop}
            columnType={this.props.cellPopover.get('columnType')}
            columnName={this.props.cellPopover.get('columnName')}/>
        </div>
      </div>
    );
  }
}

const styles = {
  selectedDrop: {
    background: '#FFF',
    listStyle: 'none',
    margin: 0,
    border: '1px solid #e9e9e9',
    borderRadius: '2px',
    boxShadow: '0 0 5px rgba(0,0,0,.1)',
    overflowX: 'auto'
  },
  value: {
  },
  checkbox: {
    marginLeft: 5,
    width: 12,
    height: 12
  }
};
