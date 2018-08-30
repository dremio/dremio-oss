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
import classNames  from 'classnames';

import EllipsedText from 'components/EllipsedText';
import FontIcon from 'components/Icon/FontIcon';
import { unavailable } from 'uiTheme/radium/typography';
import { typeToIconType } from 'constants/DataTypes';
import { constructFullPath } from 'utils/pathUtils';

import DragSource from './DragSource';
import { base, content, disabled as disabledCls } from './ColumnMenuItem.less';

@pureRender
@Radium
export default class ColumnMenuItem extends Component {
  static propTypes = {
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    disabled: PropTypes.bool,
    fullPath: PropTypes.instanceOf(Immutable.List),
    dragType: PropTypes.string.isRequired,
    handleDragStart: PropTypes.func,
    onDragEnd: PropTypes.func,
    type: PropTypes.string,
    index: PropTypes.number,
    nativeDragData: PropTypes.object,
    preventDrag: PropTypes.bool,
    name: PropTypes.string,
    fieldType: PropTypes.string,
    className: PropTypes.string
  }
  static defaultProps = {
    fullPath: Immutable.List()
  }

  checkThatDragAvailable = (e) => {
    if (this.props.preventDrag || this.props.disabled) {
      e.stopPropagation();
      e.preventDefault();
    }
  }

  renderDraggableIcon() {
    return !this.props.preventDrag && !this.props.disabled ? <FontIcon theme={theme.Draggable}
      type='Draggable' class='draggable-icon'/> : null;
  }

  render() {
    const { item, disabled, preventDrag, fieldType, className } = this.props;
    const font = disabled
      ? unavailable
      : {};
    const markAsDisabled = preventDrag || disabled;
    const isGroupBy = this.props.dragType === 'groupBy';
    // full paths are not yet supported by dremio in SELECT clauses, so force this to always be the simple name for now
    const idForDrag = true || // eslint-disable-line no-constant-condition
      isGroupBy ? item.get('name') : constructFullPath(this.props.fullPath.concat(item.get('name')));
    return (
      <div
        className={classNames(['inner-join-left-menu-item', base, className])}
        onMouseDown={this.checkThatDragAvailable}
      >
        <DragSource
          nativeDragData={this.props.nativeDragData}
          dragType={this.props.dragType}
          type={this.props.type}
          index={this.props.index}
          onDragStart={this.props.handleDragStart}
          onDragEnd={this.props.onDragEnd}
          preventDrag={preventDrag}
          isFromAnother
          id={idForDrag}>
          <div
            className={classNames(['draggable-row', content, markAsDisabled && disabledCls])}
            data-qa={`inner-join-field-${item.get('name')}-${fieldType}`}
          >
            <FontIcon type={typeToIconType[item.get('type')]} theme={styles.type}/>
            <EllipsedText style={!preventDrag ? {paddingRight: 10} : {} /* leave space for knurling */}
              text={item.get('name')}>
              <span data-qa={item.get('name')} style={[styles.name, font]}>{item.get('name')}</span>
            </EllipsedText>
            {this.renderDraggableIcon()}
          </div>
        </DragSource>
      </div>
    );
  }
}

const styles = {
  type: {
    'Icon': {
      width: 24,
      height: 20,
      backgroundPosition: 'left center'
    },
    Container: {
      width: 28,
      height: 20,
      top: 0,
      flex: '0 0 auto'
    }
  },
  name: {
    marginLeft: 5
  }
};

const theme = {
  Draggable: {
    Icon: {
      width: 10
    }
  }
};
