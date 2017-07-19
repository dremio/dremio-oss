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
import { PureComponent, PropTypes } from 'react';
import Immutable from 'immutable';
import Radium from 'radium';

import DragColumnMenu from 'components/DragComponents/DragColumnMenu';
import { formLabel } from 'uiTheme/radium/typography';

@Radium
export default class JoinColumnMenu extends PureComponent {
  static propTypes = {
    handleDragStart: PropTypes.func,
    onDragEnd: PropTypes.func,
    dragAreaColumnSize: PropTypes.number,
    dragType: PropTypes.string.isRequired,
    style: PropTypes.object,
    columns: PropTypes.object,
    disabledColumnNames: PropTypes.instanceOf(Immutable.Set),
    type: PropTypes.string,
    path: PropTypes.string
  };

  render() {
    const { columns, disabledColumnNames, handleDragStart, onDragEnd, type } = this.props;
    return (
      <div style={[styles.base]}>
        <div style={styles.titleWrap}>
          <span style={[formLabel, styles.titleText]}>
            Select Fields From {this.props.path}&nbsp;{type === 'default' ? '(Current)' : null}
          </span>
        </div>
        <DragColumnMenu
          style={styles.menu}
          items={columns}
          disabledColumnNames={disabledColumnNames}
          type='column'
          fieldType={type}
          handleDragStart={handleDragStart && handleDragStart.bind(this, this.props.type)}
          onDragEnd={onDragEnd}
          dragType={this.props.dragType}
          name={this.props.path + ' (Current)'}/>
      </div>
    );
  }
}

const styles = {
  base: {

  },
  titleWrap: {
    display: 'flex',
    width: 275,
    position: 'relative',
    justifyContent: 'flex-start',
    padding: '0 10px',
    backgroundColor: '#f3f3f3',
    height: 30,
    alignItems: 'center'
  },
  titleText: {
    overflow: 'hidden',
    textOverflow: 'ellipsis', //can be applied only to block containers https://drafts.csswg.org/css-ui-3/#text-overflow
    whiteSpace: 'nowrap'
  },
  menu: {
    backgroundColor: '#FFFFFF',
    border: '1px solid rgba(0,0,0,0.10)',
    height: 150
  }
};
