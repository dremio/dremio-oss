/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import Radium from 'radium';
import Immutable from 'immutable';
import classNames  from 'classnames';
import { injectIntl } from 'react-intl';

import EllipsedText from 'components/EllipsedText';
import FontIcon from 'components/Icon/FontIcon';
import { typeToIconType } from '@app/constants/DataTypes';
import { constructFullPath } from 'utils/pathUtils';

import Art from '@app/components/Art';
import { itemContainer, base, content, disabled as disabledCls, icon as iconCls, datasetAdd } from '@app/uiTheme/less/DragComponents/ColumnMenuItem.less';
import DragSource from './DragSource';

@Radium
class ColumnMenuItem extends PureComponent {
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
    className: PropTypes.string,
    intl: PropTypes.any,
    shouldAllowAdd: PropTypes.bool,
    addtoEditor: PropTypes.func
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

  renderDraggableIcon() { // DX-37793: This currently built with wrong icon. Correct icon has not been designed yet.
    return !this.props.preventDrag && !this.props.disabled ?
      (
        <div style={{position: 'relative', width: 0, height: 0}}>
          <FontIcon type='DropdownEnabled' class='column-draggable-icon'/>
          <FontIcon type='DropdownDisabled' class='column-draggable-icon--static'/>
        </div>
      ) : null;
  }

  render() {
    const { item, disabled, preventDrag, fieldType, className, shouldAllowAdd, addtoEditor, intl: { formatMessage } } = this.props;
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
            <div className={itemContainer}>
              <EllipsedText data-qa={item.get('name')} style={!preventDrag ? {paddingRight: 10} : {} /* leave space for knurling */}
                text={item.get('name')} title={preventDrag ? formatMessage({ id: 'Read.Only'}) : item.get('name')}>
                {item.get('name')}
              </EllipsedText>
            </div>
            {/*
              We need to put sort and partition icons to columns, i.e. ('S' and 'P' below represents
              the icons):
              col1  S |P
              col2  S |
              col3    |P
              col4  S |P
            */}
            {
              item.get('isSorted') && <Art dataQa='is-partitioned'
                src='sorted.svg'
                alt='sorted'
                title='sorted'
                className={iconCls}
              />
            }
            {
              item.get('isPartitioned') && <Art dataQa='is-partitioned'
                src='Partition.svg'
                alt='partitioned'
                title='partitioned'
                className={iconCls}
              />
            }
            {
              // need to add a empty placeholder for partition icon to keep alignment
              item.get('isSorted') && !item.get('isPartitioned') && <div className={iconCls}></div>
            }
            {/* {this.renderDraggableIcon()} */} {/* DX-37793: This currently built with wrong icon. Correct icon has not been designed yet. */}

            {
              shouldAllowAdd &&
                <Art
                  src='CirclePlus.svg'
                  alt=''
                  className={datasetAdd}
                  onClick={() => addtoEditor(item.get('name'))}
                  title='Add to SQL editor'
                />
            }
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

export default injectIntl(ColumnMenuItem);
