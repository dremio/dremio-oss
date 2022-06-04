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
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import FontIcon from 'components/Icon/FontIcon';
import TextHighlight from 'components/TextHighlight';
import EllipsedText from 'components/EllipsedText';
// need this util as MainInfoItemName.js wraps label into a link. If we do not block event bubbling
// redirect would occur
import { stopPropagation } from '@app/utils/reactEventUtils';
import Art from '../Art';
import DatasetOverlayContent from './DatasetOverlayContent';

import './DatasetItemLabel.less';

@Radium
export default class DatasetItemLabel extends PureComponent {
  static propTypes = {
    name: PropTypes.string, // defaults to last token of fullPath
    inputValue: PropTypes.string,
    fullPath: PropTypes.instanceOf(Immutable.List),
    showFullPath: PropTypes.bool,
    isNewQuery: PropTypes.bool,
    placement: PropTypes.string,
    customNode: PropTypes.node,
    dragType: PropTypes.string,
    typeIcon: PropTypes.string.isRequired,
    iconSize: PropTypes.oneOf(['MEDIUM', 'LARGE']),
    style: PropTypes.object,
    shouldShowOverlay: PropTypes.bool,
    shouldAllowAdd: PropTypes.bool,
    addtoEditor: PropTypes.func,
    isExpandable: PropTypes.bool,
    className: PropTypes.string
  };

  static defaultProps = {
    fullPath: Immutable.List(),
    showFullPath: false,
    iconSize: 'MEDIUM',
    shouldShowOverlay: false
  };

  state = {
    isOpenOverlay: false,
    isIconHovered: false,
    isDragInProgress: false
  }

  setOpenOverlay = () => this.setState({ isOpenOverlay: true });

  setCloseOverlay = () => this.setState({ isOpenOverlay: false });

  handleMouseEnterIcon = () => {
    this.setState({ isIconHovered: true });
  };
  toggleIsDragInProgress = () => {
    this.setState((prevState) => ({ isDragInProgress: !prevState.isDragInProgress }));
  };

  handleMouseLeaveIcon = () => {
    this.setState({ isIconHovered: false });
  };

  handleClick = (evt) => {
    if (!this.props.shouldShowOverlay || !this.props.isExpandable) return;
    stopPropagation(evt);
    this.setState((prevState) => ({ isOpenOverlay: !prevState.isOpenOverlay }));
  };

  renderDefaultNode() {
    let { name, inputValue, fullPath, showFullPath } = this.props;
    const joinedPath = fullPath.slice(0, -1).join('.');

    if (!name && fullPath) {
      name = fullPath.last();
    }

    return (
      <div style={styles.datasetLabel}>
        <EllipsedText text={name} data-qa={name} className='heading'>
          <TextHighlight text={name} inputValue={inputValue}/>
        </EllipsedText>
        { showFullPath && <EllipsedText text={joinedPath} className='heading2'>
          <TextHighlight text={joinedPath} inputValue={inputValue}/>
        </EllipsedText> }
      </div>
    );
  }

  render() {
    const { fullPath,
      customNode,
      typeIcon,
      isNewQuery,
      style,
      iconSize,
      shouldShowOverlay,
      isExpandable,
      className,
      shouldAllowAdd,
      addtoEditor
    } = this.props;

    const iconStyle = iconSize === 'LARGE' ? styles.largeIcon : {};
    const labelTypeIcon = iconSize === 'LARGE' ? `${typeIcon}Large` : typeIcon;
    const node = customNode || this.renderDefaultNode();
    const canShowOverlay = Boolean(!isNewQuery && fullPath.size && shouldShowOverlay);
    const arrowIconType = this.state.isOpenOverlay ? 'ArrowDown' : 'ArrowRight';
    const showInfoIcon = canShowOverlay && (this.state.isOpenOverlay || this.state.isIconHovered);

    return (
      <div style={[styles.base, style]} className={className}>

        <div style={styles.list} className='datasetItemLabel'>
          <div className='datasetItemLabel-item'>
            <div
              data-qa='info-icon'
              style={{...styles.iconsBase, ...(showInfoIcon && isExpandable && {cursor: 'pointer'})}}
              onMouseEnter={this.handleMouseEnterIcon}
              onMouseLeave={this.handleMouseLeaveIcon}
              onClick={this.handleClick}
              className='datasetItemLabel-item__content'
            >
              { isExpandable &&
                <Art src={`${arrowIconType}.svg`} alt='' style={{ height: 24, width: 24 }} />
              }
              <FontIcon
                type={labelTypeIcon}
                ref='dataset'
                iconStyle={{...iconStyle, verticalAlign: 'middle', flexShrink: 0 }}
              />
              { node }
            </div>
            {
              shouldAllowAdd &&
                <Art
                  src='CirclePlus.svg'
                  alt=''
                  className='datasetItemLabel-item__add'
                  onClick={() => addtoEditor(fullPath)}
                  title='Add to SQL editor'
                />
            }
          </div>

          { this.state.isOpenOverlay &&
            <DatasetOverlayContent
              fullPath={fullPath}
              showFullPath
              placement={this.props.placement}
              dragType={this.props.dragType}
              toggleIsDragInProgress={this.toggleIsDragInProgress}
              typeIcon={typeIcon}
              onClose={this.setCloseOverlay}
              shouldAllowAdd={shouldAllowAdd}
              addtoEditor={addtoEditor}
            />
          }
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    width: '100%'
  },
  datasetLabel: {
    minWidth: 0,
    marginBottom: 1,
    marginLeft: 5,
    paddingright: 10,
    flexGrow: 1
  },
  largeIcon: {
    width: 40,
    height: 38
  },
  iconsBase: {
    position: 'relative',
    display: 'flex',
    alignItems: 'center',
    overflow: 'hidden'
  },
  backdrop: {
    position: 'fixed',
    top: 0,
    bottom: 0,
    right: 0,
    left: 0,
    height: '100%',
    width: '100%',
    zIndex: 1000
  },
  infoTheme: {
    Icon: {
      width: 12,
      height: 12
    },
    Container: {
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      width: 24,
      height: 24,
      position: 'absolute',
      left: 0,
      top: 0,
      bottom: 0,
      right: 0,
      margin: 'auto'
    }
  },
  list: {
    display: 'flex',
    flexDirection: 'column'
  }
};
