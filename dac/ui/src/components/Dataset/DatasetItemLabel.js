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
import { Overlay, Portal } from 'react-overlays';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import FontIcon from 'components/Icon/FontIcon';
import TextHighlight from 'components/TextHighlight';
import EllipsedText from 'components/EllipsedText';
import {EXTRA_POPPER_CONFIG} from 'constants/Constants';
// need this util as MainInfoItemName.js wraps label into a link. If we do not block event bubbling
// redirect would occur
import { stopPropagation } from '@app/utils/reactEventUtils';
import DatasetOverlayContent from './DatasetOverlayContent';

@Radium
@pureRender
export default class DatasetItemLabel extends Component {
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
    shouldShowOverlay: PropTypes.bool
  };

  static defaultProps = {
    fullPath: Immutable.List(),
    showFullPath: false,
    iconSize: 'MEDIUM',
    shouldShowOverlay: true
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
    if (!this.props.shouldShowOverlay) return;
    stopPropagation(evt);
    this.setOpenOverlay();
  };

  renderDefaultNode() {
    let { name, inputValue, fullPath, showFullPath } = this.props;
    const joinedPath = fullPath.slice(0, -1).join('.');

    if (!name && fullPath) {
      name = fullPath.last();
    }

    return (
      <div style={styles.datasetLabel}>
        <EllipsedText text={name} data-qa={name}>
          <TextHighlight text={name} inputValue={inputValue}/>
        </EllipsedText>
        { showFullPath && <EllipsedText text={joinedPath}>
          <TextHighlight text={joinedPath} inputValue={inputValue}/>
        </EllipsedText> }
      </div>
    );
  }

  render() {
    const { fullPath, customNode, typeIcon, isNewQuery, style, iconSize, shouldShowOverlay } = this.props;
    const iconStyle = iconSize === 'LARGE' ? styles.largeIcon : {};
    const labelTypeIcon = iconSize === 'LARGE' ? `${typeIcon}Large` : typeIcon;
    const infoIconStyle = iconSize === 'LARGE' ? { width: 18, height: 18 } : {};
    const node = customNode || this.renderDefaultNode();
    const canShowOverlay = Boolean(!isNewQuery && fullPath.size && shouldShowOverlay);

    const showInfoIcon = canShowOverlay && (this.state.isOpenOverlay || this.state.isIconHovered);

    return (
      <div style={[styles.base, style]}>
        <div
          style={{...styles.iconsBase, ...(showInfoIcon && {cursor: 'pointer'})}}
          onMouseEnter={this.handleMouseEnterIcon}
          onMouseLeave={this.handleMouseLeaveIcon}
          onClick={this.handleClick}
        >
          <FontIcon
            type={labelTypeIcon}
            ref='dataset'
            iconStyle={{...iconStyle, verticalAlign: 'middle', flexShrink: 0, opacity: showInfoIcon ? 0.65 : 1}}
          />
          {showInfoIcon && <FontIcon
            ref='info'
            type='Info'
            theme={{
              ...styles.infoTheme,
              Icon: {
                ...styles.infoTheme.Icon,
                ...infoIconStyle
              }
            }}
          />}
        </div>
        { node }
        {canShowOverlay && this.state.isOpenOverlay && !this.state.isDragInProgress
        && <Portal container={document && document.body}>
          <div style={styles.backdrop} onClick={stopPropagation}>
          </div>
        </Portal>}
        { canShowOverlay && <Overlay
          rootClose
          show={this.state.isOpenOverlay}
          onHide={this.setCloseOverlay}
          container={document && document.body}
          target={() => this.refs.dataset}
          popperConfig={EXTRA_POPPER_CONFIG}
          placement={this.props.placement || 'left'}>
          {
            ({ props: overlayProps }) => <DatasetOverlayContent
              onRef={overlayProps.ref}
              style={overlayProps.style}
              fullPath={fullPath}
              showFullPath
              placement={this.props.placement}
              dragType={this.props.dragType}
              toggleIsDragInProgress={this.toggleIsDragInProgress}
              typeIcon={typeIcon}
              onClose={this.setCloseOverlay}
            />
          }
        </Overlay> }
      </div>
    );
  }
}

const styles = {
  base: {
    width: '100%',
    display: 'flex',
    justifyContent: 'flex-start',
    alignItems: 'center',
    position: 'relative'
  },
  datasetLabel: {
    minWidth: 0,
    marginBottom: 1,
    marginLeft: 5
  },
  largeIcon: {
    width: 40,
    height: 38
  },
  iconsBase: {
    position: 'relative'
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
  }
};
