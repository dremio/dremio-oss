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
import { PureComponent, createRef, Fragment } from 'react';
import { Popover, MouseEvents } from '@app/components/Popover';
import classNames from 'classnames';

import PropTypes from 'prop-types';

import FontIcon from 'components/Icon/FontIcon';
import { select as selectCls, disabled as disabledCls } from './SelectView.less';

/**
 * node or render props function
 * renderProps: ({ openDD: func, closeDD: func, isOpen: func }) => {@see PropTypes.node}
 * {see SelectView#renderNodeOrProps} for details
 */
const nodeOrRenderProps = PropTypes.oneOfType([PropTypes.node, PropTypes.func]);

export class SelectView extends PureComponent {
  static propTypes = {
    content: nodeOrRenderProps,
    children: nodeOrRenderProps,
    /** If true, an icon with an arrow is hidden */
    hideExpandIcon: PropTypes.bool,
    disabled: PropTypes.bool,
    className: PropTypes.string,
    style: PropTypes.object,
    dataQa: PropTypes.string,
    // #region list properties ------------------------------------
    listClass: PropTypes.string,
    listStyle: PropTypes.object,
    /** set to true if you want to align a popover to the right side of the content */
    listRightAligned: PropTypes.bool,
    listWidthSameAsAnchorEl: PropTypes.bool,
    // #endregion list properties ---------------------------------
    beforeOpen: PropTypes.func,
    beforeClose: PropTypes.func,
    useLayerForClickAway: PropTypes.bool,
    /** Attributes that would be applied to a content wrapper dom element */
    rootAttrs: PropTypes.object
  };

  static defaultProps = {
    useLayerForClickAway: true
  }

  state = {
    anchorEl: null
  };
  contentRef = createRef();

  renderNodeOrProps = nodeOrProps => {
    if (typeof nodeOrProps === 'function') {
      return nodeOrProps({
        openDD: this.openDD,
        closeDD: this.closeDD,
        isOpen: this.isOpen
      });
    }
    return nodeOrProps;
  }

  openDD = (e) => {
    const { beforeOpen, disabled } = this.props;

    if (disabled) {
      return;
    }
    if (beforeOpen) {
      beforeOpen();
    }
    this.setState({
      anchorEl: this.contentRef.current
    });
  }

  closeDD = () => {
    if (this.props.beforeClose) {
      this.props.beforeClose();
    }
    this.setState({
      anchorEl: null
    });
  }

  isOpen = () => !this.props.disabled && Boolean(this.state.anchorEl)

  render() {
    const {
      content,
      children,
      className,
      dataQa,
      hideExpandIcon,
      disabled,
      listClass,
      style,
      listRightAligned,
      useLayerForClickAway,
      listWidthSameAsAnchorEl,
      listStyle,
      rootAttrs
    } = this.props;
    const { anchorEl } = this.state;
    const open = this.isOpen();

    return (
      <Fragment>
        <div
          onClick={open ? null : this.openDD}
          ref={this.contentRef}
          className={classNames({
            [selectCls]: true,
            [disabledCls]: disabled,
            [className]: true
          })}
          style={style}
          data-qa={dataQa}
          {...rootAttrs}
        >
          {this.renderNodeOrProps(content)}
          {!hideExpandIcon && <FontIcon type='ArrowDownSmall' theme={styles.arrow}/>}
        </div>
        <Popover
          anchorEl={open ? anchorEl : null}
          listRightAligned={listRightAligned}
          onClose={this.closeDD}
          dataQa='convertTo' // todo change that
          listStyle={listStyle}
          listClass={listClass}
          useLayerForClickAway={useLayerForClickAway}
          clickAwayMouseEvent={MouseEvents.onClick}
          listWidthSameAsAnchorEl={listWidthSameAsAnchorEl}
        >
          {this.renderNodeOrProps(children)}
        </Popover>
      </Fragment>
    );
  }
}

const styles = {
  arrow: {
    Container: {
      // reset line height for icon
      lineHeight: 1
    }
  }
};

