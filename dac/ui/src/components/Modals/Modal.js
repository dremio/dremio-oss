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
import PropTypes from 'prop-types';
import ReactModal from 'react-modal';
import ReactDOMServer from 'react-dom/server';

import {smallModal, mediumModal, largeModal, tallModal, smallestModal, modalBody} from 'uiTheme/radium/modal';

import ModalHeader from './ModalHeader';
import './Modals.less';

export const ModalSize = {
  small: 'small',
  large: 'large',
  tall: 'tall',
  medium: 'medium',
  smallest: 'smallest'
};

export default class Modal extends Component {

  static propTypes = {
    size: PropTypes.oneOf(Object.values(ModalSize)).isRequired,
    isOpen: PropTypes.bool,
    hideCloseButton: PropTypes.bool,
    onClickCloseButton: PropTypes.func, // optional. defaults to props.hide
    hide: PropTypes.func,
    children: PropTypes.node,
    title: PropTypes.node.isRequired,
    modalHeight: PropTypes.string,
    className: PropTypes.string,
    classQa: PropTypes.string,
    style: PropTypes.object,
    dataQa: PropTypes.string
  };

  static defaultProps = {
    title: '',
    classQa: '',
    hideCloseButton: false
  };

  render() {
    const {
      size, isOpen, title, hide, hideCloseButton, onClickCloseButton, className, children, style, classQa, dataQa
    } = this.props;
    const content = {
      ...smallModal.content,
      height: this.props.modalHeight || smallModal.content.height,
      ...style
    };
    const smallModalUpdated = { ...smallModal, content };
    const mediumModalUpdated = { ...mediumModal, content: { ...mediumModal.content, ...style}};
    const largeModalUpdated = { ...largeModal, content: { ...largeModal.content, ...style}};
    const tallModalUpdated = { ...tallModal, content: { ...tallModal.content, ...style}};
    const styles = {
      [ModalSize.small]: smallModalUpdated,
      [ModalSize.medium]: mediumModalUpdated,
      [ModalSize.large]: largeModalUpdated,
      [ModalSize.tall]: tallModalUpdated,
      [ModalSize.smallest]: smallestModal
    };

    let stringTitle = title;
    if (typeof stringTitle === 'object') {
      const html = ReactDOMServer.renderToStaticMarkup(stringTitle);
      const tmp = document.createElement('div');
      tmp.innerHTML = html;
      stringTitle = tmp.textContent;
    }

    return (
      <ReactModal
        contentLabel={stringTitle}
        overlayClassName={`dremio-modal ${size}-modal qa-${classQa}`}
        isOpen={isOpen}
        onRequestClose={hide}
        style={styles[size]}
        closeTimeoutMS={150}
        className={className}
      >
        {stringTitle ?
          <ModalHeader title={stringTitle} hide={onClickCloseButton || hide} hideCloseButton={hideCloseButton}/> : null}
        <div style={modalBody} data-qa={dataQa}>
          {children}
        </div>
      </ReactModal>
    );
  }
}
