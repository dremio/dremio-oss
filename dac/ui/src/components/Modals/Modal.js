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
import { Component } from 'react';
import PropTypes from 'prop-types';
import ReactModal from 'react-modal';

import {smallModal, largeModal, smallestModal, modalBody} from 'uiTheme/radium/modal';

import ModalHeader from './ModalHeader';
import './Modals.less';

export default class Modal extends Component {

  static propTypes = {
    size: PropTypes.oneOf(['small', 'large', 'smallest']).isRequired,
    isOpen: PropTypes.bool,
    hideCloseButton: PropTypes.bool,
    onClickCloseButton: PropTypes.func, // optional. defaults to props.hide
    hide: PropTypes.func,
    children: PropTypes.node,
    title: PropTypes.string,
    modalHeight: PropTypes.string,
    className: PropTypes.string,
    classQa: PropTypes.string,
    style: PropTypes.object
  };

  static defaultProps = {
    title: '',
    classQa: '',
    hideCloseButton: false
  };

  render() {
    const {
      size, isOpen, title, hide, hideCloseButton, onClickCloseButton, className, children, style, classQa
    } = this.props;
    const content = {
      ...smallModal.content,
      height: this.props.modalHeight || smallModal.content.height,
      ...style
    };
    const smallModalUpdated = { ...smallModal, content };
    const largeModalUpdated = { ...largeModal, content: { ...largeModal.content, ...style}};
    const styles = {small: smallModalUpdated, large: largeModalUpdated, smallest: smallestModal};

    return (
      <ReactModal
        contentLabel={title}
        overlayClassName={`${size}-modal qa-${classQa}`}
        isOpen={isOpen}
        onRequestClose={hide}
        style={styles[size]}
        closeTimeoutMS={150}
        className={className}
      >
        {title ?
          <ModalHeader title={title} hide={onClickCloseButton || hide} hideCloseButton={hideCloseButton}/> : null}
        <div style={modalBody}>
          {children}
        </div>
      </ReactModal>
    );
  }
}
