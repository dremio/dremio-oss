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
import Modal from 'components/Modals/Modal';
import Keys from 'constants/Keys.json';

export default class JobProfileModal extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    profileUrl: PropTypes.string
  }

  registerEscape = () => { // DX-5720: when focus is in Profile modal, `esc` doesn't close it
    try {
      this.refs.iframe.contentWindow.addEventListener('keydown', (evt) => {
        if (evt.keyCode === Keys.ESCAPE) { // todo: keyCode deprecated, but no well-supported replacement yet
          this.props.hide();
        }
      });
    } catch (error) { // if the iframe content fails to load, suppress the cross-origin frame access error
      console.error(error);
    }
  }

  render() {
    const { isOpen, hide, profileUrl } = this.props;
    return (
      <Modal
        style={{maxWidth: 'none'}}
        size='large'
        title={la('Job Profile')}
        isOpen={isOpen}
        hide={hide}
      >
        <iframe
          id='profile_frame'
          src={profileUrl}
          style={{ height: '100%', width: '100%', border: 'none' }}
          ref='iframe'
          onLoad={this.registerEscape} />
      </Modal>
    );
  }
}
