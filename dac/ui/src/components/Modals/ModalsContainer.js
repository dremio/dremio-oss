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
import React, { PureComponent, Fragment } from 'react';

import PropTypes from 'prop-types';
import { withLocation } from 'containers/dremioLocation';
import { MODAL_CLOSE_ANIMATION_DURATION } from '@app/components/Modals/Modal';

class ModalsContainer extends PureComponent {

  static contextTypes = {
    router: PropTypes.object.isRequired
  }

  static propTypes = {
    bodyClassName: PropTypes.string,
    modals: PropTypes.object,
    children: PropTypes.node,
    location: PropTypes.object.isRequired
  }

  componentWillUnmount() {
    $(document.body).removeClass(this.props.bodyClassName);
    clearTimeout(this.lastModalHideTimerId);
  }

  lastModalHideTimerId = null;
  lastModalKey = null;
  handleHide = () => {
    const {
      // Skip modal and query and add retain rest of the properties
      modal, // eslint-disable-line @typescript-eslint/no-unused-vars
      query, // eslint-disable-line @typescript-eslint/no-unused-vars
      ...otherState
    } = this.props.location.state || {};
    this.context.router.replace({...this.props.location, state: { ...otherState }});
    this.lastModalHideTimerId = setTimeout(() => {
      this.lastModalKey = null;
      this.lastModalHideTimerId = null;
      // update a component to make sure that a modal for 'lastModalKey' is unmounted
      this.forceUpdate();
    }, MODAL_CLOSE_ANIMATION_DURATION);
  }

  renderModals() {
    const { bodyClassName, location} = this.props;
    const { modal } = location.state || {};

    //TODO use body class from react-modal when this issue is fixed
    //https://github.com/reactjs/react-modal/issues/99
    if (modal) {
      $(document.body).addClass(bodyClassName);
    } else {
      $(document.body).removeClass(bodyClassName);
    }

    // cache a previous value before update
    const lastModalKey = this.lastModalKey;
    if (modal) { // set a new prev key only if we get a not empty modal
      this.lastModalKey = modal;
    }
    return this.renderModalByKey(modal || lastModalKey); // we need render a prev modal to let it finish close animation
  }

  renderModalByKey = key => {
    const { modals, location } = this.props;
    const { modal, query, ...state} = location.state || {};

    return modals[key] && React.createElement(modals[key], {
      key, isOpen: modal === key, hide: this.handleHide, location, pathname: location.pathname,
      query: query || {}, ...state});
  }

  render() {
    const { children } = this.props;
    return (
      <Fragment>
        {this.renderModals()}
        {children}
      </Fragment>
    );
  }
}

export default withLocation(ModalsContainer);
