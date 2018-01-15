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
import React, { Component } from 'react';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

@pureRender
export default class ModalsContainer extends Component {

  static contextTypes = {
    router: PropTypes.object.isRequired,
    location: PropTypes.object.isRequired
  }

  static propTypes = {
    bodyClassName: PropTypes.string,
    modals: PropTypes.object,
    children: PropTypes.node,
    style: PropTypes.object
  }

  state = {
    shown: new Set() // can't hard-remove once added thanks to transitions out
  }

  componentWillUnmount() {
    $(document.body).removeClass(this.props.bodyClassName);
  }

  handleHide = () => {
    this.context.router.replace({...this.context.location, state: {}});
  }

  renderModals() {
    const {bodyClassName, modals} = this.props;
    const {location} = this.context;
    const {modal, query, ...state} = location.state || {};

    modal && this.state.shown.add(modal);

    const result = [];
    for (const key in modals) {
      // lazily create (faster page mount/unmount)
      this.state.shown.has(key) && result.push(React.createElement(modals[key], {
        key, isOpen: modal === key, hide: this.handleHide, location, pathname: location.pathname,
        query: query || {}, ...state}));
    }

    //TODO use body class from react-modal when this issue is fixed
    //https://github.com/reactjs/react-modal/issues/99
    if (modal) {
      $(document.body).addClass(bodyClassName);
    } else {
      $(document.body).removeClass(bodyClassName);
    }
    return result;
  }

  render() {
    const { style, children } = this.props;
    return (
      <div style={{height: '100%', ...style}}>
        {this.renderModals()}
        {children}
      </div>
    );
  }
}
