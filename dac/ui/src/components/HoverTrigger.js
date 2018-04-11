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

/**
 * Use this component for hover handling. This component implements workaround for onMouseLeave bug
 * doesn't fires sometimes on disabled elements.
 * Check https://github.com/facebook/react/issues/4251 for more details.
 */
export default class HoverTrigger extends Component {
  static propTypes = {
    onEnter: PropTypes.func,
    onLeave: PropTypes.func,
    children: PropTypes.node,
    style: PropTypes.object
  }

  hoverItem = null;

  componentDidMount() {
    this.watchMouseLeave();
  }

  componentDidUpdate() {
    this.watchMouseLeave();
  }

  componentWillUnmount() {
    this.hoverItem.removeEventListener('mouseleave', this.handleMouseLeave);
  }

  watchMouseLeave = () => {
    this.hoverItem.addEventListener('mouseleave', this.handleMouseLeave);
  }

  handleMouseLeave = (e) => {
    this.props.onLeave(e);
  }

  render() {
    return (
      <div
        style={this.props.style}
        onMouseEnter={this.props.onEnter}
        ref={(hoverItem) => this.hoverItem = hoverItem}>
        {this.props.children}
      </div>
    );
  }
}
