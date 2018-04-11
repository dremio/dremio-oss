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
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

// todo: FileFormat and AccessControl do not use this. really want a `.modal > h2` stylesheet

@pureRender
@Radium
export default class FormTitle extends Component {
  static propTypes = {
    style: PropTypes.object,
    children: PropTypes.node
  }

  render() {
    return <h2 style={[this.props.style, styles.title]}>{this.props.children}</h2>;
  }
}

const styles = {
  title: {
    borderBottom: '1px solid rgba(0,0,0,0.10)',
    margin: '10px 0',
    paddingBottom: 10
  }
};
