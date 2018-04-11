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
import { Link } from 'react-router';
import Radium from 'radium';

import invariant from 'invariant';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import * as buttonStyles from 'uiTheme/radium/buttons';

@pureRender
export default class LinkButton extends Component {

  static propTypes = {
    buttonStyle: PropTypes.string,
    style: PropTypes.object,
    children: PropTypes.node
  };

  static defaultProps = {
    buttonStyle: 'secondary'
  };

  render() {
    const {buttonStyle, ...linkProps} = this.props;
    const {style, children} = linkProps;
    invariant(buttonStyles[buttonStyle] !== undefined, `Unknown button style: "${buttonStyle}"`);

    const RadiumLink = Radium(Link);
    return (
      <RadiumLink {...linkProps} style={[styles.base, buttonStyles[buttonStyle], style]}>
        {children}
      </RadiumLink>
    );
  }
}

const styles = {
  base: {
    lineHeight: '27px',
    textDecoration: 'none'
  }
};
