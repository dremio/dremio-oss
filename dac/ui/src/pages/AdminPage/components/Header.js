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
import { Component } from 'react';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import { createSelector } from 'reselect';

// todo: discuss: could just be CSS - no need for per-tab `import`?

@Radium
@pureRender
class Header extends Component {

  static propTypes = {
    title: PropTypes.string,
    children: PropTypes.node,
    endChildren: PropTypes.node,
    style: PropTypes.object,
    titleStyle: PropTypes.object
  };

  static defaultProps = {
    titleStyle: {}
  };

  getStyle = createSelector(style => style, style => ({ ...styles.adminHeader, ...style }));

  render() {
    const { title, children, endChildren, style, titleStyle} = this.props;

    return (
      <header style={this.getStyle(style)}>
        <h3 style={titleStyle}>{title || children}</h3>
        <div>
          {endChildren}
        </div>
      </header>
    );
  }
}

export default Header;

const styles = {
  adminHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    borderBottom: '1px solid rgba(0,0,0,.1)',
    padding: '10px 0',
    minheight: 48,
    flexShrink: 0
  }
};
