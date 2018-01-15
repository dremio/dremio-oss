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
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import Menu from 'material-ui/Menu';

@Radium
@pureRender
class ExploreMenu extends Component {
  static propTypes = {
    children: PropTypes.node
  }

  render() {
    return (
      <Menu
        style={styles.base}
        listStyle={styles.padding}>
        {this.props.children}
      </Menu>
    );
  }
}

const styles = {
  base: {
    position: 'relative',
    minWidth: 110
  },
  padding: {
    paddingTop: 5,
    paddingBottom: 5
  }
};

export default ExploreMenu;
