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
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

@PureRender
@Radium
class ModalFooter extends Component {
  static propTypes = {
    styles: PropTypes.object,
    children: PropTypes.node
  }

  render() {
    const {styles} = this.props;
    const baseStyles = [style.base];
    if (styles) {
      baseStyles.push(...styles);
    }
    return (
      <div style={baseStyles} className='general-modal-footer'>
        {this.props.children}
      </div>
    );
  }
}

const style = {
  'base': {
    'position': 'absolute',
    borderRadius: 3,
    'bottom': 0,
    'height': 48,
    'width': '100%',
    'display': 'flex',
    'justifyContent': 'flex-end',
    'padding': '0 10px 0 0',
    'alignItems': 'center',
    backgroundColor: '#F3F3F3'
  }
};

export default ModalFooter;
