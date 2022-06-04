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
import { PureComponent } from 'react';
import Radium from 'radium';

import PropTypes from 'prop-types';

@Radium
class ModalFooter extends PureComponent {
  static propTypes = {
    children: PropTypes.node
  }

  render() {
    return (
      <div style={styles.base} className='general-modal-footer'>
        {this.props.children}
      </div>
    );
  }
}

const styles = {
  base: {
    position: 'absolute',
    borderRadius: 3,
    borderTop: '1px solid rgba(0, 0, 0, 0.1)',
    bottom: 0,
    height: 48,
    width: '100%',
    display: 'flex',
    justifyContent: 'flex-end',
    padding: '0 10px 0 0',
    alignItems: 'center'
  }
};

export default ModalFooter;
