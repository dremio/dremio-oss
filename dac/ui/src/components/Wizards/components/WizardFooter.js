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
import { Component, PropTypes } from 'react';
import Radium from 'radium';
import SampleDataMessage from 'pages/ExplorePage/components/SampleDataMessage';

@Radium
export default class WizardFooter extends Component {
  static propTypes = {
    children: PropTypes.node,
    style: PropTypes.object
  }

  renderPreviewWarning() {
    return (
      <SampleDataMessage />
    );
  }

  render() {
    return (
      <div className='wizard-footer' style={[styles.base, this.props.style]}>
        <div style={styles.topBorder}/>
        <div style={[styles.buttons]}>
          {this.props.children}
        </div>
        <div style={styles.warning}>
          {this.renderPreviewWarning()}
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    alignItems: 'center',
    position: 'relative',
    width: '100%',
    padding: '0 4px',
    backgroundColor: '#F5FCFF',
    borderTop: '1px solid rgba(0,0,0,0.05)'
  },
  buttons: {
    paddingTop: '10px',
    paddingBottom: '10px',
    display: 'flex',
    alignItems: 'center'
  },
  topBorder: {
    display: 'block',
    borderTop: '1px solid rgba(0,0,0,0.05)',
    margin: 0
  },
  warning: {
    margin: '5px 10px 0 5px'
  },
  warntext: {
    display: 'inline-flex',
    alignItems: 'center'
  }
};
