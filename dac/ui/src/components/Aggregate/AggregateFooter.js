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

import FontIcon from 'components/Icon/FontIcon';
import { fieldAreaWidth } from './aggregateStyles';

@pureRender
@Radium
class AggregateFooter extends Component {
  static propTypes = {
    addAnother: PropTypes.func,
    style: PropTypes.object
  };

  render() {
    return (
      <div
        className='aggregate-Footer'
        style={[styles.base, this.props.style]}>
        <div style={styles.left}></div>
        <div style={styles.center}>
          <div style={[styles.add]}
            data-qa='add-dimension'
            onClick={this.props.addAnother.bind(this, 'dimensions')}> {/* todo: ax, consistency: button */}
            <FontIcon type='Add' hoverType='AddHover'/>
            <span>{la('Add a Dimension')}</span>
          </div>
        </div>
        <div style={styles.right}>
          <div style={[styles.add]}
            data-qa='add-measure'
            onClick={this.props.addAnother.bind(this, 'measures')}> {/* todo: ax, consistency: button */}
            <FontIcon type='Add' hoverType='AddHover'/>
            <span>{la('Add a Measure')}</span>
          </div>
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    justifyContent: 'space-between',
    width: '100%',
    alignItems: 'center',
    background: 'transparent',
    minHeight: 30
  },
  left: {
    width: fieldAreaWidth,
    minWidth: fieldAreaWidth,
    height: 30,
    display: 'flex',
    alignItems: 'center',
    paddingLeft: 10
  },
  center: {
    width: '100%',
    height: 30,
    display: 'flex',
    alignItems: 'center',
    paddingLeft: 10
  },
  right: {
    height: 30,
    display: 'flex',
    alignItems: 'center',
    paddingLeft: 10,
    width: '100%'
  },
  add: {
    display: 'flex',
    alignItems: 'center',
    cursor: 'pointer'
  }
};

export default AggregateFooter;
