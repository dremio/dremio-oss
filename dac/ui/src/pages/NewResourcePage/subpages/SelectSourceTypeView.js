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
import { FLEX_START_WRAP } from 'uiTheme/radium/flexStyle';
import { h3, h5White } from 'uiTheme/radium/typography';

import './SelectSourceType.less';

export default class SelectSourceTypeView extends Component {
  static propTypes = {
    popularConnections: PropTypes.array,
    disabledConnections: PropTypes.array,
    showAutocomplete: PropTypes.bool,
    onSelectSource: PropTypes.func.isRequired
  };

  constructor(props) {
    super(props);
    this.renderTiles = this.renderTiles.bind(this);
  }

  renderTiles(connections, className) {
    return connections.map((item) => {
      return (
        <div style={{ position: 'relative' }}>
          <div
            style={styles.source}
            key={item.sourceType} className={`${item.sourceType} ${className}`}
            onClick={this.props.onSelectSource.bind(this, item)}>
            {item.label}
          </div>
          {item.beta && <div style={{...styles.comingBetaLabelWrapper }}><div>{la('beta')}</div></div>}
        </div>
      );
    });
  }

  renderDisabledConnection() {
    return this.props.disabledConnections.map((item) => {
      return (
        <div style={{ position: 'relative' }}>
          <div
            style={{ ...styles.source, ...styles.disabledStyle }}
            key={item.sourceType} className={`${item.sourceType} otherConnect`}>
            {item.label}
          </div>
          <div style={{...styles.comingBetaLabelWrapper}}><div>{la('coming soon')}</div></div>
        </div>
      );
    });
  }

  render() {
    return (
      <div className='select-connection' style={styles.base}>
        <div className='main'>
          <h3>{la('Data Source Types')}</h3>
          <div className='popular' style={FLEX_START_WRAP}>
            { this.renderTiles(this.props.popularConnections, 'popularConnect') }
          </div>
          <div className='popular' style={FLEX_START_WRAP}>
            { this.renderDisabledConnection() }
          </div>
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    margin: '20px 30px 0 30px'
  },
  source: {
    ...h3,
    fontSize: 14,
    marginBottom: 10
  },
  comingBetaLabelWrapper: {
    ...h5White,
    fontSize: 10,
    height: 16,
    position: 'absolute',
    left: '50%',
    bottom: -7,
    transform: 'translate(-50%, -50%)',
    padding: '3px 5px',
    borderRadius: 10,
    background: '#999999',
    display: 'flex',
    textTransform: 'uppercase',
    justifyContent: 'center',
    flexDirection: 'column'
  },
  disabledStyle: {
    height: 68,
    backgroundSize: 60,
    padding: '23px 0 0 90px',
    opacity: 0.5,
    cursor: 'default'
  }
};
