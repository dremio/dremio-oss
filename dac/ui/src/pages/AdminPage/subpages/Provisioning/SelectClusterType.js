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
import Immutable from 'immutable';

import FontIcon from 'components/Icon/FontIcon';
import { DIVIDER } from 'uiTheme/radium/colors';
import { h4, h3, metadataWhite } from 'uiTheme/radium/typography';

@Radium
export default class SelectClusterType extends Component {

  static propTypes = {
    style: PropTypes.object,
    clusters: PropTypes.instanceOf(Immutable.List),
    onSelectClusterType: PropTypes.func
  };

  onSelectClusterType = (cluster) => {
    if (!cluster.get('connected')) {
      return;
    }
    this.props.onSelectClusterType(cluster.get('clusterType'));
  };

  renderCluster(cluster) {
    const isActive = cluster.get('connected');
    return (
      <div style={styles.clusterStyleWrapper}
        key={cluster.get('clusterType')}
        onClick={this.onSelectClusterType.bind(this, cluster)}>
        <div key={cluster.get('clusterType')} style={[styles.clusterStyle, isActive && styles.clusterStyleActive]}>
          <FontIcon type={cluster.get('iconType')}
            style={{height: 48}}
            iconStyle={styles.iconStyle}/>
          <h3 style={[h4, {paddingLeft: 10}]}>{cluster.get('label')}</h3>
        </div>
        {!isActive
            ? <div style={[metadataWhite, styles.comingLabel]}>{la('COMING SOON')}</div>
            : null}
      </div>
    );
  }

  render() {
    const { clusters, style } = this.props;
    return (
      <div style={style}>
        <div style={{maxWidth: 720}}>{clusters.map((cluster) => this.renderCluster(cluster))}</div>
      </div>
    );
  }
}

const styles = {
  comingLabel: {
    borderRadius: 10,
    background: 'black',
    left: 70,
    bottom: '-10px',
    position: 'absolute',
    padding: '0 5px'
  },
  clusterStyleWrapper: {
    position: 'relative',
    float: 'left',
    width: 230,
    height: 68,
    margin: '15px 10px 10px 0'
  },
  clusterStyle: {
    display: 'flex',
    width: 'inherit',
    height: 'inherit',
    border: `1px solid ${DIVIDER}`,
    borderRadius: 2,
    lineHeight: '21px',
    padding: '0 6px',
    justifyContent: 'flex-start',
    alignItems: 'center',
    opacity: 0.6
  },
  clusterStyleActive: {
    opacity: 1,
    cursor: 'pointer',
    ':hover': {
      border: '1px solid #C0E9F5',
      boxShadow: '0 0 5px 0 rgba(0, 0, 0, 0.10)'
    }
  },
  iconStyle: {
    width: 48,
    height: 48
  }
};
