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
import { Component, createRef } from 'react';

import PropTypes from 'prop-types';

import FontIcon from 'components/Icon/FontIcon';
import { Tooltip } from '@app/components/Tooltip';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';

const MAX_DATASETS = 3;

const Dataset = (props) => {
  const { fullPath } = props;
  const name = fullPath[fullPath.length - 1];
  return (
    <div style={styles.dataset}>
      <FontIcon type='VirtualDataset'/>
      <span style={styles.datasetName}>{name}</span>
    </div>
  );
};

Dataset.propTypes = {
  fullPath: PropTypes.array
};


export default class DependantDatasetsWarning extends Component {
  static propTypes = {
    text: PropTypes.string,
    dependantDatasets: PropTypes.array,
    getGraphLink: PropTypes.func
  };

  state = {
    isOpenTooltip: false
  };

  moreLinkRef = createRef();

  getTooltipTarget = () => this.state.isOpenTooltip ? this.moreLinkRef.current : null;

  handleMouseEnter = () => {
    this.setState({
      isOpenTooltip: true
    });
  };

  handleMouseLeave = () => {
    this.setState({
      isOpenTooltip: false
    });
  };

  renderTooltip() {
    const { dependantDatasets } = this.props;
    const tooltipDatasets = dependantDatasets.slice(MAX_DATASETS);
    return (
      <Tooltip
          target={this.getTooltipTarget}
          id='tooltip'
          type='status'
          style={{ zIndex: 1300 }}
          placement='bottom'
        >
        <div style={styles.tooltipContainer}>
          {tooltipDatasets.map((fullPath, i) => <Dataset key={i} fullPath={fullPath} />)}
        </div>
      </Tooltip>
    );
  }
  render() {
    const { text, dependantDatasets, getGraphLink } = this.props;
    const showDatasets = dependantDatasets.slice(0, MAX_DATASETS);
    const isGreaterThanMax = dependantDatasets.length > MAX_DATASETS;

    return (
      <div className='dataset-warning'>
        <FontIcon type='Warning' style={styles.warningIcon}/>
        <div style={styles.messageText}>
          {text}
        </div>
        <div style={styles.datasetContainer}>
          {showDatasets.map(fullPath => {
            return (
              <div style={{marginRight: 5}}>
                <DatasetItemLabel
                  name={fullPath[fullPath.length - 1]}
                  fullPath={Immutable.List(fullPath)}
                  typeIcon='VirtualDataset'
                />
              </div>
            );
          })}
          {isGreaterThanMax
            ? <div style={styles.dataset}>
              <div
                ref={this.moreLinkRef}
                onMouseEnter={this.handleMouseEnter}
                onMouseLeave={this.handleMouseLeave}
                style={styles.datasetName}
                >
                <a>{la(`and ${dependantDatasets.length - MAX_DATASETS} more...`)}</a>
              </div>
            </div>
            : null}
          {this.renderTooltip()}
          {getGraphLink && <div style={styles.graphLink}>{getGraphLink()}</div>}
        </div>
      </div>
    );
  }
}

const styles = {
  warningIcon: {
    top: 10,
    position: 'absolute',
    left: 10
  },
  datasetContainer: {
    display: 'flex',
    width: '100%',
    marginTop: 5,
    overflow: 'auto'
  },
  tooltipContainer: {
    display: 'flex',
    flexDirection: 'column'
  },
  dataset: {
    display: 'flex',
    marginRight: 5
  },
  datasetName: {
    justifyContent: 'center',
    margin: 5
  },
  messageText: {
    marginTop: 6
  },
  graphLink: {
    margin: '0 5px'
  }
};
