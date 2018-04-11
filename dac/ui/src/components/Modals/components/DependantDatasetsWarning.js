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

import PropTypes from 'prop-types';

import FontIcon from 'components/Icon/FontIcon';
import Tooltip from 'components/Tooltip';
import { NAVY } from 'uiTheme/radium/colors';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';

const MAX_DATASETS = 5;
const OFFSET = 90;

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
    dependantDatasets: PropTypes.array
  };

  constructor(props) {
    super(props);

    this.state = {
      isOpenTooltip: false
    };

    this.handleMouseEnter = this.handleMouseEnter.bind(this);
    this.handleMouseLeave = this.handleMouseLeave.bind(this);
  }

  componentDidUpdate() {
    if (this.state.isOpenTooltip) {
      this.updateTooltipPosition();
    }
  }

  handleMouseEnter() {
    this.setState({
      isOpenTooltip: true
    });
  }

  handleMouseLeave() {
    this.setState({
      isOpenTooltip: false
    });
  }

  updateTooltipPosition() {
    const showMore = $(this.refs.showMore);
    const tooltip = $('#tooltip');
    tooltip.css('top', showMore.position().top + showMore.height());
    tooltip.css('left', showMore.position().left - OFFSET + showMore.width() / 2);
  }

  renderTooltip() {
    const { isOpenTooltip } = this.state;
    const { dependantDatasets } = this.props;
    const tooltipDatasets = dependantDatasets.slice(MAX_DATASETS);
    return isOpenTooltip
      ?
        <Tooltip
          id='tooltip'
          type='info'
          placement='bottom'
          tooltipInnerStyle={styles.tooltipInner}
          tooltipArrowStyle={styles.tooltipArrowStyle}
          arrowOffsetLeft={OFFSET}
          content={
            <div style={styles.tooltipContainer}>
              {tooltipDatasets.map((fullPath, i) => <Dataset key={i} fullPath={fullPath} />)}
            </div>
        }
      />
      : null;
  }
  render() {
    const { text, dependantDatasets } = this.props;
    const showDatasets = dependantDatasets.slice(0, MAX_DATASETS);
    const isGreaterThanMax = dependantDatasets.length > MAX_DATASETS;
    return (
      <div className='dataset-warning'>
        <FontIcon type='Warning' style={styles.warningIcon}/>
        <div>
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
              <span
                ref='showMore'
                onMouseEnter={this.handleMouseEnter}
                onMouseLeave={this.handleMouseLeave}
                style={styles.datasetName}
                >
                <a>and {dependantDatasets.length - MAX_DATASETS} more...</a>
              </span>
            </div>
            : null}
          {this.renderTooltip()}
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
    display: 'flex'
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
  tooltipInner: {
    background: NAVY,
    color: '#fff',
    boxShadow: '2px 2px 5px 0px rgba(0,0,0,0.05)',
    borderRadius: '2px'
  },
  tooltipArrowStyle: {
    borderTopColor: NAVY,
    borderBottomColor: NAVY
  }
};
