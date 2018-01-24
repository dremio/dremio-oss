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
import { connect } from 'react-redux';
import Immutable from 'immutable';
import moment from 'moment';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { Link } from 'react-router';
import {title, contextCard, cardTitle, contextAttrs, attrLabel, attrValue} from 'uiTheme/radium/rightContext';
import FontIcon from 'components/Icon/FontIcon';
import ViewStateWrapper from 'components/ViewStateWrapper';

import { loadDatasetContext, LOAD_DATASET_CONTEXT_VIEW_ID } from 'actions/resources/datasetContexts';
import { getEntity, getViewState } from 'selectors/resources';

@pureRender
@Radium
export class DatasetContext extends Component {

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    datasetContext: PropTypes.instanceOf(Immutable.Map),

    //connected
    loadDatasetContext: PropTypes.func.isRequired,
    viewState: PropTypes.instanceOf(Immutable.Map)
  };

  static contextTypes = {
    username: PropTypes.string,
    router: PropTypes.object,
    location: PropTypes.object
  }

  static defaultProps = {
    datasetContext: Immutable.Map()
  }

  constructor(props) {
    super(props);
    this.loadContextIfNecessary({}, this.props);
  }

  componentWillReceiveProps(nextProps) {
    this.loadContextIfNecessary(this.props, nextProps);
  }

  loadContextIfNecessary(props, nextProps) {
    if (nextProps.entity !== props.entity) {
      nextProps.loadDatasetContext(nextProps.entity);
    }
  }

  openDatasetModal = (tab) => {
    const { router, location } = this.context;
    router.push({
      ...location, state: {
        modal: 'DatasetSettingsModal',
        entityType: 'dataset',
        tab
      }
    });
  }

  render() {
    const {datasetContext, viewState} = this.props;
    const accelerationInfo = datasetContext.get('accelerationInfo') || Immutable.Map();

    const parentDatasetContainer = datasetContext.get('parentDatasetContainer') || Immutable.Map();

    const isAccelerationHovered = Radium.getState(this.state, 'acceleration', ':hover');
    const isDetailsHovered = Radium.getState(this.state, 'details', ':hover');
    return <ViewStateWrapper viewState={viewState}>
      <h4 style={title}>About this Dataset</h4>
      <div style={contextCard} key='details'>
        <div style={styles.headerWithPencil}>
          <div style={cardTitle}>Details</div>
          { isDetailsHovered && false &&
          <FontIcon
            type='Edit'
            hoverType='EditActive'
            theme={styles.pencil}
            onClick={this.openDatasetModal.bind(this, 'overview')}
              />
          }
        </div>
        <ul style={contextAttrs}>
          <li>
            <span style={attrLabel}>Created At:</span>
            <span style={attrValue}>{moment(datasetContext.get('createdAt')).format('YYYY-MM-DD')}</span>
          </li>
          <li>
            <span style={attrLabel}>Jobs</span>
            <Link to='/jobs'>{datasetContext.get('jobCount') || 0}»</Link>
          </li>
          <li>
            <span style={attrLabel}>Descendants</span>
            <span style={attrValue}>{datasetContext.get('descendants')}</span>
          </li>
        </ul>
        <div className='description'>
          {datasetContext.get('description')}
        </div>
      </div>
      <div style={contextCard} key='acceleration'>
        <div style={styles.headerWithPencil}>
          <div style={cardTitle}>Acceleration</div>
          {
            isAccelerationHovered
              ? (
                <FontIcon
                  type='Edit'
                  hoverType='EditActive'
                  theme={styles.pencil}
                  onClick={this.openDatasetModal.bind(this, 'acceleration')}
                />
              )
              : null
          }
        </div>
        <ul style={contextAttrs}>
          <li>
            <span style={attrLabel}>Status:</span>
            <span style={attrValue}>{accelerationInfo.get('enabled') ? 'enabled' : 'disabled'}</span>
          </li>
        </ul>
      </div>
      <div style={contextCard} key='parentDatasetContainer'>
        <div style={cardTitle}>Parent {parentDatasetContainer.get('type')}</div>
        <ul style={contextAttrs}>
          <li>
            <span style={attrLabel}>Name:</span>
            <span style={attrValue}>{parentDatasetContainer.get('name')}</span>
          </li>
          <li>
            <span style={attrLabel}>Created At:</span>
            <span style={attrValue}>
              {moment(parentDatasetContainer.get('createdAt')).format('YYYY-MM-DD')}
            </span>
          </li>
        </ul>
        <div className='description'>
          {parentDatasetContainer.get('description')}
        </div>
      </div>

    </ViewStateWrapper>;
  }
}

function mapStateToProps(state, props) {
  const datasetContext = getEntity(state,
    props.entity && props.entity.get('displayFullPath').join(','), 'datasetContext');

  return {
    datasetContext,
    viewState: getViewState(state, LOAD_DATASET_CONTEXT_VIEW_ID)
  };
}

export default connect(mapStateToProps, {
  loadDatasetContext
})(DatasetContext);

const styles = {
  headerWithPencil: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    height: 27
  },
  pencil: {
    Container: {
      cursor: 'pointer'
    }
  }
};
