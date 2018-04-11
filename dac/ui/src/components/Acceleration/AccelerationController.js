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
import { connect } from 'react-redux';
import Immutable from 'immutable';

import reflectionActions from 'actions/resources/reflection';
import { getViewState } from 'selectors/resources';
// import * as schemas from 'schemas';
// import ApiUtils from 'utils/apiUtils/apiUtils';
//import { overlay } from 'uiTheme/radium/overlay';
import { loadDataset } from 'actions/resources/dataset';
import ViewStateWrapper from '../ViewStateWrapper';
import AccelerationForm from './AccelerationForm';

const VIEW_ID = 'AccelerationModal';

export class AccelerationController extends Component {
  static propTypes = {
    datasetId: PropTypes.string, // populated except during teardown
    dataset: PropTypes.instanceOf(Immutable.Map),
    reflections: PropTypes.instanceOf(Immutable.Map),

    getReflections: PropTypes.func.isRequired,
    getDataset: PropTypes.func.isRequired,

    onCancel: PropTypes.func,
    onDone: PropTypes.func,
    viewState: PropTypes.instanceOf(Immutable.Map),
    resetViewState: PropTypes.func,
    updateFormDirtyState: PropTypes.func
  };

  state = {
    getComplete: false // need to track ourselves because viewState initial state looks the same as loaded-success
  }

  componentWillMount() {
    return this.props.getReflections(
      {viewId: VIEW_ID},
      { path: `dataset/${encodeURIComponent(this.props.datasetId)}/reflection` }
    ).then((response) => {
      if (response.payload instanceof Error) return;
      return this.props.getDataset(this.props.datasetId, VIEW_ID);
    }).then(() => this.setState({getComplete: true}));
  }

  handleSubmitSuccess = (values) => {
    this.props.onDone(null, true);
    // future: stay open with refresh (is that even needed?), OR close (user option)
  }

  renderContent() {
    const { viewState, reflections, dataset } = this.props;

    if (!this.state.getComplete || viewState.get('isFailed')) {
      return null; // AccelerationForm expects to only be created after data is ready
    }

    if (!dataset || !reflections) return null; // teardown guard

    return <AccelerationForm
      updateFormDirtyState={this.props.updateFormDirtyState}
      onCancel={this.props.onCancel}
      onSubmitSuccess={this.handleSubmitSuccess}
      dataset={dataset}
      reflections={reflections}
    />;
  }

  render() {
    const { viewState } = this.props;

    return (
      <ViewStateWrapper viewState={viewState}>
        {this.renderContent()}
      </ViewStateWrapper>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const reflections = state.resources.entities.get('reflection') ? state.resources.entities.get('reflection').filter(reflection => {
    return reflection.get('datasetId') === ownProps.datasetId;
  }) : new Immutable.Map();

  const dataset = state.resources.entities.get('dataset') && state.resources.entities.get('dataset').get(ownProps.datasetId);

  let viewState = getViewState(state, VIEW_ID);
  if (typeof dataset !== 'undefined' && !dataset.has('fields')) {
    viewState = Immutable.fromJS({
      isFailed: true,
      error: {
        message: la('Dataset missing schema information.')
      }
    });
  }

  return {
    reflections,
    dataset,
    viewState
  };
}

export default connect(mapStateToProps, {
  getReflections: reflectionActions.getList.dispatch,
  getDataset: loadDataset
})(AccelerationController);
