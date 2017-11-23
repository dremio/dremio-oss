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
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import Immutable from 'immutable';

import {
  loadAccelerationById,
  createEmptyAcceleration,
  updateAcceleration,
  deleteAcceleration
} from 'actions/resources/acceleration';
import { resetViewState, loadEntities } from 'actions/resources';
import { getViewState, getEntity } from 'selectors/resources';
// import * as schemas from 'schemas';
import ApiUtils from 'utils/apiUtils/apiUtils';
import { overlay } from 'uiTheme/radium/overlay';
import ViewStateWrapper from '../ViewStateWrapper';
import Spinner from '../Spinner';
import AccelerationForm from './AccelerationForm';

const VIEW_ID = 'AccelerationModal';

export class AccelerationController extends Component {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map), // optional, used as fallback to determine accelerationId and create an Acceleration on demand
    loadAccelerationById: PropTypes.func.isRequired,
    createEmptyAcceleration: PropTypes.func.isRequired,
    updateAcceleration: PropTypes.func,
    deleteAcceleration: PropTypes.func,
    onCancel: PropTypes.func,
    onDone: PropTypes.func,
    accelerationId: PropTypes.string, // populated directly or by mapStateToProps (except during teardown)
    acceleration: PropTypes.instanceOf(Immutable.Map),
    viewState: PropTypes.instanceOf(Immutable.Map),
    resetViewState: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
    loadEntities: PropTypes.func
  };

  pollId = 0; // eslint-disable-line react/sort-comp

  componentWillMount() {
    const { entity } = this.props;
    if (entity) {
      // at this point we can't tell if an Acceleration exists or not, so let's try to create one
      // - if it succeeds then that's great, we then will try to load it
      // - if it fails it means one exists, so we will then go try to load it

      this.props.createEmptyAcceleration(entity, VIEW_ID).then(response => {
        this.startPollingAccelerationDataWhileNew();
      });
    } else {
      this.startPollingAccelerationDataWhileNew();
    }
  }

  componentWillUnmount() {
    this.stopPollingAccelerationData();
  }

  loadAcceleration = () => {
    return this.props.loadAccelerationById(this.props.accelerationId, VIEW_ID);
  }

  getAccelerationVersion(props) {
    const version = props.acceleration.get('version');
    // fix for DX-6379 incorrect Configuration Modified message on every initial acceleration
    // Skip showing conflict when version changed from 0 to 1 during create acceleration
    return version ? version : undefined;
  }

  submit = (values) => {
    return ApiUtils.attachFormSubmitHandlers(
      this.props.updateAcceleration(values, this.props.acceleration.getIn(['id', 'id']))
    ).then(() => {
      this.props.onDone(null, true);
    });
  }

  /*
   * Polling
   */

  startPollingAccelerationDataWhileNew() {
    this.stopPollingAccelerationData();
    this.pollAccelerationData();
  }

  stopPollingAccelerationData() {
    clearTimeout(this.pollId);
    this.pollId = 0;
  }

  pollAccelerationData = () => {
    return this.loadAcceleration().then((response) => {
      if (!response.error) {
        const result = ApiUtils.getEntityFromResponse('acceleration', response);
        this.stopPollingAccelerationData();
        if (result && result.get('state') === 'NEW') {
          this.pollId = setTimeout(this.pollAccelerationData, 1000);
        }
      }
    });
  }

  renderContent() {
    const { acceleration } = this.props;

    if (!acceleration || acceleration.get('state') === 'NEW') {
      return null;
    }

    return <AccelerationForm
      getConflictedValues={this.getAccelerationVersion}
      updateFormDirtyState={this.props.updateFormDirtyState}
      deleteAcceleration={this.props.deleteAcceleration}
      onCancel={this.props.onCancel}
      submit={this.submit}
      acceleration={acceleration}
    />;
  }

  render() {
    const { acceleration, viewState } = this.props;

    // show descriptive spinner first to avoid flashing while polling (but let isFailed trump)
    if (acceleration && acceleration.get('state') === 'NEW' && !viewState.get('isFailed')) {
      return <div style={overlay} className='view-state-wrapper-overlay'>
        <div>
          <Spinner message={la('Determining Automatic Reflections…')} />
        </div>
      </div>;
    }

    return (
      <ViewStateWrapper viewState={this.props.viewState} hideChildrenWhenInProgress>
        {this.renderContent()}
      </ViewStateWrapper>
    );
  }
}

function mapStateToProps(state, ownProps) {
  let { accelerationId, entity } = ownProps;
  if (!accelerationId && entity) {
    const entityId = entity.get('id');

    accelerationId = entityId; // the id is defined to be the same as the dataset id
  }
  const acceleration = getEntity(state, accelerationId, 'acceleration');

  return {
    // TODO: will add selector
    acceleration,
    accelerationId,
    viewState: getViewState(state, VIEW_ID)
  };
}

export default connect(mapStateToProps, {
  loadAccelerationById,
  createEmptyAcceleration,
  updateAcceleration,
  deleteAcceleration,
  resetViewState,
  loadEntities
})(AccelerationController);
