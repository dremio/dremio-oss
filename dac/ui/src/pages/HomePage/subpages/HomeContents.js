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

import { loadHomeEntities } from 'actions/resources';
import { getHomeContents } from 'selectors/datasets';
import { getViewState } from 'selectors/resources';
import { getEntityType } from 'utils/pathUtils';
import { ENTITY_TYPES } from 'constants/Constants';

import { updateRightTreeVisibility } from 'actions/ui/ui';

import * as schemas from 'schemas';

import MainInfo from '../components/MainInfo';

export const VIEW_ID = 'HomeContents';

class HomeContents extends Component {

  static propTypes = {
    location: PropTypes.object,

    //connected
    loadHomeEntities: PropTypes.func.isRequired,
    updateRightTreeVisibility: PropTypes.func.isRequired,
    rightTreeVisible: PropTypes.bool,
    entity: PropTypes.instanceOf(Immutable.Map),
    entityType: PropTypes.oneOf(Object.values(ENTITY_TYPES)),
    viewState: PropTypes.instanceOf(Immutable.Map)
  };

  static contextTypes = {
    username: PropTypes.string.isRequired
  };

  componentWillMount() {
    const {location, entityType} = this.props;
    this.props.loadHomeEntities(location.pathname, this.context.username, schemas[entityType], VIEW_ID);
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.location.pathname !== this.props.location.pathname || nextProps.viewState.get('invalidated')) {
      nextProps.loadHomeEntities(
        nextProps.location.pathname, this.context.username, schemas[nextProps.entityType], VIEW_ID);
    }
  }

  shouldComponentUpdate(nextProps) {
    for (const [key, value] of Object.entries(nextProps)) {
      const currentValue = this.props[key];

      // getHomeContents produces a new instance on each invocation
      // TODO: Unfortunately loading file format settings triggers a load of data that gets
      // merged into `entity` (so we still churn a little bit)
      if (key === 'entity' && value && value.equals(currentValue)) {
        continue;
      }

      if (key === 'location' && value && currentValue && value.pathname === currentValue.pathname) {
        continue;
      }

      if (value !== currentValue) {
        return true;
      }
    }

    return false;
  }


  render() {
    const {entity, entityType, viewState, rightTreeVisible} = this.props;
    return (
      <MainInfo
        entityType={entityType}
        entity={entity}
        viewState={viewState}
        updateRightTreeVisibility={this.props.updateRightTreeVisibility}
        rightTreeVisible={rightTreeVisible}
      />
    );
  }
}

function mapStateToProps(state, props) {
  const { location } = props;
  const entityType = getEntityType(location.pathname);

  return {
    rightTreeVisible: state.ui.get('rightTreeVisible'),
    entity: getHomeContents(state, location.pathname),
    entityType,
    viewState: getViewState(state, VIEW_ID)
  };
}

export default connect(mapStateToProps, {
  loadHomeEntities,
  updateRightTreeVisibility
})(HomeContents);
