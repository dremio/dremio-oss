/*
 * Copyright (C) 2017-2019 Dremio Corporation
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

import { getUserName } from '@app/selectors/account';
import { getHomeContents, getNormalizedEntityPathByUrl } from '@app/selectors/home';
import { loadHomeContent as loadHomeContentAction } from '@app/actions/home';
import { getViewState } from 'selectors/resources';
import { getEntityType } from 'utils/pathUtils';
import { ENTITY_TYPES } from '@app/constants/Constants';
import { getRefQueryParams } from '@app/utils/nessieUtils';

import { updateRightTreeVisibility } from 'actions/ui/ui';

import MainInfo from '../components/MainInfo';

export const VIEW_ID = 'HomeContents';

class HomeContents extends Component {

  static propTypes = {
    location: PropTypes.object,

    //connected
    getContentUrl: PropTypes.string.isRequired,
    loadHomeContent: PropTypes.func.isRequired,
    updateRightTreeVisibility: PropTypes.func.isRequired,
    rightTreeVisible: PropTypes.bool,
    entity: PropTypes.instanceOf(Immutable.Map),
    entityType: PropTypes.oneOf(Object.values(ENTITY_TYPES)),
    viewState: PropTypes.instanceOf(Immutable.Map),
    nessie: PropTypes.object
  };

  static contextTypes = {
    username: PropTypes.string.isRequired
  };

  componentDidMount() {
    this.load();
  }

  componentDidUpdate(prevProps) {
    if (prevProps.location.pathname !== this.props.location.pathname || this.props.viewState.get('invalidated')) {
      this.load();
    }
  }

  load() {
    const {
      getContentUrl,
      entityType,
      loadHomeContent,
      nessie
    } = this.props;

    const params = this.getParams(nessie, entityType, getContentUrl);
    loadHomeContent(getContentUrl, entityType, VIEW_ID, params);
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

  getParams(nessie, entityType, url) {
    if (!['source', 'folder'].includes(entityType)) return null;
    const [, , sourceName] = url.split('/');
    return getRefQueryParams(nessie, sourceName);
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
    entity: getHomeContents(state),
    entityType,
    // do not use getNormalizedEntityPath from selectors/home here until DX-16200 would be resolved
    // we must use router location value, as redux location could be out of sync
    getContentUrl: getNormalizedEntityPathByUrl(location.pathname, getUserName(state)),
    viewState: getViewState(state, VIEW_ID),
    nessie: state.nessie
  };
}

export default connect(mapStateToProps, {
  loadHomeContent: loadHomeContentAction,
  updateRightTreeVisibility
})(HomeContents);
