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
import { Component } from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import Immutable from "immutable";

import { getHomeContents } from "@app/selectors/home";
import { loadHomeContentWithAbort } from "@app/actions/home";
import { getViewState } from "selectors/resources";
import { ENTITY_TYPES } from "@app/constants/Constants";

import { updateRightTreeVisibility } from "actions/ui/ui";

import MainInfo from "../components/MainInfo";
import { CatalogPrivilegeSwitch } from "@app/exports/components/CatalogPrivilegeSwitch/CatalogPrivilegeSwitch";
import Message from "@app/components/Message";
import { fetchArsFlag } from "@inject/utils/arsUtils";
import { store } from "@app/store/store";
import { isVersionedSource } from "@app/utils/sourceUtils";
import { isDefaultReferenceLoading } from "@app/selectors/nessie/nessie";

export const VIEW_ID = "HomeContents";

class HomeContents extends Component {
  static propTypes = {
    location: PropTypes.object,

    //connected
    getContentUrl: PropTypes.string.isRequired,
    updateRightTreeVisibility: PropTypes.func.isRequired,
    rightTreeVisible: PropTypes.bool,
    entity: PropTypes.instanceOf(Immutable.Map),
    entityType: PropTypes.oneOf(Object.values(ENTITY_TYPES)),
    viewState: PropTypes.instanceOf(Immutable.Map),
    source: PropTypes.object,
    params: PropTypes.object,
  };

  static contextTypes = {
    username: PropTypes.string.isRequired,
  };

  componentDidMount() {
    if (this.props.nessieLoading) return; //Nessie references still fetching, wait
    this.load();
  }

  componentDidUpdate(prevProps) {
    const invalidated = this.props.viewState.get("invalidated");
    const prevInvalidated = prevProps.viewState.get("invalidated");
    if (
      (!!prevProps.nessieLoading && !this.props.nessieLoading) ||
      prevProps.location.pathname !== this.props.location.pathname ||
      (invalidated && invalidated !== prevInvalidated)
    ) {
      this.load();
    }
  }

  prevLoad = null;

  load = async () => {
    const { getContentUrl, entityType, params } = this.props;

    const isArsEnabled = await fetchArsFlag();
    if (isArsEnabled && entityType === ENTITY_TYPES.home) return; // Skip home

    this.prevLoad?.abort();
    const [abortController, loadHomeContent] = loadHomeContentWithAbort(
      getContentUrl,
      entityType,
      VIEW_ID,
      params,
    );
    this.prevLoad = abortController;

    return store.dispatch(loadHomeContent);
  };

  componentWillUnmount = () => {
    this.prevLoad?.abort();
  };

  shouldComponentUpdate(nextProps) {
    for (const [key, value] of Object.entries(nextProps)) {
      const currentValue = this.props[key];

      // getHomeContents produces a new instance on each invocation
      // TODO: Unfortunately loading file format settings triggers a load of data that gets
      // merged into `entity` (so we still churn a little bit)
      if (key === "entity" && value && value.equals(currentValue)) {
        continue;
      }

      if (
        key === "location" &&
        value &&
        currentValue &&
        value.pathname === currentValue.pathname
      ) {
        continue;
      }

      if (value !== currentValue) {
        return true;
      }
    }

    return false;
  }

  render() {
    const { entity, entityType, source, viewState, rightTreeVisible } =
      this.props;

    const isArcticSource = source?.get("type") === "ARCTIC";
    if (isArcticSource) {
      return (
        <CatalogPrivilegeSwitch
          privilege="canView"
          renderEnabled={() => (
            <MainInfo
              entityType={entityType}
              entity={entity}
              source={source}
              viewState={viewState}
              updateRightTreeVisibility={this.props.updateRightTreeVisibility}
              rightTreeVisible={rightTreeVisible}
            />
          )}
          renderDisabled={() => (
            <Message
              isDismissable={false}
              messageType="error"
              message={viewState.getIn(["error", "message", "errorMessage"])}
            />
          )}
        />
      );
    }
    return (
      <MainInfo
        entityType={entityType}
        entity={entity}
        source={source}
        viewState={viewState}
        updateRightTreeVisibility={this.props.updateRightTreeVisibility}
        rightTreeVisible={rightTreeVisible}
      />
    );
  }
}

function mapStateToProps(state, props) {
  const versioned = props.source && isVersionedSource(props.source.get("type"));
  const nessieLoading =
    versioned &&
    isDefaultReferenceLoading(state.nessie[props.source.get("name")]);

  return {
    nessieLoading,
    rightTreeVisible: state.ui.get("rightTreeVisible"),
    entity: getHomeContents(state),
    viewState: getViewState(state, VIEW_ID),
  };
}

export default connect(mapStateToProps, {
  updateRightTreeVisibility,
})(HomeContents);
