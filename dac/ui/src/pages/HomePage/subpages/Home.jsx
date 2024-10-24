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
import HomePage from "pages/HomePage/HomePage";
import HomeContents from "./HomeContents";
import { connect } from "react-redux";
import {
  getNormalizedEntityPathByUrl,
  getSortedSources,
} from "#oss/selectors/home";
import { getEntityType, getSourceNameFromUrl } from "#oss/utils/pathUtils";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import { getUserName } from "#oss/selectors/account";

import { getRefQueryParams } from "#oss/utils/nessieUtils";

class Home extends Component {
  static propTypes = {
    location: PropTypes.object,
    style: PropTypes.object,
    router: PropTypes.object,

    homeContentsProps: PropTypes.object,
  };

  render() {
    const { style, location, homeContentsProps: props = {} } = this.props;
    return (
      <HomePage style={style} location={location}>
        <HomeContents location={location} {...props} />
      </HomePage>
    );
  }
}

const mapStateToProps = (state, props) => {
  const pathname =
    rmProjectBase(location.pathname, {
      projectId: props.router.params?.projectId,
    }) || "/";
  const entityType = getEntityType(pathname);
  // do not use getNormalizedEntityPath from selectors/home here until DX-16200 would be resolved
  // we must use router location value, as redux location could be out of sync
  const getContentUrl = getNormalizedEntityPathByUrl(
    pathname,
    getUserName(state),
  );

  const sourceName = getSourceNameFromUrl(getContentUrl);
  const sources = getSortedSources(state);
  const source =
    sources && sourceName
      ? sources.find((cur) => cur.get("name") === sourceName)
      : null;
  const params =
    entityType && ["source", "folder"].includes(entityType)
      ? getRefQueryParams(state.nessie, sourceName)
      : null;

  return { homeContentsProps: { source, params, entityType, getContentUrl } };
};

export default connect(mapStateToProps)(Home);
