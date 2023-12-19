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
import { PureComponent } from "react";
import PropTypes from "prop-types";
import DocumentTitle from "react-document-title";

import { page } from "uiTheme/radium/general";
import config from "@inject/utils/config";

import getSectionsConfig from "@inject/pages/AdminPage/navSections";
import AdminMixin from "dyn-load/pages/AdminPage/AdminMixin.js";

import AdminPageView from "./AdminPageView";

@AdminMixin
class AdminPage extends PureComponent {
  static propTypes = {
    location: PropTypes.object.isRequired,
    routeParams: PropTypes.object,
    children: PropTypes.node,
  };

  constructor(props) {
    super(props);

    this.state = {
      sections: [],
    };

    getSectionsConfig(config)
      .then((sections) => {
        this.setState({ sections });
        return null;
      })
      .catch((e) => {
        this.handleMenuCatchBlock(e);
      });
  }

  render() {
    const { routeParams, location, children } = this.props;
    return (
      <DocumentTitle title={laDeprecated("Settings")}>
        <AdminPageView
          routeParams={routeParams}
          sections={this.state.sections}
          style={page}
          location={location}
        >
          {children}
        </AdminPageView>
      </DocumentTitle>
    );
  }
}

export default AdminPage;
