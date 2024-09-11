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
import AccessControlListSection from "dyn-load/components/Forms/AccessControlListSection";
import { connect } from "react-redux";

import PropTypes from "prop-types";

class SharingWrapper extends Component {
  static propTypes = {
    elementConfig: PropTypes.object,
    fields: PropTypes.object,
    isFileSystemSource: PropTypes.bool,
    isExternalQueryAllowed: PropTypes.bool,
    isHive: PropTypes.bool,
    isGlue: PropTypes.bool,
  };

  render() {
    const {
      elementConfig,
      fields,
      isFileSystemSource,
      isHive,
      isGlue,
      isExternalQueryAllowed,
    } = this.props;
    let source;
    if (isFileSystemSource || isHive || isGlue) {
      source = "MUTABLE_SOURCE";
    } else if (isExternalQueryAllowed === true) {
      source = "ARP_SOURCE";
    } else {
      source = "source";
    }

    return (
      <div style={{ height: "64vh", overflow: "hidden" }}>
        <AccessControlListSection
          fields={fields}
          EntityType={source}
          isTopLevelEntity
          elementConfig={elementConfig.getConfig()}
        />
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    isFileSystemSource: state.passDataBetweenModalTabs.isFileSystemSource,
    isExternalQueryAllowed:
      state.passDataBetweenModalTabs.isExternalQueryAllowed,
    isHive: state.passDataBetweenModalTabs.isHive,
    isGlue: state.passDataBetweenModalTabs.isGlue,
  };
};

export default connect(mapStateToProps)(SharingWrapper);
