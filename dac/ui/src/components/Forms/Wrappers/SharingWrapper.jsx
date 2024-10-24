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
import additionalSharingWrapperControls from "@inject/shared/AdditionalSharingWrapperControls";
import { connect } from "react-redux";

import PropTypes from "prop-types";

class SharingWrapper extends Component {
  static propTypes = {
    elementConfig: PropTypes.object,
    fields: PropTypes.object,
    isFileSystemSource: PropTypes.bool,
    isExternalQueryAllowed: PropTypes.bool,
    isMetaStore: PropTypes.bool,
    sourceType: PropTypes.string,
  };

  render() {
    const {
      elementConfig,
      fields,
      isFileSystemSource,
      isMetaStore,
      isExternalQueryAllowed,
      sourceType,
    } = this.props;
    let source;
    if (additionalSharingWrapperControls?.()?.checkSourceType) {
      source = additionalSharingWrapperControls().checkSourceType({
        sourceType,
      });
    }
    if (!source) {
      if (isFileSystemSource || isMetaStore) {
        source = "MUTABLE_SOURCE";
      } else if (isExternalQueryAllowed) {
        source = "ARP_SOURCE";
      } else {
        source = "source";
      }
    }

    return (
      <div style={{ height: "64vh", overflow: "hidden" }}>
        <AccessControlListSection
          fields={fields}
          EntityType={source}
          sourceType={sourceType}
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
    isMetaStore: state.passDataBetweenModalTabs.isMetaStore,
    sourceType: state.passDataBetweenModalTabs.sourceType,
  };
};

export default connect(mapStateToProps)(SharingWrapper);
