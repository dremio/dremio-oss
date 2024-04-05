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
import { compose } from "redux";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import Immutable from "immutable";
import urlParse from "url-parse";
import copy from "copy-to-clipboard";
import { injectIntl } from "react-intl";

import { removeDataset, removeFile } from "actions/resources/spaceDetails";
import { showConfirmationDialog } from "actions/confirmation";
import { constructFullPath, getFullPathListFromEntity } from "utils/pathUtils";
import { UpdateMode } from "pages/HomePage/components/modals/UpdateDataset/UpdateDatasetView";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import DatasetMenuMixin from "dyn-load/components/Menus/HomePage/DatasetMenuMixin";
import { withCatalogARSFlag } from "@inject/utils/arsUtils";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

// todo: all these entities have a lot of similarities (they are all Datasets of some sort)
// but do not share a protocol/interface. This code *should* be able to
// exist without any special casing.

export const getSettingsLocation = (location, entity, entityType) => ({
  ...location,
  state: {
    modal: "DatasetSettingsModal",
    entityName: entity.get("fullPathList").last(),
    // todo: normalize
    entityId:
      entity.get("versionedResourcePath") || // VDS
      entity.get("id"), // file, folder, PDS (see resourceDecorators)

    entityType,
    type: entityType,
    isHomePage: true,
  },
});

@DatasetMenuMixin
export class DatasetMenu extends Component {
  static contextTypes = {
    location: PropTypes.object.isRequired,
  };

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map).isRequired,
    entityType: PropTypes.string.isRequired, // todo: remove and get from #entity (physicalDataset || dataset || file)

    closeMenu: PropTypes.func.isRequired,
    removeDataset: PropTypes.func.isRequired,
    removeFile: PropTypes.func.isRequired,
    showConfirmationDialog: PropTypes.func,
    openWikiDrawer: PropTypes.func,
  };

  getMenuItemUrl(itemCode) {
    const { entity } = this.props;

    const queryLink = entity.getIn(["links", "query"]);
    const editLink = entity.getIn(["links", "edit"]);
    const canAlter = entity.getIn(["permissions", "canAlter"]);
    const canSelect = entity.getIn(["permissions", "canSelect"]);

    const toLink = (canAlter || canSelect) && editLink ? editLink : queryLink;
    const urldetails = new URL(window.location.origin + toLink);
    const pathname = urldetails.pathname + `/${itemCode}` + urldetails.search;

    return wrapBackendLink(pathname);
  }

  getLocationConfig = (mode) => {
    const { entity } = this.props;
    return {
      ...this.context.location,
      state: {
        ...this.context.location.state,
        modal: "UpdateDataset",
        item: entity,
        query: {
          fullPath: entity.get("fullPath"),
          name: entity.get("datasetName"),
          getGraphLink: this.getGraphLink(),
          mode,
        },
      },
    };
  };
  // only tested with VDS
  getRenameLocation() {
    return this.getLocationConfig(UpdateMode.rename);
  }

  // only tested with VDS
  getMoveLocation() {
    return this.getLocationConfig(UpdateMode.move);
  }

  getRemoveLocation() {
    return this.getLocationConfig(UpdateMode.remove);
  }

  getRemoveFormatLocation() {
    return this.getLocationConfig(UpdateMode.removeFormat);
  }

  getSettingsLocation() {
    const { entity, entityType } = this.props;

    return getSettingsLocation(this.context.location, entity, entityType);
  }

  handleRemoveFile = () => {
    const { t } = getIntlContext();
    const { closeMenu, entity } = this.props;
    this.props.showConfirmationDialog({
      text: t("Delete.Confirmation", {
        name: entity.get("name"),
      }),
      confirmText: t("Common.Actions.Delete"),
      confirm: () => this.props.removeFile(entity),
      title: t("File.Delete"),
      confirmButtonStyle: "primary-danger",
    });
    closeMenu();
  };

  copyPath = () => {
    const fullPath = constructFullPath(
      getFullPathListFromEntity(this.props.entity)
    );
    copy(fullPath);
    this.props.closeMenu();
  };
}

export default compose(
  withCatalogARSFlag,
  connect(null, {
    removeDataset,
    removeFile,
    showConfirmationDialog,
  }),
  injectIntl
)(DatasetMenu);
