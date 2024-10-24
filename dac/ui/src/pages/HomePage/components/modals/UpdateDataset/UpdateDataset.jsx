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
import { connect } from "react-redux";
import PropTypes from "prop-types";
import Immutable from "immutable";

import Modal, { ModalSize } from "components/Modals/Modal";
import {
  CANCEL,
  CUSTOM,
  NEXT,
  PRIMARY_DANGER,
} from "components/Buttons/ButtonTypes";
import {
  moveDataSet,
  createDatasetFromExisting,
} from "actions/explore/sqlActions";
import { convertDatasetToFolder } from "actions/home";
import {
  renameSpaceDataset,
  loadDependentDatasets,
  removeDataset,
  removeFileFormat,
} from "actions/resources/spaceDetails";
import { loadSourceListData } from "actions/resources/sources";
import { getDescendantsList } from "selectors/resources";
import ApiUtils from "utils/apiUtils/apiUtils";
import { constructFullPath, splitFullPath } from "utils/pathUtils";

import { TOGGLE_VIEW_ID } from "components/RightContext/FolderContext";

import UpdateDatasetView, { UpdateMode } from "./UpdateDatasetView";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

export class UpdateDataset extends PureComponent {
  static propTypes = {
    routeParams: PropTypes.object,
    location: PropTypes.object,
    item: PropTypes.instanceOf(Immutable.Map),
    isOpen: PropTypes.bool.isRequired,
    hide: PropTypes.func.isRequired,
    query: PropTypes.object.isRequired,
    createDatasetFromExisting: PropTypes.func.isRequired,
    moveDataSet: PropTypes.func.isRequired,
    removeDataset: PropTypes.func.isRequired,
    removeFileFormat: PropTypes.func.isRequired,
    convertDatasetToFolder: PropTypes.func.isRequired,
    renameSpaceDataset: PropTypes.func.isRequired,
    loadDependentDatasets: PropTypes.func.isRequired,
    dependentDatasets: PropTypes.array,
    space: PropTypes.object,
    pathname: PropTypes.string,
    queryContext: PropTypes.instanceOf(Immutable.List),
    intl: PropTypes.object.isRequired,
    loadSourceListData: PropTypes.func.isRequired,
  };

  constructor(props) {
    const { t } = getIntlContext();
    super(props);
    this.config = {
      [UpdateMode.rename]: (dependentDatasets) => {
        const hasDeps = dependentDatasets && dependentDatasets.length;
        const buttons = [
          {
            name: t("Common.Actions.Cancel"),
            key: "cancel",
            type: CANCEL,
          },
          {
            name: t("Common.Actions.MakeCopy"),
            key: "copyDataset",
            type: hasDeps ? NEXT : CUSTOM,
          },
          {
            name: t(
              hasDeps ? "Common.Actions.RenameAnyway" : "Common.Actions.Rename",
            ),
            key: "renameDataset",
            type: hasDeps ? CUSTOM : NEXT,
          },
        ];

        return {
          title: t("Dataset.RenameDataset"),
          hidePath: true,
          buttons,
        };
      },
      [UpdateMode.move]: (dependentDatasets) => {
        const hasDeps = dependentDatasets && dependentDatasets.length;
        const buttons = [
          {
            name: t("Common.Actions.Cancel"),
            key: "cancel",
            type: CANCEL,
          },
          {
            name: t("Common.Actions.MakeCopy"),
            key: "copyDataset",
            type: hasDeps ? NEXT : CUSTOM,
          },
          {
            name: t(
              hasDeps ? "Common.Actions.MoveAnyway" : "Common.Actions.Move",
            ),
            key: "moveDataset",
            type: hasDeps ? CUSTOM : NEXT,
          },
        ];
        return {
          title: t("Dataset.MoveDataset"),
          buttons,
        };
      },
      [UpdateMode.remove]: () => ({
        title: t("Dataset.DeleteDataset"),
        hidePath: true,
        buttons: [
          {
            name: t("Common.Actions.Cancel"),
            key: "cancel",
            type: CANCEL,
          },
          {
            name: t("Common.Actions.Delete"),
            key: "removeDataset",
            type: PRIMARY_DANGER,
          },
        ],
      }),
      [UpdateMode.removeFormat]: () => ({
        title: t("Dataset.Actions.RemoveFormat"),
        hidePath: true,
        buttons: [
          {
            name: t("Common.Actions.Cancel"),
            key: "cancel",
            type: CANCEL,
          },
          {
            name: t("Dataset.Actions.RemoveFormat"),
            key: "removeFormat",
            type: NEXT,
          },
        ],
      }),
    };
  }

  UNSAFE_componentWillMount() {
    this.receiveProps(this.props, {});
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  getCurrentFullPath = (item = this.props.item) =>
    item && item.get("fullPathList");

  getNewFullPath = (datasetName, selectedEntity) =>
    splitFullPath(selectedEntity).concat(datasetName);

  receiveProps = (nextProps, oldProps) => {
    if (!oldProps.item && nextProps.item) {
      nextProps.loadDependentDatasets(nextProps.item.get("fullPathList"));
    }
  };

  copyDataset = ({ datasetName, selectedEntity }) => {
    const cPathFrom = this.getCurrentFullPath();
    const cPathTo = selectedEntity
      ? this.getNewFullPath(datasetName, selectedEntity)
      : [cPathFrom.get(0), datasetName]; // space is not selected during rename, use cPathFrom
    return this.props.createDatasetFromExisting(cPathFrom, cPathTo, {
      name: datasetName,
    });
  };

  moveDataset = ({ datasetName, selectedEntity }) => {
    const pathFrom = this.getCurrentFullPath();
    const cPathTo = this.getNewFullPath(datasetName, selectedEntity);
    return this.props.moveDataSet(pathFrom, cPathTo);
  };

  renameDataset = ({ datasetName }) => {
    return this.props.renameSpaceDataset(this.props.item, datasetName);
  };

  removeDataset = () => {
    return this.props.removeDataset(this.props.item);
  };

  removeFormat = () => {
    const { item } = this.props;
    if (item.get("entityType") === "file") {
      return this.props.removeFileFormat(item);
    } else {
      return this.props.convertDatasetToFolder(item, TOGGLE_VIEW_ID);
    }
  };

  submit = (keyAction, values) => {
    return ApiUtils.attachFormSubmitHandlers(this[keyAction](values)).then(
      (res) => {
        if (res && !res.error) {
          if (keyAction === UpdateMode.removeFormat) {
            this.props.loadSourceListData();
          }
          this.props.hide();
        }
        return null;
      },
    );
  };

  render() {
    const { dependentDatasets, query, hide, isOpen, item } = this.props;
    const { mode } = query;
    const config = mode && this.config[mode](dependentDatasets);
    const fullPath = this.getCurrentFullPath();
    // initialPath should be the parent folder
    const initialPath = fullPath
      ? constructFullPath(fullPath.slice(0, -1))
      : null;
    // use smaller popup for remove and removeFormat w/o dependencies
    const size =
      (mode === UpdateMode.remove || mode === UpdateMode.removeFormat) &&
      !(dependentDatasets && dependentDatasets.length)
        ? ModalSize.smallest
        : ModalSize.small;

    const datasetView = config ? (
      <UpdateDatasetView
        hide={hide}
        initialPath={initialPath}
        name={query.name}
        buttons={config.buttons}
        hidePath={config.hidePath}
        dependentDatasets={dependentDatasets}
        getGraphLink={query.getGraphLink}
        mode={mode}
        item={item}
        size={size}
        submit={this.submit}
      />
    ) : null;
    return (
      <Modal
        hide={hide}
        size={size}
        isOpen={isOpen}
        title={config ? config.title : ""}
      >
        {datasetView}
      </Modal>
    );
  }
}

const mapStateToProps = (state, ownProps) => {
  return {
    dependentDatasets: getDescendantsList(state),
    // todo: should not need to normalize due to location state & reload
    item: Immutable.fromJS(ownProps.item),
  };
};

export default connect(mapStateToProps, {
  renameSpaceDataset,
  createDatasetFromExisting,
  moveDataSet,
  removeDataset,
  removeFileFormat,
  convertDatasetToFolder,
  loadDependentDatasets,
  loadSourceListData,
})(UpdateDataset);
