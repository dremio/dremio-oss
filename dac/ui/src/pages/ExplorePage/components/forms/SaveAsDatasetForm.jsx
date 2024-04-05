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
import { Component, createRef } from "react";

import PropTypes from "prop-types";
import { intl } from "@app/utils/intl";
import { ModalForm, FormBody, modalFormProps } from "components/Forms";
import { FieldWithError, TextField } from "components/Fields";
import { applyValidators, isRequired } from "utils/validation";
import { getInitialResourceLocation, constructFullPath } from "utils/pathUtils";
import ResourceTreeContainer from "components/Tree/ResourceTreeContainer";
import { connectComplexForm } from "components/Forms/connectComplexForm";
import { AddFolderDialog } from "components/AddFolderDialog/AddFolderDialog";
import Message from "components/Message";
import { formRow, label } from "uiTheme/radium/forms";
import {
  TreeConfigContext,
  defaultConfigContext,
} from "@app/components/Tree/treeConfigContext";
import { withFilterTreeArs } from "@app/utils/datasetTreeUtils";
import { getHomeSource, getSortedSources } from "@app/selectors/home";
import { withCatalogARSFlag } from "@inject/utils/arsUtils";
import { getSourceByName as getNessieSourceByName } from "@app/utils/nessieUtils";
import { getRefQueryParams } from "@app/utils/nessieUtils";
import { Button } from "dremio-ui-lib/components";
import * as classes from "./SaveAsDatasetForm.module.less";

export const FIELDS = ["name", "location", "reapply"];

function validate(values) {
  return applyValidators(values, [isRequired("name"), isRequired("location")]);
}

const locationType = PropTypes.string;
// I would like to enforce that initial value and field value for location has the same type,
// as inconsistency in these 2 parameters, cause redux-form treat a form as dirty.
// I created this as function, not as standalone object, to avoid eslint errors that requires to
// document the rest of redux-form properties: onChange, error, touched
const getLocationPropType = () =>
  PropTypes.shape({
    value: locationType,
  });

export class SaveAsDatasetForm extends Component {
  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    message: PropTypes.string,
    canReapply: PropTypes.bool,
    datasetType: PropTypes.string,
    handleSubmit: PropTypes.func.isRequired,
    dependentDatasets: PropTypes.array,
    updateFormDirtyState: PropTypes.func,
    homeSource: PropTypes.object,
    sources: PropTypes.array,
    nessie: PropTypes.array,

    // redux-form
    initialValues: PropTypes.shape({
      location: locationType,
    }),
    fields: PropTypes.shape({
      location: getLocationPropType(),
      name: PropTypes.object,
    }),

    // HOC
    filterTree: PropTypes.func,
    isArsEnabled: PropTypes.bool,
  };
  ref = createRef(null);
  state = {
    addFolderDialogOpen: false,
    node: null,
    showAddFolderButton: true,
    preselectedNodeId:
      this.props.isArsEnabled && this.props.homeSource
        ? this.props.homeSource.get("name")
        : this.props.fields.location.initialValue,
    selectedVersionContext: null,
  };

  static contextTypes = {
    location: PropTypes.object,
  };

  componentDidMount() {
    const { nessie } = this.props;
    const selectedVersionContext = getRefQueryParams(
      nessie,
      this.state.preselectedNodeId
    );
    this.setState({
      selectedVersionContext,
      showAddFolderButton: this.showAddFolderButton(selectedVersionContext),
    });
  }

  showAddFolderButton = (versionContext) => {
    if (versionContext?.refType) {
      if (versionContext.refType !== "BRANCH") {
        return false;
      }
      return true;
    }
    return true;
  };

  handleChangeSelectedNode = (nodeId, node) => {
    const { onChange } = this.props.fields.location;
    if (!nodeId && !node) {
      onChange(undefined); //Clears selection
      this.setState({
        node: undefined,
        selectedVersionContext: null,
        showAddFolderButton: false,
      });
    } else {
      onChange(
        (node && constructFullPath(node.get("fullPath").toJS())) || nodeId
      );
      if (node) {
        const selectedVersionContext = getRefQueryParams(
          this.props.nessie,
          node.get("fullPath").first()
        );
        this.setState({
          node,
          selectedVersionContext,
          showAddFolderButton: this.showAddFolderButton(selectedVersionContext),
        });
      }
    }
  };

  renderHistoryWarning() {
    const { version, tipVersion } = this.context.location.query;
    if (tipVersion && tipVersion !== version) {
      return (
        <Message
          messageType="warning"
          message={laDeprecated("You may lose your previous changes.")}
          isDismissable={false}
        />
      );
    }

    return null;
  }

  submitOnEnter = (preventSubmit) => (e) => {
    const { handleSubmit, onFormSubmit } = this.props;
    if (e.key === "Enter") {
      e.preventDefault();
      if (preventSubmit) {
        return;
      }
      handleSubmit(onFormSubmit)();
    }
  };

  render() {
    const {
      fields: { name, location },
      handleSubmit,
      onFormSubmit,
      message,
      filterTree,
      isArsEnabled,
      sources,
    } = this.props;
    const preventSubmit = !location.value;
    return (
      <>
        <ModalForm
          {...modalFormProps(this.props)}
          {...(preventSubmit && { canSubmit: false })}
          onSubmit={handleSubmit(onFormSubmit)}
          wrapperStyle={{
            height: "100%",
            display: "flex",
            flexDirection: "column",
          }}
          footerChildren={
            this.state.showAddFolderButton && (
              <Button
                style={{ marginRight: "310px" }}
                variant="secondary"
                onClick={() => {
                  this.setState({ addFolderDialogOpen: true });
                }}
              >
                {intl.formatMessage({ id: "Create.Folder" })}
              </Button>
            )
          }
        >
          {this.renderHistoryWarning()}
          <FormBody className={classes["form-body"]}>
            {message && <div style={formRow}>{message}</div>}
            <div style={formRow}>
              <FieldWithError label="Name" {...name}>
                <TextField
                  initialFocus
                  {...name}
                  onKeyDown={this.submitOnEnter(preventSubmit)}
                />
              </FieldWithError>
            </div>
            <div className={classes["tree-container"]}>
              <label style={label}>Location</label>
              <TreeConfigContext.Provider
                value={{
                  ...defaultConfigContext,
                  restrictSelection: true,
                  resourceTreeControllerRef: this.ref,
                  filterTree: (tree) =>
                    filterTree(
                      // Only show nessie sources in save as dialog
                      tree.filter(
                        (node) =>
                          node.get("type") !== "SOURCE" ||
                          !!getNessieSourceByName(node.get("name"), sources)
                      )
                    ),
                }}
              >
                <ResourceTreeContainer
                  className={classes["resource-tree"]}
                  hideDatasets
                  onChange={this.handleChangeSelectedNode}
                  preselectedNodeId={this.state.preselectedNodeId}
                  fromModal
                />
                {this.state.addFolderDialogOpen && (
                  <AddFolderDialog
                    open={this.state.addFolderDialogOpen}
                    close={() => {
                      this.setState({ addFolderDialogOpen: false });
                    }}
                    node={this.state.node}
                    preselectedNodeId={this.state.preselectedNodeId}
                    selectedVersionContext={this.state.selectedVersionContext}
                    isArsEnabled={isArsEnabled}
                  />
                )}
              </TreeConfigContext.Provider>
              {this.props.fields.location.error &&
                this.props.fields.location.touched && (
                  <Message
                    messageType="error"
                    message={this.props.fields.location.error}
                  />
                )}
            </div>
          </FormBody>
        </ModalForm>
      </>
    );
  }
}

const mapStateToProps = (state, props) => {
  const sources = getSortedSources(state);
  return {
    initialValues: {
      location: getInitialResourceLocation(
        props.fullPath,
        props.datasetType,
        state.account.getIn(["user", "userName"])
      ),
    },
    sources: sources,
    homeSource: getHomeSource(sources),
    nessie: state.nessie,
  };
};

export default connectComplexForm(
  {
    form: "saveAsDataset",
    fields: FIELDS,
    validate,
  },
  [],
  mapStateToProps,
  null
)(withFilterTreeArs(withCatalogARSFlag(SaveAsDatasetForm)));
