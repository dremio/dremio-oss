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
import { FormattedMessage } from "react-intl";
import PropTypes from "prop-types";
import { ModalForm, modalFormProps } from "components/Forms";
import { connectComplexForm } from "components/Forms/connectComplexForm";
import ResourceTreeContainer from "components/Tree/ResourceTreeContainer";

import { NESSIE_REF_PREFIX } from "#oss/constants/nessie";
import { Button, IconButton, DialogContent } from "dremio-ui-lib/components";
import { TreeConfigContext } from "#oss/components/Tree/treeConfigContext";

import * as classes from "./SelectContextForm.module.less";

export const FIELDS = ["context"];

export class SelectContextForm extends Component {
  static propTypes = {
    handleSubmit: PropTypes.func.isRequired,
    onFormSubmit: PropTypes.func,
    onCancel: PropTypes.func,
    fields: PropTypes.object,

    getPreselectedNode: PropTypes.func,
    filterTree: PropTypes.func,
  };

  constructor(props) {
    super(props);

    this.handleChangeSelectedNode = this.handleChangeSelectedNode.bind(this);
  }

  handleChangeSelectedNode(nodeId) {
    this.props.fields.context.onChange(nodeId);
  }

  render() {
    const {
      fields,
      handleSubmit,
      onFormSubmit,
      onCancel,
      getPreselectedNode,
      filterTree,
    } = this.props;
    const nodeId = fields.context.value || fields.context.initialValue || "";

    return (
      <div className="select-context" style={{ height: "100%" }}>
        <ModalForm
          {...modalFormProps(this.props)}
          onSubmit={handleSubmit(onFormSubmit)}
          renderFooter={() => null}
        >
          <DialogContent
            title={<FormattedMessage id="ContextPicker.Title" />}
            toolbar={
              <IconButton aria-label="Close" onClick={onCancel}>
                <dremio-icon name="interface/close-small" />
              </IconButton>
            }
            actions={
              <>
                <Button variant="secondary" onClick={onCancel}>
                  <FormattedMessage id="Common.Cancel" />
                </Button>
                <Button
                  variant="secondary"
                  onClick={() => {
                    onFormSubmit("");
                  }}
                >
                  <FormattedMessage id="ContextPicker.Action.ClearContext" />
                </Button>
                <Button variant="primary" type="submit" data-qa="confirm">
                  <FormattedMessage id="Common.Select" />
                </Button>
              </>
            }
          >
            <div className={classes["resourceTreeContainer"]}>
              <TreeConfigContext.Provider
                value={{
                  nessiePrefix: NESSIE_REF_PREFIX,
                  filterTree,
                }}
              >
                <ResourceTreeContainer
                  fromModal
                  hideDatasets
                  onChange={this.handleChangeSelectedNode}
                  preselectedNodeId={getPreselectedNode?.(nodeId) || nodeId}
                />
              </TreeConfigContext.Provider>
            </div>
          </DialogContent>
        </ModalForm>
      </div>
    );
  }
}

export default connectComplexForm(
  {
    form: "selectContextForm",
    fields: FIELDS,
  },
  [],
  null,
  null,
)(SelectContextForm);
