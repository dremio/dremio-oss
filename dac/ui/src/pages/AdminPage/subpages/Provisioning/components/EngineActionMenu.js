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
import Immutable from "immutable";
import Menu from "@app/components/Menus/Menu";
import MenuItem from "@app/components/Menus/MenuItem";
import { getIsInReadOnlyState } from "@app/pages/AdminPage/subpages/Provisioning/provisioningUtils";

export class EngineActionMenu extends PureComponent {
  static propTypes = {
    engine: PropTypes.instanceOf(Immutable.Map).isRequired,

    editHandler: PropTypes.func,
    deleteHandler: PropTypes.func,
    addRemoveHandler: PropTypes.func,
    showConfirmationDialog: PropTypes.func,
    closeMenu: PropTypes.func,
  };

  handleEdit = (e) => {
    e.stopPropagation();
    e.preventDefault();
    const { closeMenu, editHandler, engine } = this.props;
    closeMenu();
    editHandler(engine);
  };
  handleAddRemove = (e) => {
    e.stopPropagation();
    e.preventDefault();
    const { closeMenu, addRemoveHandler, engine } = this.props;
    closeMenu();
    addRemoveHandler(engine);
  };
  handleDelete = (e) => {
    e.stopPropagation();
    e.preventDefault();
    const { closeMenu, deleteHandler, engine } = this.props;
    closeMenu();
    deleteHandler(engine);
  };

  render() {
    const { engine } = this.props;
    const isReadOnly = getIsInReadOnlyState(engine);
    const canEdit = !isReadOnly;
    const canDelete = true;
    const canAddRemove = engine.get("clusterType") === "YARN" && !isReadOnly;

    return (
      <Menu>
        {canEdit && <MenuItem onClick={this.handleEdit}>{la("Edit")}</MenuItem>}
        {canDelete && (
          <MenuItem onClick={this.handleDelete}>{la("Delete")}</MenuItem>
        )}
        {canAddRemove && (
          <MenuItem onClick={this.handleAddRemove}>
            {la("Add/Remove Executors")}
          </MenuItem>
        )}
      </Menu>
    );
  }
}
