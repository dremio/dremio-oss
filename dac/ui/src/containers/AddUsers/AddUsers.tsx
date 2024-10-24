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

import { useDispatch, useSelector } from "react-redux";
import { hideAddUsers } from "#oss/actions/addUsers";
import AddUserModal from "@inject/pages/SettingPage/subpages/Users/AddUserModal";
import "./AddUsers.less";

function AddUsersContainer() {
  const dispatch = useDispatch();

  const isAddUsersOpen: boolean = useSelector(
    (state: Record<string, any>) => state.addUsers.isOpen,
  );

  const closeModal = () => {
    dispatch(hideAddUsers());
  };

  return (
    <AddUserModal
      open={isAddUsersOpen}
      setOpen={closeModal}
      classes={{
        root: "addUsersContainer__root",
      }}
    />
  );
}

export default AddUsersContainer;
