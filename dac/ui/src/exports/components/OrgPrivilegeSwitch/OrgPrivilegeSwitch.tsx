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

import { useSelector } from "react-redux";

type Props = {
  privilege: string | string[];
  renderEnabled: () => JSX.Element;
  renderDisabled?: () => JSX.Element;
  renderPending?: () => JSX.Element;
};

const nullRender = () => null;

/**
 * Conditionally renders children based on the provided org privilege
 */
export const OrgPrivilegeSwitch = (props: Props): JSX.Element | null => {
  const {
    renderDisabled = nullRender,
    renderEnabled,
    renderPending = nullRender,
  } = props;
  const result = useSelector((state: Record<string, any>) => {
    const orgPrivileges = state.privileges.organization;
    if (typeof props.privilege === "string") {
      return orgPrivileges[props.privilege];
    } else {
      return orgPrivileges[props.privilege[0]]?.[props.privilege[1]];
    }
  });

  if (result === undefined) {
    return renderPending();
  }

  if (!result) {
    return renderDisabled();
  }

  return renderEnabled();
};
