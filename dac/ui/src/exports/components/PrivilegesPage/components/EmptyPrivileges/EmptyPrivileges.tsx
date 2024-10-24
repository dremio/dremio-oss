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

import EmptyStateContainer from "#oss/pages/HomePage/components/EmptyStateContainer";
import { useIntl } from "react-intl";
import * as classes from "./EmptyPrivileges.module.less";

const EmptyPrivileges = ({ onClick }: { onClick: () => void }) => {
  const { formatMessage } = useIntl();
  return (
    <div className={classes["emptyContainer__action"]}>
      <EmptyStateContainer
        icon="interface/privilege"
        title="Admin.Privileges.EmptyState"
      >
        <span className={classes["emptyContainer__button"]} onClick={onClick}>
          {formatMessage({ id: "Admin.Privileges.EmptyState.Action" })}
        </span>
      </EmptyStateContainer>
    </div>
  );
};

export default EmptyPrivileges;
