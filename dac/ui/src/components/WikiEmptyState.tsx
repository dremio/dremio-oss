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
import { intl } from "@app/utils/intl";
import EmptyStateContainer from "@app/pages/HomePage/components/EmptyStateContainer";
import * as classes from "./WikiEmptyState.module.less";

type WikiEmptyStateProps = {
  onAddWiki?: () => void;
};
const WikiEmptyState = (props: WikiEmptyStateProps) => {
  const { onAddWiki } = props;
  const { formatMessage } = intl;
  return (
    <EmptyStateContainer icon="interface/edit-wiki" title="No.Wiki.Content">
      {onAddWiki && (
        <span
          className={classes["edit-wiki-content"]}
          onClick={onAddWiki}
          data-qa="edit-wiki-content"
        >
          {formatMessage({ id: "Edit.Wiki.Content" })}
        </span>
      )}
    </EmptyStateContainer>
  );
};

export default WikiEmptyState;
