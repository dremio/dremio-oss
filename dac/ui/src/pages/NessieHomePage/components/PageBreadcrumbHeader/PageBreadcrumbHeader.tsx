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
import { withRouter } from "react-router";
import { isEqual } from "lodash";
import { useIntl } from "react-intl";
import BranchPicker from "#oss/pages/HomePage/components/BranchPicker/BranchPicker";
import { useNessieContext } from "../../utils/context";
import { isDefaultReferenceLoading } from "#oss/selectors/nessie/nessie";
import {
  constructVersionedEntityUrl,
  useVersionedPageContext,
} from "#oss/exports/pages/VersionedHomePage/versioned-page-utils";
import VersionedPageBreadcrumb from "#oss/exports/pages/VersionedHomePage/components/VersionedPageBreadcrumb/VersionedPageBreadcrumb";
import { CopyButton } from "dremio-ui-lib/components";

import "./PageBreadcrumbHeader.less";

function PageBreadcrumbHeader({
  path,
  rightContent,
  hasBranchPicker = true,
  className = "",
  router,
}: {
  path?: string[];
  rightContent?: any;
  hasBranchPicker?: boolean;
  className?: string;
  router?: any;
}) {
  const intl = useIntl();
  const { source, state, baseUrl } = useNessieContext();
  const versionCtx = useVersionedPageContext();
  const redirectUrl = constructVersionedEntityUrl({
    type: versionCtx.isCatalog ? "catalog" : "source",
    baseUrl: baseUrl,
    tab: versionCtx?.activeTab,
    namespace: "",
  });

  const handleApply = (_: string, newState: any) => {
    if (
      !isEqual(newState, {
        hash: state.hash,
        date: state.date,
        reference: state.reference,
      })
    ) {
      router.push(redirectUrl);
    }
  };

  return (
    <div className={`pageBreadcrumbHeader ${className}`}>
      <span className="pageBreadcrumbHeader-crumbContainer">
        <VersionedPageBreadcrumb path={path} />
        {hasBranchPicker && !isDefaultReferenceLoading(state) && (
          <BranchPicker onApply={handleApply} />
        )}
        <span className="pageBreadcrumbHeader-copyButton">
          <CopyButton
            contents={[source.name, ...(path || [])].join(".")}
            size="L"
            copyTooltipLabel={intl.formatMessage({ id: "Common.CopyPath" })}
          />
        </span>
      </span>
      <span className="pageBreadcrumbHeader-rightContent">{rightContent}</span>
    </div>
  );
}
export default withRouter(PageBreadcrumbHeader);
