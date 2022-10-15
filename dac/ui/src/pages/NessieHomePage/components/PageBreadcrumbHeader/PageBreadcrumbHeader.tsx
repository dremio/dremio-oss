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
import { useIntl } from "react-intl";
//@ts-ignore
import { CopyToClipboard } from "dremio-ui-lib";
import BranchPicker from "@app/pages/HomePage/components/BranchPicker/BranchPicker";
import NessieBreadcrumb from "../NessieBreadcrumb/NessieBreadcrumb";
import { useNessieContext } from "../../utils/context";
import { isDefaultReferenceLoading } from "@app/selectors/nessie/nessie";
import {
  constructArcticUrl,
  useArcticCatalogContext,
} from "@app/exports/pages/ArcticCatalog/arctic-catalog-utils";
import ArcticBreadcrumb from "@app/exports/pages/ArcticCatalog/components/ArcticBreadcrumb/ArcticBreadcrumb";

import "./PageBreadcrumbHeader.less";

function PageBreadcrumbHeader({
  path,
  rightContent,
  hasBranchPicker = true,
  className = "",
}: {
  path?: string[];
  rightContent?: any;
  hasBranchPicker?: boolean;
  className?: string;
}) {
  const intl = useIntl();
  const { source, state, baseUrl } = useNessieContext();
  const arcticCtx = useArcticCatalogContext();
  const Breadcrumb = arcticCtx ? ArcticBreadcrumb : (NessieBreadcrumb as any);
  const redirectUrl = arcticCtx
    ? constructArcticUrl({
        type: arcticCtx.isCatalog ? "catalog" : "source",
        baseUrl: baseUrl,
        tab: arcticCtx?.activeTab,
        namespace: "",
      })
    : "/";

  return (
    <div className={`pageBreadcrumbHeader ${className}`}>
      <span className="pageBreadcrumbHeader-crumbContainer">
        <Breadcrumb path={path} />
        {hasBranchPicker && !isDefaultReferenceLoading(state) && (
          <BranchPicker redirectUrl={redirectUrl} />
        )}
        <span className="pageBreadcrumbHeader-copyButton">
          <CopyToClipboard
            tooltipText={intl.formatMessage({ id: "Common.PathCopied" })}
            value={[source.name, ...(path || [])].join(".")}
          />
        </span>
      </span>
      <span className="pageBreadcrumbHeader-rightContent">{rightContent}</span>
    </div>
  );
}
export default PageBreadcrumbHeader;
