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
import Immutable from "immutable";
import { VersionContextType } from "dremio-ui-common/components/VersionContext.js";
import { useResourceSnapshot } from "smart-resource/react";
import { EntityOverlayResource } from "@app/resources/EntityResource";
import { useEffect } from "react";
import DatasetSummaryError from "../DatasetSummary/DatasetSummaryError/DatasetSummaryError";
import { constructFullPath } from "@app/utils/pathUtils";
import EntitySummary from "./EntitySummary";
import { transformEntity } from "@app/utils/entity-utils";

type EntitySummaryOverlayProps = {
  name: string;
  entity: any;
  fullPath: Immutable.List<string>;
  type: string;
  openDetailsPanel: (dataset: any) => void;
  entityUrl?: string;
  versionContext?: VersionContextType;
  getEntityUrl?: () => string[];
};

const EntitySummaryOverlay = ({
  entity,
  entityUrl,
  fullPath,
  name,
  type,
  versionContext,
  openDetailsPanel,
  getEntityUrl,
}: EntitySummaryOverlayProps) => {
  const [entityResource, entityResourceErr] = useResourceSnapshot(
    EntityOverlayResource
  );
  const url = getEntityUrl ? getEntityUrl() : entityUrl;

  useEffect(() => {
    if (!entity) EntityOverlayResource.fetch({ entityUrl: url });
  }, [entityUrl, entity, url]);

  const constructedFullPath = constructFullPath(fullPath);
  const transformedEntity = transformEntity(entityResource || {});
  const isResourceLoaded = transformedEntity.name === fullPath.last();
  const curEntity = entity?.toJS() || {
    ...transformedEntity,
    name: fullPath.last(),
  };

  return entityResourceErr ? (
    <DatasetSummaryError
      is403={false}
      fullPath={constructedFullPath}
      title={name}
      datasetType="blue-folder"
      versionContext={versionContext}
    />
  ) : !curEntity ? null : (
    <EntitySummary
      entity={curEntity}
      fullPath={constructedFullPath}
      type={type}
      versionContext={versionContext}
      openDetailsPanel={openDetailsPanel}
      sourceType={transformedEntity.type}
      isLoading={!isResourceLoaded}
    />
  );
};

export default EntitySummaryOverlay;
