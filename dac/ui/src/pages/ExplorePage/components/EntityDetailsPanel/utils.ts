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
import { useEffect } from "react";
import { useResourceSnapshot, useResourceStatus } from "smart-resource/react";

import { EntityPanelResource } from "@app/resources/EntityResource";
import { transformEntity } from "@app/utils/entity-utils";
import Immutable from "immutable";

export const useEntityPanelDetails = (
  entity: any,
  handleEntityDetails: any,
) => {
  const [entityResource, entityError] =
    useResourceSnapshot(EntityPanelResource);
  const entityStatus = useResourceStatus(EntityPanelResource);
  const getEntityUrl = entity?.get("getEntityUrl");
  // Lazily fetch the entity URL so that TreeNodes don't have to compute it
  const entityUrl = getEntityUrl ? getEntityUrl() : entity?.get("entityUrl");
  const entityId = entity?.get("entityId");

  useEffect(() => {
    if (entityUrl) {
      EntityPanelResource.fetch({ entityUrl: entityUrl });
    }
  }, [entityUrl]);

  useEffect(() => {
    if (entityResource) {
      handleEntityDetails(
        Immutable.fromJS({ ...transformEntity(entityResource), entityId }),
      );
    }
  }, [entityResource, handleEntityDetails, entityId]);

  useEffect(() => {
    if (entityError) {
      handleEntityDetails(Immutable.fromJS({ error: true }));
    }
  }, [entityError, handleEntityDetails]);

  useEffect(() => {
    return () => {
      EntityPanelResource.reset();
    };
  }, []);

  return entityStatus;
};
