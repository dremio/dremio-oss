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
import { type FunctionComponent } from "react";
import { Avatar, Card } from "dremio-ui-lib/dist-esm";
import classes from "../ArcticCatalogCard/ArcticCatalogCard.less";
import { nameToInitials } from "../../utilities/nameToInitials";
import { formatFixedDateTimeLong } from "../../utilities/formatDate";

type ArcticCatalogCardProps = {
  project: any;
};

export const SonarProjectCard: FunctionComponent<ArcticCatalogCardProps> = (
  props
) => {
  const { project } = props;
  const { formatMessage } = intl;
  return (
    <Card
      title={
        <>
          <dremio-icon
            name="brand/arctic-catalog-source"
            class={classes["arctic-catalog-card__title-icon"]}
            alt=""
          ></dremio-icon>
          <h2>{project.name}</h2>
        </>
      }
    >
      <dl className="dremio-description-list">
        <dt>{formatMessage({ id: "Common.Owner" })}</dt>
        <dd>
          <Avatar
            initials={nameToInitials(
              project.createdByDetails?.fullname || project.createdBy
            )}
            style={{ marginRight: "var(--dremio--spacing--05)" }}
          />
          {project.createdByDetails?.fullname || project.createdBy}
        </dd>
        {/* <dt>Cloud</dt>
        <dd>{project.cloudId}</dd> */}
        <dt>{formatMessage({ id: "Common.CreatedOn" })}</dt>
        <dd>{formatFixedDateTimeLong(project.createdAt)}</dd>
        {/* <dt>Engines</dt>
        <dd>?</dd> */}
        <dt>{formatMessage({ id: "Common.Status" })}</dt>
        <dd>{project.state}</dd>
      </dl>
    </Card>
  );
};
