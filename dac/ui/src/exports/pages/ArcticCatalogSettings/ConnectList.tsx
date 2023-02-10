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

import { ExternalLink } from "dremio-ui-lib/dist-esm";
import classes from "./ConnectList.module.less";
import sparkIcon from "dremio-ui-lib/icons/dremio/corporate/spark.svg";
import flinkIcon from "dremio-ui-lib/icons/dremio/corporate/flink.svg";
import sonarIcon from "dremio-ui-lib/icons/dremio/corporate/sonar.svg";
import AddToSonarButton from "@app/exports/components/AddToSonarDialog/AddToSonarButton";
import { ArcticCatalog } from "@app/exports/endpoints/ArcticCatalogs/ArcticCatalog.type";

export const ConnectList = ({ catalog }: { catalog: ArcticCatalog }) => {
  return (
    <ul className={classes["connect-list"]}>
      <li className={classes["connect-list__item"]}>
        <span>
          <img src={sonarIcon} alt="" /> Add as a Source to a Sonar Project
        </span>
        <AddToSonarButton catalog={catalog} />
      </li>
      <li className={classes["connect-list__item"]}>
        <span>
          <img src={sparkIcon} alt="" /> Connect from Spark
        </span>
        <ExternalLink
          variant="list"
          href="https://docs.dremio.com/cloud/getting-started/connecting-spark/"
        >
          Learn more
        </ExternalLink>
      </li>
      <li className={classes["connect-list__item"]}>
        <span>
          <img src={flinkIcon} alt="" /> Connect from Flink
        </span>
        <ExternalLink
          variant="list"
          href="https://docs.dremio.com/cloud/getting-started/connecting-flink/"
        >
          Learn more
        </ExternalLink>
      </li>
    </ul>
  );
};
