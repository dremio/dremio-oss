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
//@ts-ignore
import sonarPreviewImg from "./images/sonar-preview.png";
import { ServiceCard } from "./ServiceCard";
import { Tooltip } from "dremio-ui-lib";
import { ExternalLink } from "dremio-ui-lib/components";
import * as PATHS from "@app/exports/paths";

type Props = {
  action: JSX.Element;
};

export const SonarServiceCard = (props: Props): JSX.Element => {
  return (
    <ServiceCard
      action={props.action}
      href={PATHS.sonarProjects()}
      showcaseImage={<img src={sonarPreviewImg} alt="" />}
      serviceName={intl.formatMessage({ id: "Brand.Sonar" })}
      serviceIconName="corporate/sonar"
      description={
        <>
          A lakehouse query engine that allows analysts to explore data with
          sub-second query response times.{" "}
          <Tooltip
            enterDelay={0}
            title={
              <div
                style={{
                  lineHeight: "20px",
                  marginBlock: "-16px",
                  padding: "var(--dremio--spacing--2)",
                }}
              >
                <p>
                  <strong>Sonar</strong>
                </p>
                <p>
                  Sonar is a lakehouse query engine that lets analysts explore
                  data with sub-second query response times and allows data
                  engineers to ingest and transform data with DML operations.
                  Additionally, Sonar connects to external databases so you
                  don’t have to move all your data into object storage in order
                  to query it.
                </p>

                <div style={{ marginTop: "1em" }}>
                  <p>
                    <strong>Read more on docs…</strong>
                  </p>
                  <ul
                    onClick={(e: any) => {
                      e.stopPropagation();
                    }}
                  >
                    <li>
                      <ExternalLink
                        href="https://docs.dremio.com/cloud/getting-started/"
                        variant="list"
                      >
                        Get Started with Dremio
                      </ExternalLink>
                    </li>
                    <li>
                      <ExternalLink
                        href="https://docs.dremio.com/cloud/sources/"
                        variant="list"
                      >
                        Connecting to Your Data
                      </ExternalLink>
                    </li>
                  </ul>
                </div>
              </div>
            }
            placement="right"
            type="richTooltip"
          >
            <span className="link">Learn more.</span>
          </Tooltip>
        </>
      }
    />
  );
};
