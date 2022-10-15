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
import arcticPreviewImg from "./images/arctic-preview.png";
import { ServiceCard } from "./ServiceCard";
import { Tooltip } from "dremio-ui-lib";
import { ExternalLink } from "dremio-ui-lib/dist-esm";

type Props = {
  action: JSX.Element;
};

export const ArcticServiceCard = (props: Props): JSX.Element => {
  return (
    <ServiceCard
      action={props.action}
      showcaseImage={<img src={arcticPreviewImg} alt="" />}
      serviceName={intl.formatMessage({ id: "Brand.Arctic" })}
      serviceIconName="corporate/arctic"
      description={
        <>
          {" "}
          A catalog for Iceberg tables that enables data to be managed like code
          with Git-like capabilities.{" "}
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
                  <strong>Arctic</strong>
                </p>
                <p>
                  Arctic is a lakehouse catalog for Iceberg tables that enables
                  data to be managed like code with Git-like capabilities such
                  as branches and tags. Arctic works with multiple query engines
                  (Dremio, Sonar, Spark).
                </p>

                <div style={{ marginTop: "1em" }}>
                  <p>
                    <strong>Read more on docs…</strong>
                  </p>
                  <ul>
                    <li>
                      <ExternalLink href="#" variant="list">
                        Git-like data management
                      </ExternalLink>
                    </li>
                    <li>
                      <ExternalLink href="#" variant="list">
                        Supported engines
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
