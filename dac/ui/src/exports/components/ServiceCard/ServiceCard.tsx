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

import { type FunctionComponent } from "react";
import { Card } from "dremio-ui-lib/components";
import classes from "./ServiceCard.less";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";

type ServiceCardProps = {
  action: JSX.Element;
  description: JSX.Element | string;
  serviceIconName: string;
  serviceName: string;
  showcaseImage: JSX.Element;
  href?: string;
};

const LinkWrapper = ({
  href,
  children,
  className,
}: {
  href?: string;
  className: string;
  children: any;
}) => {
  if (!href) return children;
  return (
    <LinkWithRef className={className} to={href}>
      {children}
    </LinkWithRef>
  );
};

export const ServiceCard: FunctionComponent<ServiceCardProps> = (props) => {
  return (
    <Card className={classes["service-card"]}>
      <LinkWrapper className={classes["service-card__link"]} href={props.href}>
        <div className={classes["service-card__content"]}>
          <div className={classes["service-card__showcase-container"]}>
            {props.showcaseImage}
          </div>

          <div className={classes["service-card__title"]}>
            <dremio-icon
              name={props.serviceIconName}
              class={classes["service-card__icon"]}
              alt=""
            ></dremio-icon>
            {props.serviceName}
          </div>

          <div className={classes["service-card__description"]}>
            {props.description}
          </div>

          <div className={classes["service-card__action"]}>{props.action}</div>
        </div>
      </LinkWrapper>
    </Card>
  );
};
