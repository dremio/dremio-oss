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
import { useRef, useState, useEffect } from "react";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";
//@ts-ignore
import { Tooltip } from "dremio-ui-lib";
import * as classes from "./BreadcrumbLink.module.less";

const BreadcrumbLink = (props: {
  text: string;
  to: string;
  iconName?: string;
  containerClass?: string;
}) => {
  const { text, to, iconName, containerClass = "" } = props;
  const crumbRef = useRef<any>(null);
  const [showTooltip, setShowTooltip] = useState(false);

  useEffect(() => {
    if (
      !showTooltip &&
      crumbRef.current?.offsetWidth < crumbRef.current?.scrollWidth
    ) {
      setShowTooltip(true);
    }
  }, [crumbRef, showTooltip]);

  const textClass = classes["breadcrumb-link-text"];

  return (
    <LinkWithRef
      className={`${classes["breadcrumb-link"]} ${containerClass}`}
      to={to}
    >
      {iconName && (
        <dremio-icon class={classes["breadcrumb-icon"]} name={iconName} />
      )}
      {showTooltip ? (
        <Tooltip interactive title={text}>
          <span className={textClass}>{text}</span>
        </Tooltip>
      ) : (
        <span ref={crumbRef} className={textClass}>
          {text}
        </span>
      )}
    </LinkWithRef>
  );
};

export default BreadcrumbLink;
