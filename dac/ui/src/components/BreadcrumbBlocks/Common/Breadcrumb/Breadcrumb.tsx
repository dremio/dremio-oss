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
import * as classes from "./Breadcrumb.module.less";
//@ts-ignore
import { Tooltip } from "dremio-ui-lib";

const Breadcrumb = (props: {
  text: string;
  iconName?: string;
  containerClass?: string;
  onClick?: any;
}) => {
  const { text, onClick, iconName, containerClass = "" } = props;
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

  const textClass = classes["breadcrumb-text"];

  return (
    <div
      className={`${classes["breadcrumb"]} ${containerClass}`}
      onClick={onClick}
    >
      {iconName && (
        <dremio-icon
          id="breadcrumb-icon"
          class={classes["breadcrumb-icon"]}
          name={iconName}
        />
      )}
      {showTooltip ? (
        <Tooltip interactive title={text}>
          <span id="breadcrumb-text" className={textClass}>
            {text}
          </span>
        </Tooltip>
      ) : (
        <span id="breadcrumb-text" ref={crumbRef} className={textClass}>
          {text}
        </span>
      )}
    </div>
  );
};

export default Breadcrumb;
