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

import * as React from "react";
import { IconButton } from "./IconButton";
import { CopyContainer } from "./CopyContainer";
import { type Placement } from "@floating-ui/react-dom-interactions";

type Props = {
  contents: string;
  className?: string;
  size?: "S" | "L";
  placement?: Placement;
  copyTooltipLabel?: string;
  portal?: boolean;
};

export const CopyButton = (props: Props) => {
  const {
    size = "S",
    contents,
    className,
    placement,
    portal = true,
    copyTooltipLabel,
  } = props;
  let copyButtonStyle;
  switch (size) {
    case "S":
      copyButtonStyle = {
        blockSize: "1.25em",
        inlineSize: "1.25em",
      };
      break;
    case "L":
      copyButtonStyle = {
        blockSize: "1.75em",
        inlineSize: "1.75em",
      };
      break;
  }

  return (
    <CopyContainer
      contents={contents}
      placement={placement}
      copyTooltipLabel={copyTooltipLabel}
      portal={portal}
    >
      <IconButton aria-label="Copy" className={className}>
        {/*@ts-ignore*/}
        <dremio-icon name="interface/copy" alt="" style={copyButtonStyle}>
          {/*@ts-ignore*/}
        </dremio-icon>
      </IconButton>
    </CopyContainer>
  );
};
