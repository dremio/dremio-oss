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
import { Tooltip } from "./Tooltip/Tooltip";
import { useHasClipboardPermissions } from "./utilities/useHasClipboardPermissions";
import copy from "copy-to-clipboard";
import { type Placement } from "@floating-ui/react-dom-interactions";

const writeToClipboard = (text: string): Promise<void> => {
  return navigator.clipboard.writeText(text);
};

type Props = {
  children: JSX.Element;
  contents: string;
  placement?: Placement;
};

export const CopyContainer = (props: Props) => {
  const hasPermission = useHasClipboardPermissions();
  const [hasCopied, setHasCopied] = React.useState(false);

  return (
    <Tooltip
      placement={props.placement || "top"}
      content={hasCopied ? <span>Copied</span> : <span>Copy</span>}
      onClose={() => {
        setHasCopied(false);
      }}
    >
      {React.cloneElement(props.children, {
        onClick: () => {
          hasPermission
            ? writeToClipboard(props.contents)
            : copy(props.contents);
          setHasCopied(true);
        },
      })}
    </Tooltip>
  );
};
