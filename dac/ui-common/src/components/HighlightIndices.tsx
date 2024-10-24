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

import { type FC } from "react";

const wrapHighlightIndicies = (
  text: string,
  indices: number[],
): Array<string | React.ReactElement> => {
  if (indices.length === 0) return [];
  const output: Array<string | JSX.Element> = [text.slice(0, indices[0])];

  let highlighted = "";

  for (let i = 0; i < indices.length; i++) {
    const next = indices[i + 1];
    const current = indices[i];

    if (next !== undefined && current + 1 === next) {
      highlighted += text[current];
    } else {
      highlighted += text[current];
      output.push(<mark>{highlighted}</mark>);
      highlighted = "";
      output.push(text.slice(current + 1, next));
    }
  }
  return output;
};

export const HighlightIndices: FC<{
  children: string;
  indices?: number[];
}> = (props) => {
  if (!props.indices || props.indices.length === 0) {
    return <>{props.children}</>;
  }
  return <>{wrapHighlightIndicies(props.children, props.indices)}</>;
};
