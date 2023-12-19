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

type CodeViewProps = {
  title: React.ReactNode;
  children: React.ReactNode;
  contentClass?: string;
};

export const CodeView = (props: CodeViewProps) => {
  return (
    <div className="code-view">
      <header className="code-view__header">{props.title}</header>
      <div className={`code-view__content ${props.contentClass}`}>
        {props.children}
      </div>
    </div>
  );
};
