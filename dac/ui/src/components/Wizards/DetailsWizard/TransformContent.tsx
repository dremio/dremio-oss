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

import Immutable from "immutable";
import Transform from "@app/pages/ExplorePage/components/Transform/Transform";
import * as classes from "./TransformContent.module.less";

type TransformContentProps = {
  dataset: Immutable.Map<string, any>;
  submit: (values: Record<string, any>, submitType: unknown) => any;
  cancel: () => void;
  changeFormType: (formType: string) => void;
};

function TransformContent({
  dataset,
  submit,
  cancel,
  changeFormType,
}: TransformContentProps) {
  return (
    <div className={classes["transform-content"]}>
      <Transform
        dataset={dataset}
        submit={submit}
        cancel={cancel}
        changeFormType={changeFormType}
      />
    </div>
  );
}

export default TransformContent;
