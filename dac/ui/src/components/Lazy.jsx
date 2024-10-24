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
import LoadingOverlay from "#oss/components/LoadingOverlay";

/**
 * A wrapper around React.lazy, which returns a functional component. It passes props through to
 * dynamically loaded component. This wrapper is needed because router can not use the direct result
 * of React.lazy.
 *
 * @param importFn - a function passed to React.lazy, which must call dynamic import().
 * {@see https://reactjs.org/docs/code-splitting.html#reactlazy}
 */
export const lazy = (importFn) => {
  const Comp = React.lazy(importFn);
  return (props) => <Comp {...props} />;
};

/**
 * A default implementation of ReactSuspense for Dremio with a spinner as fallback component
 * @param {*} props - props that overrides defaults
 */
export const Suspense = (props) => (
  <React.Suspense fallback={<LoadingOverlay />} {...props} />
);
