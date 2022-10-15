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
import Lottie from "react-lottie";
// @ts-ignore
import loadingAnimation from "./loadingBar.json";

type LoadingBarProps = {
  width?: number;
  height?: number;
  className?: string;
};

const LoadingBar = ({
  width = 24,
  height = 24,
  className,
}: LoadingBarProps) => {
  const defaultOptions = {
    loop: true,
    autoplay: true,
    animationData: loadingAnimation,
    rendererSettings: {
      preserveAspectRatio: "xMidYMid slice",
    },
  };

  return (
    <div className={className}>
      <Lottie options={defaultOptions} width={width} height={height} />
    </div>
  );
};

export default LoadingBar;
