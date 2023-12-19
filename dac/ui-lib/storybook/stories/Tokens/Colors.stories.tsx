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

import { useEffect, useRef, useState } from "react";

export default {
  title: "Tokens/Colors",
};

const families = [
  "brand",
  "neutral",
  "red",
  "red-cool",
  "orange-warm",
  "orange",
  "green",
  "mint",
  "cyan",
  "blue",
  "blue-warm",
  "indigo",
  "indigo-vivid",
  "violet-warm",
  "magenta",
  "gray",
];
const grades = [25, 50, 75, 100, 150, 200, 300, 400, 500, 600, 700, 800, 900];

const getColorProperties = (
  el: HTMLElement
): { backgroundColor: string; color: string } => {
  const appliedStyle = window.getComputedStyle(el);
  return {
    backgroundColor: appliedStyle.backgroundColor,
    color: appliedStyle.color,
  };
};

const ColorChip = ({
  family,
  grade,
}: {
  family: string;
  grade?: number | string;
}) => {
  const chipRef = useRef<HTMLElement | null>(null);
  const [, setChipProperties] = useState({});
  useEffect(() => {
    setChipProperties(getColorProperties(chipRef.current!));
  }, []);
  return (
    <div style={{ textAlign: "center" }}>
      <div
        ref={chipRef}
        className={`bg-${family}${
          grade ? `-${grade}` : ""
        } flex items-center justify-center`}
        style={{
          width: "96px",
          height: "48px",
          fontSize: "14px",
          userSelect: "all",
        }}
      >
        {grade}
      </div>
      {/* <code style={{ color: "var(--color--gray--600)" }}>
        {chipProperties.backgroundColor}
      </code> */}
    </div>
  );
};
export const Colors = () => (
  <div className="dremio-layout-stack" style={{ "--space": "2em" }}>
    {families.map((family) => (
      <div key={family}>
        <h2
          className="dremio-typography-bold mb-6"
          style={{ textTransform: "capitalize", fontSize: "24px" }}
        >
          {family}
        </h2>
        <div className="flex flex-row">
          {grades.map((grade) => (
            <ColorChip grade={grade} family={family} />
          ))}
        </div>
      </div>
    ))}
    <div>
      <h2
        className="dremio-typography-extra-large dremio-typography-bold mb-6"
        style={{ textTransform: "capitalize", fontSize: "24px" }}
      >
        Danger
      </h2>

      <div className="flex flex-row">
        <ColorChip family="danger" grade={50} />
        <ColorChip family="danger" grade={500} />
      </div>
    </div>
    <div>
      <h2
        className="dremio-typography-extra-large dremio-typography-bold mb-6"
        style={{ textTransform: "capitalize", fontSize: "24px" }}
      >
        Warning
      </h2>
      <div className="flex flex-row">
        <ColorChip family="warning" grade={50} />
        <ColorChip family="warning" grade={500} />
      </div>
    </div>
    <div>
      <h2
        className="dremio-typography-extra-large dremio-typography-bold mb-6"
        style={{ textTransform: "capitalize", fontSize: "24px" }}
      >
        Success
      </h2>
      <div className="flex flex-row">
        <ColorChip family="success" grade={50} />
        <ColorChip family="success" grade={300} />
      </div>
    </div>
    <div>
      <h2
        className="dremio-typography-extra-large dremio-typography-bold mb-6"
        style={{ textTransform: "capitalize", fontSize: "24px" }}
      >
        Info
      </h2>
      <div className="flex flex-row">
        <ColorChip family="info" grade={50} />
        <ColorChip family="info" grade={400} />
      </div>
    </div>
  </div>
);
