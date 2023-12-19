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
/* eslint-disable */
import fs from "fs";

/**
 * Run golden tests using expected outputs stored in a file relative to the test file:
 *   ./resources/{test file base name}.test.json
 *
 * In the case of a test failure, the full test actual results are printed out to a file to easily update the expected
 * results as needed without code changes. The file is created relative to the test file:
 *   ./resources/{test file base name}.actual.json
 *
 * If testCases is a string[], the test case names are used as input to the provided testBody method to create actuals.
 * Else, if testCases is an object {test name -> value}, the given values are used as input to the provided testBody
 *   methods to create actuals.
 */
export function runGoldenTests<TestResult>(
  testCases: string[],
  expecteds: Record<string, TestResult>,
  testFileName: string,
  testBody: (input: string) => TestResult
): void;
/** Run golden tests, where the testCases values are used as input to the test body */
export function runGoldenTests<TestInput, TestResult>(
  testCases: Record<string, TestInput>,
  expecteds: Record<string, TestResult>,
  testFileName: string,
  testBody: (input: TestInput) => TestResult
): void;
export function runGoldenTests<TestInput, TestResult>(
  testCases: string[] | Record<string, TestInput>,
  expecteds: Record<string, TestResult>,
  testFileName: string,
  testBody: (input: string | TestInput) => TestResult
): void {
  const testNameIsData = Array.isArray(testCases);
  const testNames: string[] = testNameIsData
    ? testCases
    : Object.keys(testCases);
  if (
    Object.keys(expecteds).some((testName) => !testNames.includes(testName))
  ) {
    throw new Error("Test expecteds contains a test name not found");
  }
  const actuals: Record<string, TestResult> = {};
  const actualsFileName: string = testFileName
    .replace(new RegExp(/\/(?=[^\/]+$)/), "/resources/")
    .replace(new RegExp(/(\.test)?\.\w+$/), ".actual.json");

  const printTestActual = (actuals: Record<string, TestResult>) => {
    const actualString = JSON.stringify(actuals, null, 2);
    const expectedString = JSON.stringify(expecteds, null, 2);
    if (actualString != expectedString) {
      fs.writeFile(actualsFileName, actualString + "\n", () => {});
    } else {
      fs.unlink(actualsFileName, () => {});
    }
  };

  it.each(testNames)("%s", (testName: string) => {
    const testInput = testNameIsData ? testName : testCases[testName];
    const actual = testBody(testInput);
    actuals[testName] = actual;
    const expected: TestResult | undefined = expecteds[testName];
    expect(actual).toEqual(expected);
  });

  afterAll(() => {
    printTestActual(actuals);
  });
}
