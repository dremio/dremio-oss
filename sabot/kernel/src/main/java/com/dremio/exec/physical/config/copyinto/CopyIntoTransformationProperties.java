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
package com.dremio.exec.physical.config.copyinto;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.physical.config.ExtendedProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents properties for the Copy Into transformation process, implementing the {@link
 * ExtendedProperty} interface. This class stores a list of transformation properties, each defined
 * by a {@link Property} object.
 */
public class CopyIntoTransformationProperties implements ExtendedProperty {

  private final List<Property> properties = new ArrayList<>();

  /** Default constructor for serialization purposes. */
  public CopyIntoTransformationProperties() {}

  /**
   * Adds a new transformation property to the list.
   *
   * @param property The {@link Property} object to be added.
   */
  public void addProperty(Property property) {
    this.properties.add(property);
  }

  /**
   * Returns the list of transformation properties.
   *
   * @return A list of {@link Property} objects.
   */
  public List<Property> getProperties() {
    return properties;
  }

  /**
   * Represents a single transformation property, including the transformation expression, source
   * column names, and target column name.
   */
  public static class Property {
    private LogicalExpression transformationExpression;
    private List<String> sourceColNames;
    private String targetColName;

    /** Default constructor for serialization purposes. */
    private Property() {}

    /**
     * Constructs a new Property with the specified transformation expression, source column names,
     * and target column name.
     *
     * @param transformationExpression The {@link LogicalExpression} representing the
     *     transformation.
     * @param sourceColNames The list of source column names involved in the transformation.
     * @param targetColName The target column name for the transformation result.
     */
    public Property(
        LogicalExpression transformationExpression,
        List<String> sourceColNames,
        String targetColName) {
      this.transformationExpression = transformationExpression;
      this.sourceColNames = sourceColNames;
      this.targetColName = targetColName;
    }

    /**
     * Returns the transformation expression for this property.
     *
     * @return The {@link LogicalExpression} representing the transformation.
     */
    public LogicalExpression getTransformationExpression() {
      return transformationExpression;
    }

    /**
     * Returns the list of source column names involved in the transformation.
     *
     * @return A list of source column names.
     */
    public List<String> getSourceColNames() {
      return sourceColNames;
    }

    /**
     * Returns the target column name for the transformation result.
     *
     * @return The target column name.
     */
    public String getTargetColName() {
      return targetColName;
    }
  }
}
