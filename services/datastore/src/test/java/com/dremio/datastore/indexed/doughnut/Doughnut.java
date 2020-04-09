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
package com.dremio.datastore.indexed.doughnut;

import java.io.Serializable;
import java.util.Objects;

/**
 * Doughnut for indexed store tests.
 */
public class Doughnut implements Serializable {
  private final String name;
  private final String flavor;
  private final double price;
  private final int thickness;
  private final long diameter;

  public Doughnut(String name, String flavor, double price, int thickness, long diameter) {
    super();
    this.name = name;
    this.flavor = flavor;
    this.price = price;
    this.thickness = thickness;
    this.diameter = diameter;
  }

  public Doughnut(String name, String flavor, double price) {
    super();
    this.name = name;
    this.flavor = flavor;
    this.price = price;
    this.thickness = 0;
    this.diameter = 0;
  }

  public String getName() {
    return name;
  }

  public double getPrice() {
    return price;
  }

  public String getFlavor() {
    return flavor;
  }

  public int getThickness() {
    return thickness;
  }

  public long getDiameter() {
    return diameter;
  }

  @Override
  public String toString() {
    return "Doughnut [name=" + name + ", flavor=" + flavor + ", price=" + price + ", thickness=" + thickness + ", diameter=" + diameter + "]";
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, flavor, price, thickness, diameter);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    Doughnut other = (Doughnut) obj;
    if (flavor == null) {
      if (other.flavor != null) {
        return false;
      }
    } else if (!flavor.equals(other.flavor)) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (Double.doubleToLongBits(price) != Double.doubleToLongBits(other.price)) {
      return false;
    }
    if (thickness != other.thickness) {
      return false;
    }
    if (diameter != other.diameter) {
      return false;
    }
    return true;
  }
}
