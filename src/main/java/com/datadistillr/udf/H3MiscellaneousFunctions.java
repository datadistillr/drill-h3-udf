/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datadistillr.udf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;


public class H3MiscellaneousFunctions {

  @FunctionTemplate(names = {"degreesToRads", "degrees_to_rads"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class degreesToRadians implements DrillSimpleFunc {

    @Param
    Float8Holder degrees;

    @Output
    Float8Holder radians;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      radians.value = java.lang.Math.toRadians(degrees.value);
    }
  }

  @FunctionTemplate(names = {"radsToDegrees", "rads_to_degrees"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class radiansToDegrees implements DrillSimpleFunc {

    @Param
    Float8Holder radians;

    @Output
    Float8Holder degrees;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      degrees.value = java.lang.Math.toDegrees(radians.value);
    }
  }


  @FunctionTemplate(names = {"hexAreaKm2", "hex_area_km2"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class hexAreaKm2 implements DrillSimpleFunc {

    @Param
    IntHolder resolution;

    @Output
    Float8Holder result;

    @Workspace
    com.uber.h3core.H3Core h3;

    @Override
    public void setup() {
      try {
        h3 = com.uber.h3core.H3Core.newInstance();
      } catch (java.io.IOException e) {
        h3 = null;
      }
    }

    @Override
    public void eval() {
      if (h3 == null) {
        result.value = 0;
      } else {
        result.value = h3.hexArea(resolution.value, com.uber.h3core.AreaUnit.km2);
      }
    }
  }

  @FunctionTemplate(names = {"hexAreaM2", "hex_area_m2"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class hexAreaM2 implements DrillSimpleFunc {

    @Param
    IntHolder resolution;

    @Output
    Float8Holder result;

    @Workspace
    com.uber.h3core.H3Core h3;

    @Override
    public void setup() {
      try {
        h3 = com.uber.h3core.H3Core.newInstance();
      } catch (java.io.IOException e) {
        h3 = null;
      }
    }

    @Override
    public void eval() {
      if (h3 == null) {
        result.value = 0;
      } else {
        result.value = h3.hexArea(resolution.value, com.uber.h3core.AreaUnit.m2);
      }
    }
  }


}
