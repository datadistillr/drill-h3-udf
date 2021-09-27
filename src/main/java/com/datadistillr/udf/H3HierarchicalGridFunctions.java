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

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

public class H3HierarchicalGridFunctions {

  @FunctionTemplate(names = {"h3ToParent", "h3_to_parent"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class h3ToParentUDF implements DrillSimpleFunc {

    @Param
    BigIntHolder h3Input;

    @Param
    IntHolder parentResolution;

    @Output
    BigIntHolder result;

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
        result.value = h3.h3ToParent(h3Input.value, parentResolution.value);
      }
    }
  }

  @FunctionTemplate(names = {"h3ToParent", "h3_to_parent"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class h3ToParentAddress implements DrillSimpleFunc {

    @Param
    VarCharHolder h3Input;

    @Param
    IntHolder parentResolution;

    @Output
    VarCharHolder out;

    @Workspace
    com.uber.h3core.H3Core h3;

    @Inject
    DrillBuf buffer;

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
      if (h3 != null) {
        String h3InputString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(h3Input);
        String result = h3.h3ToParentAddress(h3InputString, parentResolution.value);

        byte[] rowStringBytes = result.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        buffer = buffer.reallocIfNeeded(rowStringBytes.length);
        buffer.setBytes(0, rowStringBytes);

        out.start = 0;
        out.end = rowStringBytes.length;
        out.buffer = buffer;

      }
    }
  }
}
