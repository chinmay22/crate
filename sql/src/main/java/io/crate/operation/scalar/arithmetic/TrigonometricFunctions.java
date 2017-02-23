/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.operation.scalar.arithmetic;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.scalar.DoubleScalar;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Collections;
import java.util.function.DoubleUnaryOperator;

public final class TrigonometricFunctions {

    public static void register(ScalarFunctionModule module) {
        register(module, "sin", Math::sin);
        register(module, "asin", x -> Math.asin(checkRange(x)));
        register(module, "cos", Math::cos);
        register(module, "acos", x -> Math.acos(checkRange(x)));
        register(module, "tan", Math::tan);
        register(module, "atan", x -> Math.atan(checkRange(x)));
    }

    private static void register(ScalarFunctionModule module, String name, DoubleUnaryOperator func) {
        for (DataType inputType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            FunctionIdent ident = new FunctionIdent(name, Collections.singletonList(inputType));
            module.register(new DoubleScalar(new FunctionInfo(ident, DataTypes.DOUBLE), func));
        }
    }

    private static double checkRange(double value) {
        if (value < -1.0 || value > 1.0) {
            throw new IllegalArgumentException("input value " + value + " is out of range. " +
                                               "Values must be in range of [-1.0, 1.0]");
        }
        return value;
    }
}
