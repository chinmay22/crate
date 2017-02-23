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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

public class AbsFunction extends Scalar<Number, Number> {

    public static final String NAME = "abs";

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    private final FunctionInfo info;

    public AbsFunction(DataType dataType) {
        this.info = new FunctionInfo(
            new FunctionIdent(NAME, ImmutableList.of(dataType)), dataType);
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Number evaluate(Input<Number>... args) {
        assert args.length == 1 : "number of args must be 1";
        Number value = args[0].value();
        if (value != null) {
            return (Number) info.returnType().value(Math.abs(value.doubleValue()));
        }
        return null;
    }

    private static class Resolver implements FunctionResolver {

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new AbsFunction(dataTypes.get(0));
        }

        @Nullable
        @Override
        public List<DataType> getSignature(List<DataType> dataTypes) {
            return Signature.SIGNATURES_SINGLE_NUMERIC.apply(dataTypes);
        }
    }
}
