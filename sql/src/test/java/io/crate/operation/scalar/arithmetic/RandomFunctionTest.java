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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.TransactionContext;
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataType;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.*;


public class RandomFunctionTest extends AbstractScalarFunctionsTest {

    private RandomFunction random;

    @Before
    public void prepareRandom() {
        random = (RandomFunction) functions.get(new FunctionIdent(RandomFunction.NAME, Collections.<DataType>emptyList()));

    }

    @Test
    public void testEvaluateRandom() {
        assertThat(random.evaluate(new Input[0]),
            is(allOf(greaterThanOrEqualTo(0.0), lessThan(1.0))));
    }

    @Test
    public void normalizeReference() {
        Function function = new Function(random.info(), Collections.<Symbol>emptyList());
        Function normalized = (Function) random.normalizeSymbol(function, new TransactionContext(SessionContext.SYSTEM_SESSION));
        assertThat(normalized, sameInstance(function));
    }

}
