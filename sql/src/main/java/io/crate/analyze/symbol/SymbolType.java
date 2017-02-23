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

package io.crate.analyze.symbol;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.GeoReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.Reference;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.List;

public enum SymbolType {

    AGGREGATION(Aggregation::new),
    REFERENCE(Reference::new),
    RELATION_OUTPUT(Field::new),
    FUNCTION(Function::new),
    LITERAL(Literal::new),
    INPUT_COLUMN(InputColumn::new),
    DYNAMIC_REFERENCE(DynamicReference::new),
    VALUE(Value::new),
    MATCH_PREDICATE(null),
    FETCH_REFERENCE(null),
    RELATION_COLUMN(RelationColumn::new),
    INDEX_REFERENCE(IndexReference::new),
    GEO_REFERENCE(GeoReference::new),
    GENERATED_REFERENCE(GeneratedReference::new),
    PARAMETER(ParameterSymbol::new),
    SELECT_SYMBOL(SelectSymbol::new);

    public static final List<SymbolType> VALUES = ImmutableList.copyOf(values());

    private final Symbol.SymbolFactory factory;

    SymbolType(Symbol.SymbolFactory factory) {
        this.factory = factory;
    }

    public Symbol newInstance(StreamInput in) throws IOException {
        return factory.newInstance(in);
    }

    public boolean isValueSymbol() {
        return ordinal() == LITERAL.ordinal();
    }
}
