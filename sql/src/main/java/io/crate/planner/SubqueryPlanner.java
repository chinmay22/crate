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

package io.crate.planner;

import io.crate.analyze.QuerySpec;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import org.elasticsearch.common.util.Consumer;

import java.util.HashMap;
import java.util.Map;

public class SubqueryPlanner {

    private final Planner.Context plannerContext;
    private final Visitor visitor;
    private final Map<Plan, SelectSymbol> subQueries = new HashMap<>();

    public SubqueryPlanner(Planner.Context plannerContext) {
        this.plannerContext = plannerContext;
        this.visitor = new Visitor();
    }

    private void planSubquery(SelectSymbol selectSymbol) {
        AnalyzedRelation relation = selectSymbol.relation();
        SelectAnalyzedStatement selectAnalyzedStatement = new SelectAnalyzedStatement(((QueriedRelation) relation));
        Plan subPlan = plannerContext.planSingleRowSubselect(selectAnalyzedStatement);
        subQueries.put(subPlan, selectSymbol);
    }

    public Map<Plan, SelectSymbol> planSubQueries(QuerySpec querySpec) {
        querySpec.visitSymbols(visitor);
        return subQueries;
    }

    private class Visitor extends SymbolVisitor<Symbol, Void> implements Consumer<Symbol> {

        @Override
        public Void visitFunction(io.crate.analyze.symbol.Function function, Symbol parent) {
            for (Symbol arg : function.arguments()) {
                process(arg, function);
            }
            return null;
        }

        @Override
        public Void visitSelectSymbol(SelectSymbol selectSymbol, Symbol parent) {
            planSubquery(selectSymbol);
            return null;
        }

        @Override
        public void accept(Symbol symbol) {
            process(symbol, null);
        }
    }
}
