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

package io.crate.plugin;

import io.crate.ClusterIdService;
import io.crate.module.CrateCoreModule;
import io.crate.rest.CrateRestMainAction;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;

import java.util.Collection;
import java.util.Collections;

public class CrateCorePlugin extends Plugin {

    private final Settings settings;

    public CrateCorePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "crate-core";
    }

    @Override
    public String description() {
        return "Crate Core";
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        return Collections.<Class<? extends LifecycleComponent>>singletonList(ClusterIdService.class);
    }

    @Override
    public Collection<Module> nodeModules() {
        return Collections.<Module>singletonList(new CrateCoreModule(settings));
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(CrateRestMainAction.class);
    }
}
