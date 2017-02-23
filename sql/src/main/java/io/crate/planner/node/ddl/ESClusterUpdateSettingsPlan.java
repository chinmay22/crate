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

package io.crate.planner.node.ddl;

import com.google.common.collect.ImmutableMap;
import io.crate.planner.PlanVisitor;
import io.crate.planner.UnnestablePlan;
import io.crate.sql.tree.Expression;

import javax.annotation.Nullable;
import java.util.*;

public class ESClusterUpdateSettingsPlan extends UnnestablePlan {

    private final Map<String, List<Expression>> persistentSettings;
    private final Map<String, List<Expression>> transientSettings;
    private final Set<String> transientSettingsToRemove;
    private final Set<String> persistentSettingsToRemove;
    private final UUID jobId;

    public ESClusterUpdateSettingsPlan(UUID jobId,
                                       Map<String, List<Expression>> persistentSettings,
                                       Map<String, List<Expression>> transientSettings) {
        this.jobId = jobId;
        this.persistentSettings = persistentSettings;
        // always override transient settings with persistent ones, so they won't get overridden
        // on cluster settings merge, which prefers the transient ones over the persistent ones
        // which we don't
        this.transientSettings = new HashMap<>(persistentSettings);
        this.transientSettings.putAll(transientSettings);

        persistentSettingsToRemove = null;
        transientSettingsToRemove = null;
    }

    public ESClusterUpdateSettingsPlan(UUID jobId, Map<String, List<Expression>> persistentSettings) {
        this(jobId, persistentSettings, persistentSettings); // override stale transient settings too in that case
    }

    public ESClusterUpdateSettingsPlan(UUID jobId, Set<String> persistentSettingsToRemove, Set<String> transientSettingsToRemove) {
        this.jobId = jobId;
        this.persistentSettingsToRemove = persistentSettingsToRemove;
        this.transientSettingsToRemove = transientSettingsToRemove;
        persistentSettings = ImmutableMap.of();
        transientSettings = ImmutableMap.of();
    }

    public Map<String, List<Expression>> persistentSettings() {
        return persistentSettings;
    }

    public Map<String, List<Expression>> transientSettings() {
        return transientSettings;
    }

    @Nullable
    public Set<String> persistentSettingsToRemove() {
        return persistentSettingsToRemove;
    }

    @Nullable
    public Set<String> transientSettingsToRemove() {
        return transientSettingsToRemove;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitESClusterUpdateSettingsPlan(this, context);
    }

    @Override
    public UUID jobId() {
        return jobId;
    }
}
