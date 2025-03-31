/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.policy.content.maintenance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.policy.content.PolicyContentUtil;
import org.apache.polaris.core.policy.validator.InvalidPolicyException;

public class OrphanFileRemovalPolicyContent extends BaseMaintenancePolicyContent {
  private static final String DEFAULT_POLICY_SCHEMA_VERSION = "2025-02-03";
  private static final Set<String> POLICY_SCHEMA_VERSIONS = Set.of(DEFAULT_POLICY_SCHEMA_VERSION);

  @JsonProperty(value = "max_orphan_file_age_in_days")
  private int maxOrphanFileAgeInDays;

  private List<String> locations;

  @JsonCreator
  public OrphanFileRemovalPolicyContent(
      @JsonProperty(value = "enable", required = true) boolean enable) {
    super(enable);
  }

  public int getMaxOrphanFileAgeInDays() {
    return maxOrphanFileAgeInDays;
  }

  public void setMaxOrphanFileAgeInDays(int maxOrphanFileAgeInDays) {
    this.maxOrphanFileAgeInDays = maxOrphanFileAgeInDays;
  }

  public List<String> getLocations() {
    return locations;
  }

  public void setLocations(List<String> locations) {
    this.locations = locations;
  }

  public static OrphanFileRemovalPolicyContent fromString(String content) {
    if (Strings.isNullOrEmpty(content)) {
      throw new InvalidPolicyException("Policy is empty");
    }

    OrphanFileRemovalPolicyContent policy;
    try {
      policy = PolicyContentUtil.MAPPER.readValue(content, OrphanFileRemovalPolicyContent.class);
    } catch (Exception e) {
      throw new InvalidPolicyException(e);
    }

    validateVersion(content, policy, DEFAULT_POLICY_SCHEMA_VERSION, POLICY_SCHEMA_VERSIONS);

    int maxAge = policy.getMaxOrphanFileAgeInDays();
    if (maxAge < 0) {
      throw new InvalidPolicyException(
          "Invalid max_orphan_file_age_in_days: "
              + maxAge
              + ". It must be greater than or equal to 0");
    }

    return policy;
  }
}
