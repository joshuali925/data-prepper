/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.meter;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class EMFLoggingRegistryConfigTest {
    @Test
    void testDefault() {
        final EMFLoggingRegistryConfig objectUnderTest = EMFLoggingRegistryConfig.DEFAULT;
        assertThat(objectUnderTest.prefix(), equalTo("emf"));
    }
}