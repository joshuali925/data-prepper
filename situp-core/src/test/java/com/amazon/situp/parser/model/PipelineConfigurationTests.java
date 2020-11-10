package com.amazon.situp.parser.model;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.plugins.buffer.BlockingBuffer;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.amazon.situp.TestDataProvider.DEFAULT_READ_BATCH_DELAY;
import static com.amazon.situp.TestDataProvider.DEFAULT_WORKERS;
import static com.amazon.situp.TestDataProvider.TEST_DELAY;
import static com.amazon.situp.TestDataProvider.TEST_WORKERS;
import static com.amazon.situp.TestDataProvider.VALID_PLUGIN_SETTING_1;
import static com.amazon.situp.TestDataProvider.VALID_PLUGIN_SETTING_2;
import static com.amazon.situp.TestDataProvider.validMultipleConfiguration;
import static com.amazon.situp.TestDataProvider.validMultipleConfigurationOfSizeOne;
import static com.amazon.situp.TestDataProvider.validSingleConfiguration;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class PipelineConfigurationTests {

    @Test
    public void testPipelineConfigurationCreation() {
        final PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(validSingleConfiguration(),
                null,
                validMultipleConfigurationOfSizeOne(),
                validMultipleConfiguration(),
                TEST_WORKERS, TEST_DELAY);
        final PluginSetting actualSourcePluginSetting = pipelineConfiguration.getSourcePluginSetting();
        final List<PluginSetting> actualProcessorPluginSettings = pipelineConfiguration.getProcessorPluginSettings();
        final List<PluginSetting> actualSinkPluginSettings = pipelineConfiguration.getSinkPluginSettings();

        comparePluginSettings(actualSourcePluginSetting, VALID_PLUGIN_SETTING_1);
        assertThat(pipelineConfiguration.getBufferPluginSetting(), notNullValue());
        comparePluginSettings(pipelineConfiguration.getBufferPluginSetting(), BlockingBuffer.getDefaultPluginSettings());
        assertThat(actualProcessorPluginSettings.size(), is(1));
        actualProcessorPluginSettings.forEach(processorSettings -> comparePluginSettings(processorSettings, VALID_PLUGIN_SETTING_1));
        assertThat(actualSinkPluginSettings.size(), is(2));
        comparePluginSettings(actualSinkPluginSettings.get(0), VALID_PLUGIN_SETTING_1);
        comparePluginSettings(actualSinkPluginSettings.get(1), VALID_PLUGIN_SETTING_2);
        assertThat(pipelineConfiguration.getWorkers(), is(TEST_WORKERS));
        assertThat(pipelineConfiguration.getReadBatchDelay(), is(TEST_DELAY));
    }

    @Test
    public void testOnlySourceAndSink() {
        final PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(validSingleConfiguration(),
                null,
                null,
                validMultipleConfigurationOfSizeOne(),
                null, null);
        final PluginSetting actualSourcePluginSetting = pipelineConfiguration.getSourcePluginSetting();
        final List<PluginSetting> actualProcessorPluginSettings = pipelineConfiguration.getProcessorPluginSettings();
        final List<PluginSetting> actualSinkPluginSettings = pipelineConfiguration.getSinkPluginSettings();

        comparePluginSettings(actualSourcePluginSetting, VALID_PLUGIN_SETTING_1);
        assertThat(pipelineConfiguration.getBufferPluginSetting(), notNullValue());
        comparePluginSettings(pipelineConfiguration.getBufferPluginSetting(), BlockingBuffer.getDefaultPluginSettings());
        assertThat(actualProcessorPluginSettings, CoreMatchers.isA(Iterable.class));
        assertThat(actualProcessorPluginSettings.size(), is(0));
        assertThat(actualSinkPluginSettings.size(), is(1));
        comparePluginSettings(actualSinkPluginSettings.get(0), VALID_PLUGIN_SETTING_1);
        assertThat(pipelineConfiguration.getWorkers(), is(DEFAULT_WORKERS));
        assertThat(pipelineConfiguration.getReadBatchDelay(), is(DEFAULT_READ_BATCH_DELAY));
    }

    @Test() //not using expected to assert the message
    public void testNoSourceConfiguration() {
        try {
            new PipelineConfiguration(
                    null,
                    validSingleConfiguration(),
                    validMultipleConfiguration(),
                    validMultipleConfiguration(),
                    TEST_WORKERS, TEST_DELAY);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, source is a required component"));
        }
    }

    @Test() //not using expected to assert the message
    public void testNoSinkConfiguration() {
        try {
            new PipelineConfiguration(
                    validSingleConfiguration(),
                    validSingleConfiguration(),
                    validMultipleConfiguration(),
                    null,
                    TEST_WORKERS, TEST_DELAY);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, at least one sink is required"));
        }
    }

    @Test() //not using expected to assert the message
    public void testInvalidWorkersConfiguration() {
        try {
            new PipelineConfiguration(
                    validSingleConfiguration(),
                    validSingleConfiguration(),
                    validMultipleConfiguration(),
                    validMultipleConfiguration(),
                    0, TEST_DELAY);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, workers cannot be 0"));
        }
    }

    @Test() //not using expected to assert the message
    public void testInvalidDelayConfiguration() {
        try {
            new PipelineConfiguration(
                    validSingleConfiguration(),
                    validSingleConfiguration(),
                    validMultipleConfiguration(),
                    validMultipleConfiguration(),
                    TEST_WORKERS, 0);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, delay cannot be 0"));
        }
    }

    private void comparePluginSettings(final PluginSetting actual, final PluginSetting expected) {
        assertThat("Plugin names are different", actual.getName(), is(expected.getName()));
        final Map<String, Object> actualSettings = actual.getSettings();
        final Map<String, Object> expectedSettings = expected.getSettings();
        assertThat("Plugin settings have different number of attributes", actualSettings.size(), is(expectedSettings.size()));
        actualSettings.forEach((key, value) -> {
            assertThat(actualSettings.get(key), is(expectedSettings.get(key))); //all tests use string values so equals is fine
        });
    }
}
