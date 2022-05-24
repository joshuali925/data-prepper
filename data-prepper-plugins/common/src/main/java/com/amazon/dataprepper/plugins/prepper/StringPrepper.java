/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An simple String implementation of {@link Prepper} which generates new Records with upper case or lowercase content. The current
 * simpler implementation does not handle errors (if any).
 */
@DataPrepperPlugin(name = "string_converter", pluginType = Prepper.class, pluginConfigurationType = StringPrepper.Configuration.class)
public class StringPrepper implements Prepper<Record<Event>, Record<Event>> {
    private static Logger LOG = LoggerFactory.getLogger(StringPrepper.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TypeReference<Map<String, Object>> mapTypeReference = new TypeReference<Map<String, Object>>() {};

    public static final String UPPER_CASE = "upper_case";

    private final boolean upperCase;

    public static class Configuration {
        private boolean upperCase = true;

        public boolean getUpperCase() {
            return upperCase;
        }

        public void setUpperCase(final boolean upperCase) {
            this.upperCase = upperCase;
        }
    }

    /**
     * Mandatory constructor for Data Prepper Component - This constructor is used by Data Prepper
     * runtime engine to construct an instance of {@link StringPrepper} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param configuration instance with metadata information from pipeline pluginSetting file.
     */
    @DataPrepperPluginConstructor
    public StringPrepper(final Configuration configuration) {
        this.upperCase = configuration.getUpperCase();
    }

    private Collection<Record<Event>> executePPL(Collection<Record<Event>> input) {
        try {
            ProcessBuilder builder = new ProcessBuilder("java", "-jar", "/Users/lijshu/Projects/os-2.0/sql/libppl/build/libs/libppl-2.0.0.0-SNAPSHOT.jar", "source = stdin");
            Process process = builder.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
            input.forEach(eventRecord -> {
                Event event = eventRecord.getData();
                String line = event.toJsonString();
                try {
                    writer.write(line);
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            writer.flush();
            writer.close();
            Event e = JacksonEvent.builder()
                .withEventType("testppl")
                .withData(reader.lines().collect(Collectors.joining("\n")))
                .build();
            Collection<Record<Event>> modifiedRecords = new ArrayList<>(1);
            modifiedRecords.add(new Record<Event>(e));
            return modifiedRecords;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Record<Event>> execute(final Collection<Record<Event>> records) {
        return executePPL(records);
        // final Collection<Record<Event>> modifiedRecords = new ArrayList<>(records.size());
        // for (Record<Event> record : records) {
        //     final Event recordEvent = record.getData();
        //     final String eventJson = recordEvent.toJsonString();
        //     try {
        //         final Map<String, Object> newData = processEventJson(eventJson);
        //         final Event newRecordEvent = JacksonEvent.builder()
        //                 .withEventMetadata(recordEvent.getMetadata())
        //                 .withData(newData)
        //                 .build();
        //         modifiedRecords.add(new Record<>(newRecordEvent));
        //     } catch (JsonProcessingException e) {
        //         LOG.error("Unable to process Event data: {}", eventJson, e);
        //     }
        // }
        // return modifiedRecords;
    }

    private Map<String, Object> processEventJson(final String data) throws JsonProcessingException {
        final Map<String, Object> dataMap = objectMapper.readValue(data, mapTypeReference);
        return dataMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    final Object val = entry.getValue();
                    if (val instanceof String) {
                        return upperCase? ((String) val).toUpperCase() : ((String) val).toLowerCase();
                    } else {
                        return val;
                    }
                }));
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }


    @Override
    public void shutdown() {

    }
}
