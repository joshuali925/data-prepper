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
import java.util.Arrays;
import java.util.List;
import org.opensearch.sql.libppl.LibPPLQueryAction;
import org.opensearch.sql.libppl.LibPPLQueryActionFactory;
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

    private final String[] query;
    private final String pplLibPath;

    public static class Configuration {
        private String[] query = new String[]{"source = stdin"};
        private String pplLibPath;

        public String[] getQuery() {
            return query;
        }

        public String getPplLibPath() {
            return pplLibPath;
        }

        public void setQuery(final String[] query) {
            this.query = query;
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
        this.query = configuration.getQuery();
        this.pplLibPath = configuration.getPplLibPath();
    }

    private Collection<Record<Event>> executePPL(Collection<Record<Event>> input) {
        List<Map<String, Object>> list = input.stream().map(record -> record.getData().toMap()).collect(Collectors.toList());
        LibPPLQueryAction libPPLQueryAction = LibPPLQueryActionFactory.create(list);
        libPPLQueryAction.execute(query[0]);
        Iterable<Map<String, Object>> output = libPPLQueryAction.getOutput();
        Collection<Record<Event>> modifiedRecords = new ArrayList<>();
        output.forEach(map -> {
            Event newEvent = JacksonEvent.builder()
                .withData(map)
                .withEventType("event")
                .build();
            modifiedRecords.add(new Record<>(newEvent));
        });
        return modifiedRecords;
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
                        return ((String) val).toUpperCase();
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
