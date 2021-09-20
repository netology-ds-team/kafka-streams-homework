package ru.netology.dsw.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class StateUpdateSupplier implements ProcessorSupplier<String, Object, Void, Void> {
    private final String storeName;

    public StateUpdateSupplier(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public Processor<String, Object, Void, Void> get() {
        return new StateUpdater(storeName);
    }

    private static class StateUpdater implements Processor<String, Object, Void, Void> {
        private final String storeName;
        private KeyValueStore<String, Object> stateStore;

        public StateUpdater(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext context) {
            stateStore = (KeyValueStore<String, Object>) context.getStateStore(storeName);
        }

        @Override
        public void process(Record<String, Object> record) {
            stateStore.put(record.key(), record.value());
        }

        @Override
        public void close() {
        }
    }
}
