package io.dogy.async;

import java.io.Closeable;
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class BatchProcessor<K, V> implements Closeable {

    private final AtomicBoolean isDisposed = new AtomicBoolean(false);
    private final ExecutorService executorService;
    private final BlockingQueue<Map.Entry<K, AsyncCallback<V>>> queue;
    private final Semaphore semaphore;
    private final int corePoolSize;

    private BatchProcessor(Builder<K, V> builder, Consumer<List<Map.Entry<K, AsyncCallback<V>>>> applyFn) {
        final int batchSize = builder.batchSize;

        this.executorService = builder.executorService;
        this.corePoolSize = builder.corePoolSize;
        this.queue = builder.queue;
        this.semaphore = new Semaphore(this.corePoolSize);
        this.semaphore.drainPermits();

        for (int i = 0; i < corePoolSize; i++) {
            CompletableFuture.runAsync(() -> {
                while (!isDisposed.get()) {
                    List<Map.Entry<K, AsyncCallback<V>>> batch = new LinkedList<>();

                    try {
                        int n = queue.drainTo(batch, batchSize);
                        if (n == 0) {
                            this.semaphore.acquire();
                            continue;
                        }

                        applyFn.accept(batch);
                    } catch (Exception e) {
                        for (Map.Entry<K, AsyncCallback<V>> entry : batch) {
                            entry.getValue().completeExceptionally(e);
                        }
                    } finally {
                        for (Map.Entry<K, AsyncCallback<V>> entry : batch) {
                            entry.getValue().complete();
                        }
                    }
                }
            }, executorService);
        }
    }

    public static <K, V> Builder<K, V> newBuilder() {
        return new Builder<>();
    }

    public AsyncCallback<V> put(K payload) {
        AsyncCallback<V> callback = new AsyncCallback<>();
        this.queue.offer(new AbstractMap.SimpleImmutableEntry<>(payload, callback));

        this.semaphore.release(this.corePoolSize);
        return callback;
    }

    @Override
    public void close() {
        this.isDisposed.set(true);
        this.executorService.shutdown();
    }

    public static class Builder<K, V> {

        public ExecutorService executorService = Executors.newCachedThreadPool();
        public int corePoolSize = 1;
        public BlockingQueue<Map.Entry<K, AsyncCallback<V>>> queue = new LinkedBlockingQueue<>();
        public int batchSize = 100;

        public Builder<K, V> with(Consumer<Builder<K, V>> consumer) {
            consumer.accept(this);
            return this;
        }

        public BatchProcessor<K, V> build(Consumer<List<Map.Entry<K, AsyncCallback<V>>>> applyFn) {
            assert applyFn != null;
            assert batchSize > 0;
            assert corePoolSize > 0;

            return new BatchProcessor<>(this, applyFn);
        }

    }
}
