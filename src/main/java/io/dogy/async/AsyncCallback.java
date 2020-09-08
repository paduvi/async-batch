package io.dogy.async;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncCallback<T> {

    private final Semaphore lock = new Semaphore(0);
    private T result = null;
    private Exception exception;
    private final AtomicBoolean done = new AtomicBoolean(false);

    public T get() throws Exception {
        try {
            this.lock.acquire();
            if (exception != null) {
                throw exception;
            }
            return result;
        } finally {
            this.lock.release();
        }
    }

    public void join() throws Exception {
        get();
    }

    public void complete() {
        complete(null, null);
    }

    public void complete(T result) {
        complete(null, result);
    }

    public void completeExceptionally(Exception e) {
        complete(e, null);
    }

    public void complete(Exception e, T result) {
        if (this.done.getAndSet(true)) {
            return;
        }
        this.exception = e;
        this.result = result;
        this.lock.release();
    }

}
