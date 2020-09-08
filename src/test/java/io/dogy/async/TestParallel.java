package io.dogy.async;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestParallel {
    public static void main(String[] args) {
        BatchProcessor<Integer, Integer> processor = BatchProcessor.<Integer, Integer>newBuilder()
                .with(builder -> {
                    builder.corePoolSize = 3;
                    builder.batchSize = 30;
                })
                .build(entries -> {
                    List<Integer> list = new LinkedList<>();
                    for (Map.Entry<Integer, AsyncCallback<Integer>> entry : entries) {
                        list.add(entry.getKey());

                        // uncomment this to manually complete callback
                        // otherwise it will auto complete with result=null

                        entry.getValue().complete(entry.getKey() * 2);
                    }
                    System.out.println(list);
                });

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 1E3; i++) {
            final int input = i;
            executorService.submit(() -> {
                AsyncCallback<Integer> callback = processor.put(input);
                try {
                    System.out.printf("Input: %s - Output: %s\n", input, callback.get());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        executorService.shutdown();
        processor.close();
    }
}
