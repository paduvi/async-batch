package io.dogy.async;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestSimple {
    public static void main(String[] args) throws InterruptedException {
        BatchProcessor<Integer, Object> processor = BatchProcessor.<Integer, Object>newBuilder()
                .with(builder -> {
                    builder.corePoolSize = 3;
                    builder.batchSize = 30;
                })
                .build(entries -> {
                    List<Integer> list = new LinkedList<>();
                    for (Map.Entry<Integer, AsyncCallback<Object>> entry : entries) {
                        list.add(entry.getKey());

                        // uncomment this to manually complete callback
                        // otherwise it will auto complete with result=null

                        // entry.getValue().complete(entry.getKey());
                    }
                    System.out.println(list);
                });

        for (int i = 0; i < 1E4; i++) {
            processor.put(i);

            if (i % 100 == 0) {
                Thread.sleep(100);
            }
        }

        processor.close();
    }
}
