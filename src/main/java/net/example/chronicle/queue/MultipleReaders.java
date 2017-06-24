package net.example.chronicle.queue;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;

public class MultipleReaders {

    private static final int EVENT_COUNT  = 1000;
    private static final int READER_COUNT = 5;

    public static void main(String[] args) throws InterruptedException {
        Path path = Paths.get("target", "queue");
        BlockingQueue<Long> indexQueue = new ArrayBlockingQueue<Long>(EVENT_COUNT);
        BlockingQueue<Long> outputQueue = new ArrayBlockingQueue<Long>(EVENT_COUNT);
        ExecutorService executor = Executors.newFixedThreadPool(READER_COUNT + 1);

        IntStream.range(0, READER_COUNT).forEach(i -> executor.submit(() -> tailerTask(path, indexQueue, outputQueue)));
        executor.submit(() -> appenderTask(path, indexQueue));

        for (int i = 0; i < EVENT_COUNT; i++) {
            System.out.println(outputQueue.take());
        }
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.SECONDS);
    }

    private static void appenderTask(Path path, BlockingQueue<Long> indexQueue) {
        long id = 0;
        ExcerptAppender app = null;
        try (ChronicleQueue queue = binary(path).rollCycle(RollCycles.HOURLY).build()) {
            app = queue.acquireAppender();
            for (int i = 0; i < EVENT_COUNT; i++) {
                try (DocumentContext dc = app.writingDocument()) {
                    Bytes<?> bytes = dc.wire().bytes();
                    bytes.writeLong(++id);
                }
                indexQueue.put(app.lastIndexAppended());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void tailerTask(Path path, BlockingQueue<Long> indexQueue, BlockingQueue<Long> outputQueue) {
        ExcerptTailer tailer = null;
        try (ChronicleQueue queue = binary(path).rollCycle(RollCycles.HOURLY).build()) {
            tailer = queue.createTailer();
            while (true) {
                long index;
                try {
                    index = indexQueue.take();
                } catch (InterruptedException e) {
                    return;
                }
                if (!tailer.moveToIndex(index)) {
                    System.err.println("Bad index: " + index);
                } else {
                    try (DocumentContext dc = tailer.readingDocument()) {
                        Bytes<?> bytes = dc.wire().bytes();
                        long id = bytes.readLong();
                        outputQueue.put(id);
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
