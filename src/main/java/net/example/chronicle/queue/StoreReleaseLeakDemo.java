package net.example.chronicle.queue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.WireType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreReleaseLeakDemo {

    private static final Logger logger = LoggerFactory.getLogger(StoreReleaseLeakDemo.class);
    private static Path dir;
    private static int counter;

    public static void main(String[] args) throws IOException, InterruptedException {
        dir = Files.createTempDirectory("leaks");
        Path queueDir = dir.resolve("queue");

        StoreFileListener fileListener = new StoreFileListener() {

            @Override
            public void onAcquired(int cycle, File file) {
                logger.info("Acquired {}", file.getName());
                counter++;
            }

            @Override
            public void onReleased(int cycle, File file) {
                logger.info("Released {}", file.getName());
                counter--;
                if (counter < 0) {
                    logger.error("Negative counter {}", counter);
                }
            }

        };
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir)
                .wireType(WireType.BINARY_LIGHT)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .storeFileListener(fileListener)
                .build()) {

            writeEvents(queue, 100);

            readEvents(queue);

        }
        LinkedList<Path> resources = new LinkedList<>();
        Files.walk(dir).forEach(resources::addFirst);
        resources.forEach(StoreReleaseLeakDemo::deleteFile);

        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String pid = runtime.getName();
        pid = pid.substring(0, pid.indexOf("@"));
        logger.info("Looking for leaks in {}", pid);

        int leakCount = 0;
        Process check = new ProcessBuilder()
                .command("/usr/sbin/lsof", "-p", pid)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .redirectInput(ProcessBuilder.Redirect.INHERIT)
                .start();
        try (BufferedReader input = new BufferedReader(new InputStreamReader(check.getInputStream()))) {
            while (true) {
                String line = input.readLine();
                if (line == null) {
                    break;
                }
                if (line.contains("leaks")) {
                    logger.info(line);
                    leakCount++;
                }
            }
        }
        check.waitFor();
        logger.info("Found {} leaks", leakCount);
        logger.info("Counter: {}", counter);
    }

    private static void readEvents(final SingleChronicleQueue queue) {
        ExcerptTailer tailer = queue.createTailer();
        tailer.direction(TailerDirection.BACKWARD);
        tailer.toEnd();
        int readCount = 0;
        while (tailer.readBytes(bytes -> {
            int index = bytes.readInt();
            long nanos = bytes.readLong();
            if (index < 0) {
                logger.debug(String.format("#%03d: %08x", index, nanos));
            }
        })) {
            readCount++;
        }
        logger.info("Read {} events", readCount);
    }

    private static void writeEvents(final SingleChronicleQueue queue, int eventsPerCycle) throws UnrecoverableTimeoutException, InterruptedException {
        ExcerptAppender app = queue.acquireAppender();
        int n = 1000;
        for (int i = 0; i < n; i++) {
            int index = i;
            app.writeBytes(bytes -> {
                bytes.writeInt(index);
                bytes.writeLong(System.nanoTime());
            });
            if ((i + 1) % eventsPerCycle == 0) {
                Thread.sleep(1000);
            }
        }
        logger.info("Written {} events", n);
    }

    private static void deleteFile(Path file) {
        if (Files.exists(file)) {
            try {
                logger.info("Deleting {}", file);
                Files.delete(file);
            } catch (IOException ex) {
                logger.error(String.valueOf(ex.getMessage()), ex);
                throw new IllegalStateException(ex);
            }
        }
    }

}
