package com.baidu.fsg.uid;

import com.baidu.fsg.uid.business.CachedBusinessUidGenerator;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class CachedBusinessUidGeneratorTest {

    private static  int SIZE = 7000000; // 700w
    private static final boolean VERBOSE = false;
    private static final int THREADS = Runtime.getRuntime().availableProcessors() << 1;

    private CachedBusinessUidGenerator uidGenerator;

    /**
     * Test for serially generate
     *
     * @throws IOException
     */
    public void testSerialGenerate() throws IOException {
        // Generate UID serially
        Set<String> uidSet = new HashSet<>(SIZE);
        for (int i = 0; i < SIZE; i++) {
            try {
                doGenerate(uidSet, i);
            }catch (Exception e) {
                e.printStackTrace();
                SIZE--;
            }
        }

        // Check UIDs are all unique
        checkUniqueID(uidSet);
    }

    /**
     * Test for parallel generate
     *
     * @throws InterruptedException
     * @throws IOException
     */
    public void testParallelGenerate() throws InterruptedException, IOException {
        AtomicInteger control = new AtomicInteger(-1);
        Set<String> uidSet = new ConcurrentSkipListSet<>();

        // Initialize threads
        List<Thread> threadList = new ArrayList<>(THREADS);
        for (int i = 0; i < THREADS; i++) {
            Thread thread = new Thread(() -> workerRun(uidSet, control));
            thread.setName("UID-generator-" + i);

            threadList.add(thread);
            thread.start();
        }

        // Wait for worker done
        for (Thread thread : threadList) {
            thread.join();
        }

        // Check generate 700w times
        Assert.assertEquals(SIZE, control.get());

        // Check UIDs are all unique
        checkUniqueID(uidSet);
    }

    /**
     * Woker run
     */
    private void workerRun(Set<String> uidSet, AtomicInteger control) {
        for (; ; ) {
            int myPosition = control.updateAndGet(old -> (old == SIZE ? SIZE : old + 1));
            if (myPosition == SIZE) {
                return;
            }

            doGenerate(uidSet, myPosition);
        }
    }

    /**
     * Do generating
     */
    private void doGenerate(Set<String> uidSet, int index) {
        String uid = uidGenerator.getUID();
        boolean existed = !uidSet.add(uid);
        if (existed) {
            System.out.println("Found duplicate UID " + uid);
        }
    }

    /**
     * Check UIDs are all unique
     */
    private void checkUniqueID(Set<String> uidSet) throws IOException {
        System.out.println(uidSet.size());
        Assert.assertEquals(SIZE, uidSet.size());
    }

    public static void main(String[] args) throws Exception {
        CachedBusinessUidGeneratorTest test = new CachedBusinessUidGeneratorTest();
        CachedBusinessUidGenerator generator = new CachedBusinessUidGenerator();
        generator.setHighSeqBits(4);
        generator.setSequenceBits(4);
        generator.setWorkerIdAssigner(() -> 99);
        generator.afterPropertiesSet();
        test.uidGenerator = generator;

        test.testSerialGenerate();

    }
}
