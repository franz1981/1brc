/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import sun.misc.Unsafe;

/**
 * Changelog:
 *
 * Initial submission:               62000 ms
 * Chunked reader:                   16000 ms
 * Optimized parser:                 13000 ms
 * Branchless methods:               11000 ms
 * Adding memory mapped files:       6500 ms (based on bjhara's submission)
 * Skipping string creation:         4700 ms
 * Custom hashmap...                 4200 ms
 * Added SWAR token checks:          3900 ms
 * Skipped String creation:          3500 ms (idea from kgonia)
 * Improved String skip:             3250 ms
 * Segmenting files:                 3150 ms (based on spullara's code)
 * Not using SWAR for EOL:           2850 ms
 * Inlining hash calculation:        2450 ms
 * Replacing branchless code:        2200 ms (sometimes we need to kill the things we love)
 * Added unsafe memory access:       1900 ms (keeping the long[] small and local)
 * Fixed bug, UNSAFE bytes String:   1850 ms
 * Separate hash from entries:       1550 ms
 * Various tweaks for Linux/cache    1550 ms (should/could make a difference on target machine)
 *
 */
public class CalculateAverage_royvanrijn {

    private static final String FILE = "/tmp/measurements.txt";

    private static final Unsafe UNSAFE = initUnsafe();
    private static final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

    private static Unsafe initUnsafe() {
        try {
            final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        new CalculateAverage_royvanrijn().run();
    }

    public void run() throws Exception {

        // Calculate input segments.
        final int numberOfChunks = Runtime.getRuntime().availableProcessors();
        final long[] chunks = getSegments(numberOfChunks);

        final List<MeasurementRepository> repositories = IntStream.range(0, chunks.length - 1)
                .mapToObj(chunkIndex -> processMemoryArea(chunks[chunkIndex], chunks[chunkIndex + 1]))
                .parallel()
                .toList();

        // Sometimes simple is better:
        final HashMap<String, byte[]> measurements = HashMap.newHashMap(1 << 10);
        for (MeasurementRepository repository : repositories) {
            for (int i = 0; i < repository.cities.length; i++) {
                String city = repository.cities[i];
                if (city != null) {
                    measurements.merge(city, repository.table[i], MeasurementRepository.EntryFlyweigth::mergeWith);
                }
            }
        }

        System.out.print("{" +
                measurements.entrySet().stream().sorted(Map.Entry.comparingByKey())
                        .map(entry -> entry.getKey() + '=' + MeasurementRepository.EntryFlyweigth.toMeasurementString(entry.getValue()))
                        .collect(Collectors.joining(", ")));
        System.out.println("}");
    }

    /**
     * Simpler way to get the segments and launch parallel processing by thomaswue
     */
    private static long[] getSegments(final int numberOfChunks) throws IOException {
        try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            final long fileSize = fileChannel.size();
            final long segmentSize = (fileSize + numberOfChunks - 1) / numberOfChunks;
            final long[] chunks = new long[numberOfChunks + 1];
            final long mappedAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
            chunks[0] = mappedAddress;
            final long endAddress = mappedAddress + fileSize;
            for (int i = 1; i < numberOfChunks; ++i) {
                long chunkAddress = mappedAddress + i * segmentSize;
                // Align to first row start.
                while (chunkAddress < endAddress && UNSAFE.getByte(chunkAddress++) != '\n') {
                    // nop
                }
                chunks[i] = Math.min(chunkAddress, endAddress);
            }
            chunks[numberOfChunks] = endAddress;
            return chunks;
        }
    }

    private MeasurementRepository processMemoryArea(final long fromAddress, final long toAddress) {

        MeasurementRepository repository = new MeasurementRepository();
        long ptr = fromAddress;
        final byte[] dataBuffer = new byte[16 * 8];
        while ((ptr = processName(dataBuffer, ptr, toAddress, repository)) < toAddress) {
            // empty loop
        }

        return repository;
    }

    private static final long SEPARATOR_PATTERN = compilePattern((byte) ';');

    /**
     * Already looping the longs here, lets shoehorn in making a hash
     */
    private long processName(final byte[] data, final long start, final long limit, final MeasurementRepository measurementRepository) {
        int hash = 1;
        long i;
        int dataPtr = 0;
        for (i = start; i <= limit - 8; i += 8) {
            long word = UNSAFE.getLong(i);
            if (isBigEndian) {
                word = Long.reverseBytes(word); // Reversing the bytes is the cheapest way to do this
            }
            final long match = word ^ SEPARATOR_PATTERN;
            long mask = ((match - 0x0101010101010101L) & ~match) & 0x8080808080808080L;

            if (mask != 0) {
                final long partialWord = word & ((mask >> 7) - 1);
                hash = longHashStep(hash, partialWord);
                UNSAFE.putLong(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + dataPtr * Long.BYTES, partialWord);
                final int index = Long.numberOfTrailingZeros(mask) >> 3;
                return processNumber(start, i + index, hash, data, measurementRepository);
            }
            UNSAFE.putLong(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + dataPtr * Long.BYTES, word);
            dataPtr++;
            hash = longHashStep(hash, word);
        }
        // Handle remaining bytes near the limit of the buffer:
        long partialWord = 0;
        int len = 0;
        for (; i < limit; i++) {
            byte read;
            if ((read = UNSAFE.getByte(i)) == ';') {
                hash = longHashStep(hash, partialWord);
                UNSAFE.putLong(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + dataPtr * Long.BYTES, partialWord);
                return processNumber(start, i, hash, data, measurementRepository);
            }
            partialWord = partialWord | ((long) read << len);
            len += 8;
        }
        return limit;
    }

    private static final long DOT_BITS = 0x10101000;
    private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

    /**
     * Awesome branchless parser by merykitty.
     */
    private long processNumber(final long startAddress, final long delimiterAddress, final int hash, final byte[] data,
                               final MeasurementRepository measurementRepository) {

        long word = UNSAFE.getLong(delimiterAddress + 1);
        if (isBigEndian) {
            word = Long.reverseBytes(word);
        }

        final long invWord = ~word;
        final int decimalSepPos = Long.numberOfTrailingZeros(invWord & DOT_BITS);
        final long signed = (invWord << 59) >> 63;
        final long designMask = ~(signed & 0xFF);
        final long digits = ((word & designMask) << (28 - decimalSepPos)) & 0x0F000F0F00L;
        final long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
        final int measurement = (int) ((absValue ^ signed) - signed);

        // Store this entity:
        measurementRepository.update(startAddress, data, (int) (delimiterAddress - startAddress), hash, measurement);

        // Return the next address:
        return delimiterAddress + (decimalSepPos >> 3) + 4;
    }

    // branchless max (unprecise for large numbers, but good enough)
    static int max(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return a - (diff & dsgn);
    }

    // branchless min (unprecise for large numbers, but good enough)
    static int min(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return b + (diff & dsgn);
    }

    private static int longHashStep(final int hash, final long word) {
        return 31 * hash + (int) (word ^ (word >>> 32));
    }

    private static long compilePattern(final byte value) {
        return ((long) value << 56) | ((long) value << 48) | ((long) value << 40) | ((long) value << 32) |
                ((long) value << 24) | ((long) value << 16) | ((long) value << 8) | (long) value;
    }

    /**
     * Extremely simple linear probing hashmap that should work well enough.
     */
    static class MeasurementRepository {
        private static final int TABLE_SIZE = 1 << 18; // large enough for the contest.
        private static final int TABLE_MASK = (TABLE_SIZE - 1);

        private final byte[][] table = new byte[TABLE_SIZE][];
        private final String[] cities = new String[TABLE_SIZE];

        static final class EntryFlyweigth {
            // binary layout is:
            // 16 bytes byte[] header
            // int hash;
            // int min;
            // int max;
            // int count;
            // long sum;
            // byte[] data; // no need to align here, we can be exact
            private static final int HASH_OFFSET = Unsafe.ARRAY_BYTE_BASE_OFFSET;
            private static final int MIN_OFFSET = HASH_OFFSET + Integer.BYTES;
            private static final int MAX_OFFSET = MIN_OFFSET + Integer.BYTES;
            private static final int COUNT_OFFSET = MAX_OFFSET + Integer.BYTES;
            private static final int SUM_OFFSET = COUNT_OFFSET + Integer.BYTES;
            private static final int DATA_OFFSET = SUM_OFFSET + Long.BYTES;
            private static final int DATA_INDEX = DATA_OFFSET - Unsafe.ARRAY_BYTE_BASE_OFFSET;

            public static byte[] createEntry(byte[] data, int length, int hash) {
                byte[] entry = new byte[DATA_INDEX + length];
                UNSAFE.putInt(entry, HASH_OFFSET, hash);
                UNSAFE.putInt(entry, MIN_OFFSET, 1000);
                UNSAFE.putInt(entry, MAX_OFFSET, -1000);
                UNSAFE.putInt(entry, COUNT_OFFSET, 0);
                UNSAFE.putLong(entry, SUM_OFFSET, 0);
                System.arraycopy(data, 0, entry, DATA_INDEX, length);
                return entry;
            }

            public static int hash(byte[] self) {
                return UNSAFE.getInt(self, HASH_OFFSET);
            }

            public static void updateWith(byte[] self, int measurement) {
                UNSAFE.putInt(self, MIN_OFFSET, min(UNSAFE.getInt(self, MIN_OFFSET), measurement));
                UNSAFE.putInt(self, MAX_OFFSET, max(UNSAFE.getInt(self, MAX_OFFSET), measurement));
                UNSAFE.putInt(self, COUNT_OFFSET, UNSAFE.getInt(self, COUNT_OFFSET) + 1);
                UNSAFE.putLong(self, SUM_OFFSET, UNSAFE.getLong(self, SUM_OFFSET) + measurement);
            }

            public static byte[] mergeWith(byte[] self, byte[] entry) {
                UNSAFE.putInt(self, MIN_OFFSET, min(UNSAFE.getInt(self, MIN_OFFSET), UNSAFE.getInt(entry, MIN_OFFSET)));
                UNSAFE.putInt(self, MAX_OFFSET, max(UNSAFE.getInt(self, MAX_OFFSET), UNSAFE.getInt(entry, MAX_OFFSET)));
                UNSAFE.putInt(self, COUNT_OFFSET, UNSAFE.getInt(self, COUNT_OFFSET) + UNSAFE.getInt(entry, COUNT_OFFSET));
                UNSAFE.putLong(self, SUM_OFFSET, UNSAFE.getLong(self, SUM_OFFSET) + UNSAFE.getLong(entry, SUM_OFFSET));
                return self;
            }

            public static String toMeasurementString(byte[] self) {
                int min = UNSAFE.getInt(self, MIN_OFFSET);
                int max = UNSAFE.getInt(self, MAX_OFFSET);
                int count = UNSAFE.getInt(self, COUNT_OFFSET);
                long sum = UNSAFE.getLong(self, SUM_OFFSET);
                return round(min) + "/" + round((1.0 * sum) / count) + "/" + round(max);
            }

            private static double round(double value) {
                return Math.round(value) / 10.0;
            }
        }

        public void update(final long address, final byte[] data, final int length, final int hash, final int temperature) {

            int index = hash & TABLE_MASK;
            byte[] tableEntry;
            while ((tableEntry = table[index]) != null
                    && (EntryFlyweigth.hash(tableEntry) != hash
                            || Arrays.mismatch(tableEntry, EntryFlyweigth.DATA_INDEX, EntryFlyweigth.DATA_INDEX + length, data, 0, length) >= 0)) { // search for the right spot
                index = (index + 1) & TABLE_MASK;
            }

            if (tableEntry == null) {
                tableEntry = createNewEntry(address, data, length, hash, index);
            }
            EntryFlyweigth.updateWith(tableEntry, temperature);
        }

        private byte[] createNewEntry(final long address, final byte[] data, final int length, final int hash, final int index) {
            // Add the entry:
            final byte[] tableEntry = EntryFlyweigth.createEntry(data, length, hash);
            table[index] = tableEntry;
            final String city = new String(tableEntry, EntryFlyweigth.DATA_INDEX, length, StandardCharsets.UTF_8);
            cities[index] = city;
            return tableEntry;
        }
    }

}
