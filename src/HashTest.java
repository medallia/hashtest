import com.google.common.collect.ImmutableMap;
import net.openhft.koloboke.collect.map.hash.HashIntLongMap;
import net.openhft.koloboke.collect.map.hash.HashIntLongMaps;
import net.openhft.koloboke.collect.map.hash.HashIntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;
import net.openhft.koloboke.function.IntLongConsumer;
import net.openhft.koloboke.function.IntObjConsumer;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class HashTest {
	private static final int SIZE = 1000;
	private static final int TIMES = 10000;

	private interface HashTester {
		void put();

		long streamSumAll();

		long getSumAll();
	}

	private abstract static class HashMapTesterBase implements HashTester {
		protected final Map<Integer, Long> map = new HashMap<>(SIZE);

		@Override
		public void put() {
			for (int i = 0; i < SIZE; ++i)
				map.put(i, i * 3L);
		}

		@Override
		public long getSumAll() {
			long sum = 0;
			for (int i = 0; i < SIZE; ++i)
				sum += map.get(i);
			return sum;
		}
	}

	private static class HashMapTesterKeyset extends HashMapTesterBase {
		@Override
		public long streamSumAll() {
			long sum = 0;
			for (Long l : map.values()) {
				sum += l;
			}
			return sum;
		}
	}

	private static class HashMapTesterReduce extends HashMapTesterBase {
		@Override
		public long streamSumAll() {
			return map.values().stream().mapToLong(t -> t.longValue()).sum();
		}
	}

	private static class HashMapTesterPReduce extends HashMapTesterBase {
		@Override
		public long streamSumAll() {
			return map.values().parallelStream().mapToLong(t -> t.longValue()).sum();
		}
	}

	private static class HashMapTesterForeach extends HashMapTesterBase {
		private static class Longholder {
			public long sum = 0;
		}

		@Override
		public long streamSumAll() {
			final Longholder lh = new Longholder();
			map.forEach((key, value) -> lh.sum += value);
			return lh.sum;
		}
	}

	private static class IntKoloboke implements HashTester {
		private final HashIntLongMap map = HashIntLongMaps.newMutableMap(SIZE);

		@Override
		public void put() {
			for (int i = 0; i < SIZE; ++i)
				map.put(i, i * 3L);
		}

		private static class Longholder {
			public long sum = 0;
		}

		@Override
		public long streamSumAll() {
			final Longholder lh = new Longholder();
			map.forEach((IntLongConsumer) (key, value) -> lh.sum += value);
			return lh.sum;
		}

		@Override
		public long getSumAll() {
			long sum = 0;
			for (int i = 0; i < SIZE; ++i)
				sum += map.get(i);
			return sum;
		}

	}

	private static class IntObjKoloboke implements HashTester {
		private final HashIntObjMap<Long> map = HashIntObjMaps.newMutableMap(SIZE);

		@Override
		public void put() {
			for (int i = 0; i < SIZE; ++i)
				map.put(i, Long.valueOf(i * 3L));
		}

		private static class Longholder {
			public long sum = 0;
		}

		@Override
		public long streamSumAll() {
			final Longholder lh = new Longholder();
			map.forEach((IntObjConsumer<Long>) (key, value) -> lh.sum += value);
			return lh.sum;
		}

		@Override
		public long getSumAll() {
			long sum = 0;
			for (int i = 0; i < SIZE; ++i)
				sum += map.get(i);
			return sum;
		}

	}

	private static void benchmark(Supplier<HashTester> constructor) {
		System.gc();
		System.gc();

		final MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();

		MemoryUsage preMemory = mbean.getHeapMemoryUsage();

		long putTime = Long.MAX_VALUE, streamTime = Long.MAX_VALUE, sumTime = Long.MAX_VALUE;

		long startMem = preMemory.getUsed();

		HashTester ht = constructor.get();
		long sum = 0;

		for (int i = 0; i < TIMES; ++i) {
			final long start = System.nanoTime();
			ht.put();
			final long stop = System.nanoTime();
			putTime = Long.min(putTime, stop - start);
		}
		for (int i = 0; i < TIMES; ++i) {
			final long start = System.nanoTime();
			sum += ht.streamSumAll();
			final long stop = System.nanoTime();
			streamTime = Long.min(streamTime, stop - start);
		}

		for (int i = 0; i < TIMES; ++i) {
			final long start = System.nanoTime();
			sum += ht.getSumAll();
			final long stop = System.nanoTime();
			sumTime = Long.min(sumTime, stop - start);
		}

		System.gc();
		System.gc();

		MemoryUsage postMemory = mbean.getHeapMemoryUsage();
		long stopMem = postMemory.getUsed();

		System.out.format("%30s %12d %12d %12d: %12d [%12d]\n", ht.getClass().getName(), putTime, streamTime, sumTime, sum, stopMem - startMem);
	}

	private static long warmUp(Supplier<HashTester> constructor) {
		long sum = 0;
		for (int i = 0; i < TIMES; ++i) {
			HashTester ht = constructor.get();
			ht.put();
			sum += ht.getSumAll();
			sum += ht.streamSumAll();
		}
		System.gc();
		return sum;
	}

	public static void main(String[] args) {
		long sum = 0;

		sum += warmUp(HashMapTesterKeyset::new);
		sum += warmUp(HashMapTesterReduce::new);
		sum += warmUp(HashMapTesterPReduce::new);
		sum += warmUp(HashMapTesterForeach::new);
		sum += warmUp(IntKoloboke::new);
		sum += warmUp(IntObjKoloboke::new);
		System.out.println("Warmed for " + sum);

		benchmark(HashMapTesterKeyset::new);
		benchmark(HashMapTesterKeyset::new);
		benchmark(HashMapTesterReduce::new);
		benchmark(HashMapTesterPReduce::new);
		benchmark(HashMapTesterForeach::new);
		benchmark(IntKoloboke::new);
		benchmark(IntObjKoloboke::new);
	}
}
