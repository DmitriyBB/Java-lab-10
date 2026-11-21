import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Один прогон с фиксированным N. Не benchmark: нет серии, прогрева JIT и изоляции шума.
 * Sleep — ожидание, не модель CPU.
 */
public class Task5 {
    public static void main(String[] args) throws Exception {
        int n = 400;
        long waitMs = 50;
        System.out.println("Java " + Runtime.version() + ", ядер " + Runtime.getRuntime().availableProcessors());
        System.out.println("platform per task: " + measure(() ->
                Executors.newThreadPerTaskExecutor(Thread.ofPlatform().factory()), n, waitMs));
        System.out.println("virtual per task:  " + measure(Executors::newVirtualThreadPerTaskExecutor, n, waitMs));
        System.out.println("platform pool 8:   " + measure(() -> Executors.newFixedThreadPool(8), n, waitMs));
        System.out.println("Цифры одного прогона. VT почти не добавляют platform threads: на блокировке "
                + "снимаются с carrier. Маленький pool растягивает то же ожидание. "
                + "Стеки OS-потоков видны в committed memory, не в куче.");
    }

    static String measure(Supplier<ExecutorService> factory, int n, long waitMs) throws Exception {
        var mx = ManagementFactory.getThreadMXBean();
        mx.resetPeakThreadCount();
        long mem0 = committed();
        long t0 = System.nanoTime();
        try (var pool = factory.get()) {
            List<Future<?>> fs = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                fs.add(pool.submit(() -> {
                    Thread.sleep(waitMs);
                    return null;
                }));
            }
            for (var f : fs) f.get();
        }
        long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        return "время ~" + ms + " мс, пик platform threads " + mx.getPeakThreadCount()
                + ", Δ committed ~" + ((committed() - mem0) / (1024 * 1024)) + " МиБ";
    }

    static long committed() {
        return ManagementFactory.getPlatformMXBean(com.sun.management.OperatingSystemMXBean.class)
                .getCommittedVirtualMemorySize();
    }
}
