import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Task4 {
    public static void main(String[] args) throws Exception {
        starvation();
        split();
        blockingOnVirtual();
    }

    /** Оба потока пула ждут I/O — короткая CPU-задача не стартует, хотя процессор свободен. */
    static void starvation() throws Exception {
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            CountDownLatch holding = new CountDownLatch(2);
            CountDownLatch release = new CountDownLatch(1);
            for (int i = 0; i < 2; i++) {
                pool.submit(() -> { holding.countDown(); release.await(); return null; });
            }
            holding.await();
            AtomicBoolean started = new AtomicBoolean();
            Future<String> quick = pool.submit(() -> { started.set(true); return "ok"; });
            try {
                quick.get(150, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                System.out.println("starvation: started=" + started.get());
            }
            release.countDown();
            System.out.println("после освобождения: " + quick.get(1, TimeUnit.SECONDS));
        }
    }

    static void split() throws Exception {
        int cores = Runtime.getRuntime().availableProcessors();
        // CPU: потоков по числу ядер — больше platform-потоков ядра не ускоряют
        try (ExecutorService cpu = Executors.newFixedThreadPool(cores);
             ExecutorService io = Executors.newVirtualThreadPerTaskExecutor()) {
            CountDownLatch release = new CountDownLatch(1);
            for (int i = 0; i < 32; i++) io.submit(() -> { release.await(); return null; });
            long t = System.nanoTime();
            long v = cpu.submit(() -> burn(30)).get(2, TimeUnit.SECONDS);
            System.out.println("CPU-пул (" + cores + ") ~"
                    + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + " мс, пока 32 VT ждут, v=" + v);
            release.countDown();
        }
        long fixed = parallel(Executors.newFixedThreadPool(cores), cores);
        long virtual = parallel(Executors.newVirtualThreadPerTaskExecutor(), cores);
        System.out.println("тот же счёт: fixed ~" + fixed + " мс, virtual ~" + virtual + " мс");
        System.out.println("Virtual thread на блокировке снимается с carrier — ждать могут тысячи. "
                + "На CPU он занимает carrier, carrier'ов ~ядра, поэтому счёт не ускоряется. "
                + "I/O: VT per task, без потолка «на глаз». Cached platform pool на каждую блокировку "
                + "держит OS-поток и стек ~МиБ.");
    }

    /** Прямой блокирующий код, обычный catch. */
    static void blockingOnVirtual() throws Exception {
        try (ExecutorService io = Executors.newVirtualThreadPerTaskExecutor()) {
            System.out.println(call(io, "JAVA-1"));
            try {
                call(io, "");
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }

    static String call(ExecutorService io, String id) throws Exception {
        try {
            return io.submit(() -> read(id)).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception ex) throw ex;
            throw e;
        }
    }

    static String read(String id) throws IOException, InterruptedException {
        Thread.sleep(20); // ожидание I/O, не CPU
        if (id.isBlank()) throw new IOException("пустое тело");
        return id + " — Java (120 ч)";
    }

    static long parallel(ExecutorService ex, int n) throws Exception {
        try (ex) {
            long t = System.nanoTime();
            List<Future<Long>> fs = new ArrayList<>();
            for (int i = 0; i < n; i++) fs.add(ex.submit(() -> burn(40)));
            for (var f : fs) f.get(5, TimeUnit.SECONDS);
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t);
        }
    }

    static long burn(long ms) {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ms);
        long v = 0;
        while (System.nanoTime() < end) v = v * 1664525 + 1013904223;
        return v;
    }
}
