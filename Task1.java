import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.*;

public class Task1 {
    public static void main(String[] args) throws Exception {
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {

            // Runnable: void, checked exception внутрь run() не пробросить
            System.out.println("Runnable get() = "
                    + pool.submit(() -> {}).get(1, TimeUnit.SECONDS));
            Future<?> r = pool.submit(() -> {
                try {
                    throw new IOException("сеть");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            try {
                System.out.println("Runnable get() = " + r.get(1, TimeUnit.SECONDS));
            } catch (ExecutionException e) {
                System.out.println("Runnable: " + e.getCause().getClass().getSimpleName());
            }

            // Callable: результат + throws Exception
            Future<Integer> ok = pool.submit(() -> 21 * 2);
            System.out.println("Callable get() = " + ok.get(1, TimeUnit.SECONDS));
            try {
                pool.submit(() -> { throw new IOException("сеть"); }).get(1, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                System.out.println("Callable: " + e.getCause());
            }

            Future<String> slow = pool.submit(() -> {
                Thread.sleep(1000);
                return "готово";
            });
            try {
                slow.get(80, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                System.out.println("timeout, isDone=" + slow.isDone() + " — это не сбой");
            }
            // без нового бюджета повторный get() снова блокирует на весь остаток
            try {
                slow.get(50, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                System.out.println("второй get с новым бюджетом, isDone=" + slow.isDone());
            }
            slow.cancel(true);

            Future<String> failed = pool.submit(() -> {
                throw new IllegalStateException("битые данные");
            });
            try {
                failed.get(1, TimeUnit.SECONDS);
            } catch (ExecutionException first) {
                long t = System.nanoTime();
                try {
                    failed.get(1, TimeUnit.SECONDS);
                } catch (ExecutionException second) {
                    System.out.println("ошибка уже в Future, повторный get ~"
                            + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + " мс");
                }
            }

            CountDownLatch closed = new CountDownLatch(1);
            Future<String> longTask = pool.submit(() -> {
                try {
                    for (int i = 0; i < 50; i++) {
                        if (Thread.currentThread().isInterrupted()) return "cancelled";
                        Thread.sleep(100);
                    }
                    return "done";
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // run/лямбда не throws — восстанавливаем флаг
                    return "interrupted";
                } finally {
                    closed.countDown();
                }
            });
            Thread.sleep(250);
            System.out.println("cancel(true) = " + longTask.cancel(true));
            closed.await(2, TimeUnit.SECONDS);
            System.out.println("тело вышло, isCancelled=" + longTask.isCancelled());
            try {
                longTask.get();
            } catch (CancellationException e) {
                System.out.println("get() после cancel → CancellationException, ждать тело он не будет");
            }

            CountDownLatch finished = new CountDownLatch(1);
            Future<?> blind = pool.submit(() -> {
                long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(150);
                while (System.nanoTime() < end) { }
                finished.countDown();
            });
            Thread.sleep(20);
            blind.cancel(true);
            System.out.println("cancel принят, цикл ещё идёт: " + (finished.getCount() == 1));
            finished.await(1, TimeUnit.SECONDS);
            System.out.println("interrupt — запрос: ставит флаг и будит sleep/wait/очередь. "
                    + "Поток не убивают. Кто флаг не смотрит и не блокируется interruptibly — доработает сам.");
        }
    }
}
