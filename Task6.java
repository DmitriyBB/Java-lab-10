import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Task6 {
    /** Дедлайн от System.nanoTime — монотонные часы, не Instant/wall clock. */
    record Budget(long deadlineNanos) {
        static Budget ofMillis(long ms) {
            return new Budget(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ms));
        }

        long remainingNanos() {
            return deadlineNanos - System.nanoTime();
        }
    }

    record Running(String name, Future<String> f, CountDownLatch done) {}

    public static void main(String[] args) throws Exception {
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            Budget budget = Budget.ofMillis(400);
            List<Running> all = new ArrayList<>();

            System.out.println("A: " + stage(pool, budget, all, "A", 80));

            Running b = start(pool, all, "B", 80);
            Running c = start(pool, all, "C", 2000);
            System.out.println("B: " + await(budget, b));
            System.out.println("C: " + await(budget, c));

            if (budget.remainingNanos() <= 0) {
                System.out.println("D не стартовала: бюджет кончился");
            }

            for (Running r : all) r.f.cancel(true);
            for (Running r : all) {
                r.done.await(2, TimeUnit.SECONDS);
                System.out.println(r.name + " тело завершено, cancelled=" + r.f.isCancelled());
            }
            System.out.println("Structured concurrency: подзадачи — дети блока. Ошибка/дедлайн "
                    + "отменяет остальных, выйти можно только после их конца. Иначе задача в пуле "
                    + "переживает return, ошибка и отмена теряются. Preview API нет — остаток, "
                    + "cancel и join собраны вручную. get() после cancel не ждёт тело, поэтому latch.");
        }
    }

    static String stage(ExecutorService pool, Budget b, List<Running> all, String name, long workMs)
            throws InterruptedException {
        if (b.remainingNanos() <= 0) return null;
        return await(b, start(pool, all, name, workMs));
    }

    static Running start(ExecutorService pool, List<Running> all, String name, long workMs) {
        CountDownLatch done = new CountDownLatch(1);
        Future<String> f = pool.submit(() -> {
            try {
                long left = workMs;
                while (left > 0) {
                    long slice = Math.min(20, left);
                    Thread.sleep(slice);
                    left -= slice;
                }
                return name + " done";
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return name + " interrupted";
            } finally {
                done.countDown();
            }
        });
        Running r = new Running(name, f, done);
        all.add(r);
        return r;
    }

    static String await(Budget b, Running r) throws InterruptedException {
        long left = b.remainingNanos();
        if (left <= 0) {
            r.f.cancel(true);
            return "deadline";
        }
        try {
            return r.f.get(left, TimeUnit.NANOSECONDS);
        } catch (TimeoutException | CancellationException e) {
            r.f.cancel(true);
            return "cancelled";
        } catch (ExecutionException e) {
            return "error: " + e.getCause();
        }
    }
}
