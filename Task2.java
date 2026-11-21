import java.util.List;
import java.util.concurrent.*;

public class Task2 {
    record Course(String code, String title, int hours) {}
    record User(long id, String name) {}
    record Enrollment(long courseId, String name) {}

    public static void main(String[] args) throws Exception {
        try (ExecutorService io = Executors.newFixedThreadPool(2, Thread.ofPlatform().name("io-", 1).factory())) {

            CompletableFuture.runAsync(() ->
                    System.out.println("без Executor → " + Thread.currentThread().getName())).join();

            CompletableFuture.supplyAsync(() -> {
                        log("supplyAsync, загрузка");
                        return new Course("JAVA-1", "Java", 120);
                    }, io)
                    .thenApply(c -> {
                        log("thenApply, формат");
                        return c.code() + " — " + c.title() + " (" + c.hours() + " ч)";
                    })
                    .thenAccept(card -> log("thenAccept: " + card))
                    .join();
            System.out.println("supplyAsync на io-пуле. thenApply/thenAccept без своего Executor: "
                    + "если предыдущая стадия ещё не готова, они идут в том же потоке io.");

            CompletableFuture.completedFuture("JAVA-1").thenApply(code -> {
                System.out.println("уже готовый future → " + Thread.currentThread().getName());
                return code;
            }).join();

            CompletableFuture<CompletableFuture<List<Enrollment>>> nested =
                    loadUser(1, io).thenApply(u -> loadEnrollments(u.id(), io));
            System.out.println("thenApply: внешний future уже готов, записи isDone=" + nested.join().isDone());

            CompletableFuture<List<Enrollment>> flat =
                    loadUser(2, io).thenCompose(u -> loadEnrollments(u.id(), io));
            System.out.println("thenCompose → " + flat.join());
            System.out.println("thenApply: T→U, если U — future, получается вложенность. "
                    + "thenCompose: T→CompletionStage<U>, снаружи плоский CompletableFuture<U>.");
        }
    }

    static CompletableFuture<User> loadUser(long id, Executor io) {
        return CompletableFuture.supplyAsync(() -> {
            log("пользователь " + id);
            return new User(id, "Alice");
        }, io);
    }

    static CompletableFuture<List<Enrollment>> loadEnrollments(long id, Executor io) {
        return CompletableFuture.supplyAsync(() -> {
            log("записи " + id);
            try { Thread.sleep(80); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return List.of(new Enrollment(101, "Concurrency"), new Enrollment(202, "Virtual Threads"));
        }, io);
    }

    static void log(String msg) {
        System.out.println("[" + Thread.currentThread().getName() + "] " + msg);
    }
}
