import java.util.*;
import java.util.concurrent.*;

public class Task3 {
    record Course(long id, String title) {}
    record LoadError(long id, String message) {}
    record Partial(List<Course> successes, List<LoadError> errors) {}

    public static void main(String[] args) throws Exception {
        List<Long> ids = List.of(1L, 2L, 3L, 4L);
        try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
            System.out.println("порядок запроса: " + ids);
            try {
                System.out.println("fail-fast: " + loadFailFast(pool, ids).join());
            } catch (CompletionException e) {
                System.out.println("fail-fast: " + unwrap(e) + " — списка курсов нет");
            }
            System.out.println("частичный: " + loadPartial(pool, ids).join());
            System.out.println("allOf → CompletableFuture<Void>. Порядок и тип держит List<CompletableFuture<Course>>. "
                    + "Fail-fast: любой сбой валит весь набор. Частичный: успехи по порядку id + список ошибок. "
                    + "allOf соседей сам не отменяет.");
        }
    }

    static CompletableFuture<List<Course>> loadFailFast(ExecutorService pool, List<Long> ids) {
        List<CompletableFuture<Course>> fs = ids.stream().map(id -> load(pool, id)).toList();
        return CompletableFuture.allOf(fs.toArray(CompletableFuture[]::new))
                .thenApply(__ -> fs.stream().map(CompletableFuture::join).toList());
    }

    static CompletableFuture<Partial> loadPartial(ExecutorService pool, List<Long> ids) {
        record Slot(Course course, LoadError error) {}
        List<CompletableFuture<Slot>> fs = ids.stream()
                .map(id -> load(pool, id).handle((c, e) -> e == null
                        ? new Slot(c, null)
                        : new Slot(null, new LoadError(id, unwrap(e).getMessage()))))
                .toList();
        return CompletableFuture.allOf(fs.toArray(CompletableFuture[]::new))
                .thenApply(__ -> {
                    List<Course> ok = new ArrayList<>();
                    List<LoadError> err = new ArrayList<>();
                    for (var f : fs) {
                        Slot s = f.join();
                        if (s.error() == null) ok.add(s.course());
                        else err.add(s.error());
                    }
                    return new Partial(List.copyOf(ok), List.copyOf(err));
                });
    }

    static CompletableFuture<Course> load(ExecutorService pool, long id) {
        return CompletableFuture.supplyAsync(() -> {
            if (id == 3) throw new RuntimeException("Курс " + id + " недоступен");
            return new Course(id, "Course-" + id);
        }, pool);
    }

    static Throwable unwrap(Throwable e) {
        while (e instanceof CompletionException && e.getCause() != null) e = e.getCause();
        return e;
    }
}
