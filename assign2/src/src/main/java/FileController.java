import java.nio.channels.Pipe;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class FileController implements Runnable {
    ScheduledExecutorService ses;
    Pipe pipe;
    public FileController(Pipe pipe) {
        this.pipe = pipe;
        this.ses = Executors.newScheduledThreadPool(Constants.MAX_THREADS);
    }

    @Override
    public void run() {

    }
}
