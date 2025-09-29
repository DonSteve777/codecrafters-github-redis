import java.io.IOException;
import java.io.OutputStream;

/**
 * Interfaz funcional para los comandos de Redis
 */
@FunctionalInterface
public interface RedisCommand {
    void execute(String[] args, OutputStream out) throws IOException;
}
