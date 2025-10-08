import static java.lang.System.out;

/**
 * Clase principal del servidor Redis refactorizado
 */
public class Main {

  public static void main(String[] args) {
    out.println("¡Iniciando servidor Redis refactorizado!");
    
    // Parsear argumentos de línea de comandos
    CommandLineArgs cmdArgs = new CommandLineArgs(args);
    
    // Crear e iniciar el servidor Redis
    RedisServer server = new RedisServer(cmdArgs.getDir(), cmdArgs.getDbfilename(), cmdArgs.getPort());
    server.start();
  }
}
