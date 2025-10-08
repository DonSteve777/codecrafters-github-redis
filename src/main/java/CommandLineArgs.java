/**
 * Clase para manejar los argumentos de línea de comandos
 */
public class CommandLineArgs {
    
    private String dir;
    private String dbfilename;
    private static final int DEFAULT_PORT = 6379;
    private int port = DEFAULT_PORT;
    
    public CommandLineArgs(String[] args) {
        parseArgs(args);
    }
    
    /**
     * Parsea los argumentos de línea de comandos
     * Formato esperado: --dir <directorio> --dbfilename <archivo>
     */
    private void parseArgs(String[] args) {
       // if (args.length >= 4) {
            for (int i = 0; i < args.length - 1; i++) {
                switch (args[i]) {
                    case "--dir":
                        this.dir = args[i + 1];
                        break;
                    case "--dbfilename":
                        this.dbfilename = args[i + 1];
                        break;
                    case "--port":
                        this.port = Integer.parseInt(args[i + 1]);
                        break;
                }
            }
       // }
    }
    
    /**
     * Valida que los argumentos requeridos estén presentes
     */
    public boolean isValid() {
        return dir != null && dbfilename != null;
    }
    
    public String getDir() {
        return dir;
    }
    
    public String getDbfilename() {
        return dbfilename;
    }

    public int getPort() {
        return port;
    }
    
    @Override
    public String toString() {
        return String.format("CommandLineArgs{dir='%s', dbfilename='%s'}", dir, dbfilename);
    }
}
