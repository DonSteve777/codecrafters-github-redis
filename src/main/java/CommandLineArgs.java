/**
 * Clase para manejar los argumentos de línea de comandos
 */
public class CommandLineArgs {
    
    private String dir;
    private String dbfilename;
    
    public CommandLineArgs(String[] args) {
        parseArgs(args);
    }
    
    /**
     * Parsea los argumentos de línea de comandos
     * Formato esperado: --dir <directorio> --dbfilename <archivo>
     */
    private void parseArgs(String[] args) {
        if (args.length >= 4) {
            for (int i = 0; i < args.length - 1; i++) {
                switch (args[i]) {
                    case "--dir":
                        this.dir = args[i + 1];
                        break;
                    case "--dbfilename":
                        this.dbfilename = args[i + 1];
                        break;
                }
            }
        }
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
    
    @Override
    public String toString() {
        return String.format("CommandLineArgs{dir='%s', dbfilename='%s'}", dir, dbfilename);
    }
}
