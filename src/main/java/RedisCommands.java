import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Clase que contiene la implementación de todos los comandos Redis
 */
public class RedisCommands {
    
    private final Map<String, String> dataStore = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final String dir;
    private final String dbfilename;
    
    public RedisCommands(String dir, String dbfilename) {
        this.dir = dir;
        this.dbfilename = dbfilename;
    }
    
    public Map<String, String> getDataStore() {
        return dataStore;
    }
    
    /**
     * Comando ECHO - devuelve el mensaje recibido
     */
    public static final RedisCommand ECHO = (args, out) -> {
        System.out.println("xxx ECHO");
        for (int i = 1; i < args.length; i++) {
            out.write(("+" + args[i] + "\r\n").getBytes());
        }
    };
    
    /**
     * Comando PING - responde con PONG
     */
    public static final RedisCommand PING = (_args, out) -> {
        out.write("+PONG\r\n".getBytes());
    };
    
    /**
     * Comando SET - establece un valor para una clave
     */
    public RedisCommand createSetCommand() {
        return (args, out) -> {
            System.out.println("xxx SET");
            dataStore.put(args[1], args[2]);
            out.write("+OK\r\n".getBytes());
            
            // Manejo de expiración si se proporciona
            if (args.length > 3) {
                handleExpiration(args);
            }
        };
    }
    
    /**
     * Comando GET - obtiene el valor de una clave
     */
    public RedisCommand createGetCommand() {
        return (args, out) -> {
            System.out.println("xxx GET " + args[1]);
            String value = dataStore.get(args[1]);
            System.out.println("xxx value " + value);
            
            if (value == null) {
                out.write("$-1\r\n".getBytes());
            } else {
                out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
            }
        };
    }
    
    /**
     * Comando CONFIG - maneja la configuración
     */
    public RedisCommand createConfigCommand() {
        return (args, out) -> {
            if (args[1].equals("GET")) {
                if (args[2].equals("dir")) {
                    out.write(("*2\r\n$3\r\ndir\r\n$" + dir.length() + "\r\n" + dir + "\r\n").getBytes());
                } else if (args[2].equals("dbfilename")) {
                    out.write(("*2\r\n$10\r\ndbfilename\r\n$" + dbfilename.length() + "\r\n" + dbfilename + "\r\n").getBytes());
                }
            }
        };
    }
    
    /**
     * Comando KEYS - lista las claves que coinciden con un patrón
     */
    public RedisCommand createKeysCommand() {
        return (_args, out) -> {
            try {
                Map<String, String> rdbData = RDBParser.parseRDB(dir, dbfilename);
                dataStore.putAll(rdbData);
                
                StringBuilder sb = new StringBuilder();
                sb.append("*").append(rdbData.size()).append("\r\n");
                
                for (String key : rdbData.keySet()) {
                    sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                }
                
                out.write(sb.toString().getBytes());
            } catch (IOException e) {
                System.err.println("Error leyendo archivo RDB: " + dir + "/" + dbfilename);
                out.write("*0\r\n".getBytes());
            }
        };
    }
    
    /**
     * Maneja la expiración de claves
     */
    private void handleExpiration(String[] args) {
        System.out.println("xxx SET con EX");
        
        if (!args[3].equals("px")) {
            throw new IllegalArgumentException("Argumento inválido");
        }
        
        long expirationTime = Long.parseLong(args[4]);
        String key = args[1];
        
        // Programa la eliminación de la clave usando ScheduledExecutorService
        scheduler.schedule(() -> {
            dataStore.remove(key);
            System.out.println("expire key " + key);
        }, expirationTime, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Cierra el scheduler cuando ya no se necesite
     */
    public void shutdown() {
        scheduler.shutdown();
    }
}
