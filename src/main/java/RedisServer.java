import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Servidor Redis que maneja las conexiones de clientes y procesa comandos
 */
public class RedisServer {
    
    private static final int DEFAULT_PORT = 6379;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final RedisCommands redisCommands;
    private final Map<String, RedisCommand> commandMap;
    private final int port;


    public RedisServer(String dir, String dbfilename, int port) {
        this.redisCommands = new RedisCommands(dir, dbfilename);
        this.commandMap = createCommandMap();
        
        // Cargar datos del archivo RDB si existe
        loadInitialData(dir, dbfilename);
        this.port = port;
    }
    
    /**
     * Crea el mapa de comandos disponibles
     */
    private Map<String, RedisCommand> createCommandMap() {
        Map<String, RedisCommand> commands = new HashMap<>();
        commands.put("ECHO", RedisCommands.ECHO);
        commands.put("PING", RedisCommands.PING);
        commands.put("SET", redisCommands.createSetCommand());
        commands.put("GET", redisCommands.createGetCommand());
        commands.put("CONFIG", redisCommands.createConfigCommand());
        commands.put("KEYS", redisCommands.createKeysCommand());
        return commands;
    }
    
    /**
     * Carga los datos iniciales del archivo RDB
     */
    private void loadInitialData(String dir, String dbfilename) {
        if (dir != null && dbfilename != null) {
            try {
                Map<String, String> rdbData = RDBParser.parseRDB(dir, dbfilename);
                redisCommands.getDataStore().putAll(rdbData);
                System.out.println("Datos cargados desde RDB: " + rdbData.size() + " claves");
            } catch (IOException e) {
                System.err.println("Error cargando datos iniciales del RDB: " + e.getMessage());
            }
        }
    }
    
    /**
     * Inicia el servidor en el puerto especificado
     */
    public void start(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            setupShutdownHook(serverSocket);
            serverSocket.setReuseAddress(true);
            
            System.out.println("Servidor Redis iniciado en puerto " + port);
            
            while (running.get() && !serverSocket.isClosed()) {
                Socket clientSocket = serverSocket.accept();
                Thread clientThread = new Thread(() -> handleClient(clientSocket));
                clientThread.start();
            }
            
        } catch (IOException e) {
            System.err.println("Error en el servidor: " + e.getMessage());
        } finally {
            redisCommands.shutdown();
        }
    }
    
    /**
     * Inicia el servidor en el puerto por defecto
     */
    public void start() {
        start(port);
    }
    
    /**
     * Configura el hook de shutdown para cerrar graciosamente
     */
    private void setupShutdownHook(ServerSocket serverSocket) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            try {
                serverSocket.close();
            } catch (IOException e) {
                // Ignorar errores al cerrar
            }
        }));
    }
    
    /**
     * Maneja un cliente individual
     */
    private void handleClient(Socket clientSocket) {
        try (OutputStream out = clientSocket.getOutputStream();
             BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
            
            String line;
            while ((line = in.readLine()) != null) {
                processCommand(line, in, out);
            }
            
        } catch (IOException e) {
            System.err.println("Error manejando cliente: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                // Ignorar errores al cerrar
            }
        }
    }
    
    /**
     * Procesa un comando recibido del cliente
     */
    private void processCommand(String line, BufferedReader in, OutputStream out) throws IOException {
        System.out.println("xxx empezamos: ");
        int numArgs = Integer.parseInt(line.substring(1));
        System.out.println("numArgs " + numArgs);
        
        String[] args = new String[numArgs];
        
        // Leer los argumentos del comando
        for (int i = 0; i < numArgs; i++) {
            String lengthLine = in.readLine(); // línea con $N
            Integer.parseInt(lengthLine.substring(1)); // validar formato
            String value = in.readLine(); // valor real
            args[i] = value;
        }
        
        // Ejecutar el comando si existe
        RedisCommand command = commandMap.get(args[0]);
        if (command != null) {
            command.execute(args, out);
            out.flush();
        } else {
            System.err.println("Comando no reconocido: " + args[0]);
        }
    }
}
