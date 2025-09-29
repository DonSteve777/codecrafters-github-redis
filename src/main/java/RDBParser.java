import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Clase responsable de parsear archivos RDB de Redis
 * 
----------------------------#
52 45 44 49 53              # Magic String "REDIS"
30 30 30 33                 # RDB Version Number as ASCII string. "0003" = 3
----------------------------
FA                          # Auxiliary field
$string-encoded-key         # May contain arbitrary metadata
$string-encoded-value       # such as Redis version, creation time, used memory, ...
----------------------------
FE 00                       # Indicates database selector. db number = 00
FB                          # Indicates a resizedb field
$length-encoded-int         # Size of the corresponding hash table
$length-encoded-int         # Size of the corresponding expire hash table
----------------------------# Key-Value pair starts
FD $unsigned-int            # "expiry time in seconds", followed by 4 byte unsigned int
$value-type                 # 1 byte flag indicating the type of value
$string-encoded-key         # The key, encoded as a redis string
$encoded-value              # The value, encoding depends on $value-type
----------------------------
FC $unsigned long           # "expiry time in ms", followed by 8 byte unsigned long
$value-type                 # 1 byte flag indicating the type of value
$string-encoded-key         # The key, encoded as a redis string
$encoded-value              # The value, encoding depends on $value-type
----------------------------
$value-type                 # key-value pair without expiry
$string-encoded-key
$encoded-value
----------------------------
FE $length-encoding         # Previous db ends, next db starts.
----------------------------
...                         # Additional key-value pairs, databases, ...

FF                          ## End of RDB file indicator
8-byte-checksum             ## CRC64 checksum of the entire file.
 */
public class RDBParser {
    
    /**
     * Parsea un archivo RDB y extrae los pares clave-valor
     * 
     * @param dir directorio donde se encuentra el archivo RDB
     * @param dbfilename nombre del archivo RDB
     * @return Map con los pares clave-valor encontrados
     * @throws IOException si hay error leyendo el archivo
     */
    public static Map<String, String> parseRDB(String dir, String dbfilename) throws IOException {
        Map<String, String> redisData = new HashMap<>();
        byte[] data = Files.readAllBytes(Paths.get(dir + "/" + dbfilename));
        
        // MAL!!! funciona solamente para una db en el dbredisfile
        for (int i = 0; i < data.length; i++) {
            int val = data[i] & 0xFF;
            if (val == 0xFE) {
                RDBEntry entry = parseRDBEntry(data, i);
                if (entry != null) {
                    redisData.put(entry.key, entry.value);
                    // Por ahora solo procesamos una clave
                    break;
                }
            }
        }
        
        return redisData;
    }
    
    /**
     * Parsea una entrada individual del archivo RDB
     */
    private static RDBEntry parseRDBEntry(byte[] data, int startIndex) {
        try {
            int i = startIndex;
            // int dbIndex = data[i + 1] & 0xFF; // no utilizado por ahora
            // int FB = data[i + 2] & 0xFF; // no utilizado por ahora
            int tableSize = data[i + 3] & 0xFF;
            // int expTableSize = data[i + 4] & 0xFF; // no utilizado por ahora
            // int valueType = data[i + 5] & 0xFF; // no utilizado por ahora
            
            int keyLength = data[i + 6] & 0xFF;
            byte[] subKey = Arrays.copyOfRange(data, i + 7, i + 7 + keyLength);
            String key = new String(subKey, "UTF-8");
            
            int valueLength = data[i + 7 + keyLength] & 0xFF;
            byte[] subValue = Arrays.copyOfRange(data, i + 7 + keyLength + 1, 
                                                i + 7 + keyLength + 1 + valueLength);
            String value = new String(subValue, "UTF-8");
            
            return new RDBEntry(key, value, tableSize);
        } catch (Exception e) {
            System.err.println("Error parseando entrada RDB: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Clase interna para representar una entrada del RDB
     */
    static class RDBEntry {
        final String key;
        final String value;
        final int tableSize;
        
        RDBEntry(String key, String value, int tableSize) {
            this.key = key;
            this.value = value;
            this.tableSize = tableSize;
        }
    }
}
