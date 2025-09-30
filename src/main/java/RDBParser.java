import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import static java.lang.System.out;

/**
 * Clase responsable de parsear archivos RDB de Redis
 * https://rdb.fnordig.de/file_format.html#value-type
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
        out.println("dentro");
        // MAL!!! funciona solamente para una db en el dbredisfile
        int i = 0;
        int val = 0;
        while(i < data.length && val != 0xFE ){
            val = data[i] & 0xFF;
            i++;
        }
        i--;    // vuelvo a 0XFE, por no cambiar el codigo siguiente, que  no tiene en cuneta el i++
        out.println("0xFE encontrado. ");
        if (i < data.length){
            // DB start
            int dbIndex = data[i + 1] & 0xFF; // no utilizado por ahora
            int FB = data[i + 2] & 0xFF; // no utilizado por ahora
            int tableSize = data[i + 3] & 0xFF;
            int expTableSize = data[i + 4] & 0xFF; // no utilizado por ahora
            // System.out.println("db selector " + Integer.toHexString(dbIndex));
            // System.out.println( Integer.toHexString(FB));
            // System.out.println(Integer.toHexString(tableSize));
            // System.out.println(Integer.toHexString(expTableSize));
            out.println("cabecera descartada. ");
            /// entries
            val = 0;
            i+=5;
            // leo entradas hasta el flag de final de db 0xFE 
            while(i < data.length && val != 0xFE ){
                val = data[i] & 0xFF;
                i++;
                RdbOpCode op = RdbOpCode.fromByte(val); 
                if ( op != null){
                    System.out.println("expery time encontrado en RDFile");
                    System.out.println(Integer.toHexString(op.getCode())); 
                    break;
                }else{
                    System.out.println("value-type leido.");
                    // int valueType = firtsEntryByte;
                    RDBEntry entry = parseRDBEntry(data, i);
                    if (entry != null) {
                        redisData.put(entry.key, entry.value);
                        i = entry.currentByte;
                    }
                }
            }
        }
        // podria leer aqui los resize y hacer esto para inicializar el map :
        // redisData = new HashMap<>(resize);
        // read DB
        return redisData;
    }
    
    /**
     * Parsea una entrada individual del archivo RDB
     */
    private static RDBEntry parseRDBEntry(byte[] data, int startIndex) {
        try {
            int i = startIndex;
            int keyLength = data[i] & 0xFF;
            out.println("keyLength " + keyLength);
            i++;
            byte[] subKey = Arrays.copyOfRange(data, i , i + keyLength);
            i+= keyLength;
            String key = new String(subKey, "UTF-8");
            out.println("key " + key);
            
            int valueLength = data[i] & 0xFF;
            out.println("valueLength " + valueLength);
            i++;
            byte[] subValue = Arrays.copyOfRange(data, i, 
            i + valueLength);
            String value = new String(subValue, "UTF-8");
            out.println("value " + value);
            
            return new RDBEntry(key, value, i + valueLength );

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
        final int currentByte;
        
        RDBEntry(String key, String value, int currentByte ) {
            this.key = key;
            this.value = value;
            this.currentByte = currentByte;
        }
    }
}
