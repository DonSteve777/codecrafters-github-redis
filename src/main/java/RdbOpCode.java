public enum RdbOpCode {
    EXPIRETIME_SECONDS(0xFD),
    EXPIRETIME_MS(0xFC);
    // FREQUENCY(0xF7),
    // RESIZEDB(0xFB),
    // EOF(0xFF);

    private final int code;

    RdbOpCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static RdbOpCode fromByte(int b) {
        for (RdbOpCode op : values()) {
            if (op.code == b) return op;
        }
        return null; // si no matchea → es un value-type
    }
}