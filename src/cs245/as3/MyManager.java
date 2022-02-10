package cs245.as3;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class MyManager {

    private long txID;
    private int txOffset;
    private HashMap<Long, byte[]> writeset;
    private byte[][] records;

    public MyManager(long txID) {
        this.txID = txID;
        this.writeset = null;
        this.records = null;
    }

    public void writeset2Records() {
        records = new byte[writeset.size()+2][128];
        ByteBuffer buffer;

        // start
        buffer = ByteBuffer.allocate(10);
        buffer.put(new byte[]{10});
        buffer.putLong(txID);
        buffer.put(new byte[]{-1});
        records[0] = buffer.array();

        // data update log
        int line = 1;
        for (Long key: writeset.keySet()) {
            records[line++] = writesetEntry2Record(key, writeset.get(key));
        }

        // commit
        buffer = ByteBuffer.allocate(10);
        buffer.put(new byte[]{10});
        buffer.putLong(txID);
        buffer.put(new byte[]{-2});
        records[writeset.size()+1] = buffer.array();
    }

    public byte[] writesetEntry2Record(Long key, byte[] value) {

        // record = recordLength(1) + txID(8) + key(8)  + value(?<=100)
        byte recordLength = (byte) (Byte.BYTES + Long.BYTES + Long.BYTES + value.length);
        ByteBuffer buffer = ByteBuffer.allocate(recordLength);
        buffer.put(recordLength);
        buffer.putLong(txID);
        buffer.putLong(key);
        buffer.put(value);

        return buffer.array();
    }


    public byte[][] getRecords() {
        return records;
    }

    public HashMap<Long, byte[]> getWriteset() {
        return writeset;
    }

    public void setWriteset(HashMap<Long, byte[]> writeset) {
        this.writeset = writeset;
    }

    public int getTxOffset() {
        return txOffset;
    }

    public void setTxOffset(int txOffset) {
        this.txOffset = txOffset;
    }

}
