package cs245.as3;

import java.nio.ByteBuffer;
import java.util.*;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {

	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;

	private HashMap<Long, MyManager> myManagerMap;
	private LinkedHashMap<Integer, Set<Long>> checkPointQueue;
	private StorageManager sm;
	private LogManager lm;

	public TransactionManager() {
		myManagerMap = new HashMap<>();
		checkPointQueue = new LinkedHashMap<>();
		latestValues = null;
		sm = null;
		lm = null;
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		this.sm = sm;
		this.lm = lm;
		latestValues = sm.readStoredTable();

		int start = lm.getLogTruncationOffset(), end = lm.getLogEndOffset();
		byte nextRecordLength = 0;
		if(start < end) nextRecordLength = lm.readLogRecord(start, 1)[0];
		ByteBuffer byteBuffer;
		byte[] record;

		start++;
		while(start < end && nextRecordLength != 0) {
			byte thisRecordLength = nextRecordLength;
			if(start + thisRecordLength < end) {
				record = lm.readLogRecord(start, thisRecordLength);
				nextRecordLength = record[thisRecordLength - 1];
			}
			else {
				record = lm.readLogRecord(start, thisRecordLength - 1);
				nextRecordLength = 0;
			}

			byteBuffer = ByteBuffer.wrap(record);
			long txID = byteBuffer.getLong();

			if(thisRecordLength == (byte) 10) {
				if(byteBuffer.get() == (byte)-1) {
					start(txID);
					myManagerMap.get(txID).setTxOffset(start - 1);
				}
				else {
					commitDuringRedo(txID);
				}
			}
			else {
				long key = byteBuffer.getLong();
				byte[] value = Arrays.copyOfRange(record, 16, thisRecordLength - 1);
				write(txID, key, value);
			}

			if(nextRecordLength != 0)
				start += thisRecordLength;
		}
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this
		myManagerMap.put(txID, new MyManager(txID));
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read() 
	 * calls until the transaction making the write commits. For simplicity, we will not make reads 
	 * to this same key from txID itself after we make a write to the key. 
	 */
	public void write(long txID, long key, byte[] value) {
		HashMap<Long, byte[]> writeset = myManagerMap.get(txID).getWriteset();
		if (writeset == null) {
			writeset = new HashMap<>();
			myManagerMap.get(txID).setWriteset(writeset);
		}
		writeset.put(key, value);
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		MyManager myManager = myManagerMap.get(txID);
		HashMap<Long, byte[]> writeset = myManager.getWriteset();
		if (writeset != null) {
			// data buffer
			for (Long key: writeset.keySet()) {
				long tag = 0;
				latestValues.put(key, new TaggedValue(tag, writeset.get(key)));
			}

			// log file
			myManager.writeset2Records();
			byte[][] records = myManager.getRecords();

			int txOffset = lm.appendLogRecord((records[0]));
			myManager.setTxOffset(txOffset);
			for (int i = 1; i < records.length; i++) {
				lm.appendLogRecord(records[i]);
			}

			// prepared data file
			for (Long key: writeset.keySet()) {
				sm.queueWrite(key, txOffset, writeset.get(key));
			}

			checkPointQueue.put(myManager.getTxOffset(), myManager.getWriteset().keySet());
		}
		myManagerMap.remove(txID);
	}

	public void commitDuringRedo(long txID) {
		MyManager myManager = myManagerMap.get(txID);
		HashMap<Long, byte[]> writeset = myManager.getWriteset();
		if (writeset != null) {
			checkPointQueue.put(myManager.getTxOffset(), myManager.getWriteset().keySet());
			
			// data buffer
			for (Long key: writeset.keySet()) {
				long tag = 0;
				latestValues.put(key, new TaggedValue(tag, writeset.get(key)));

				sm.queueWrite(key, myManager.getTxOffset(), writeset.get(key));
			}

		}
		myManagerMap.remove(txID);
	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		myManagerMap.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long txOffset, byte[] persisted_value) {
		Set<Long> set = checkPointQueue.get((int) txOffset);
		set.remove(key);
		if(set.size() == 0) {
			checkPointQueue.remove((int)txOffset);
		}

		if(checkPointQueue.size() != 0) {
			Map.Entry<Integer, Set<Long>> next = checkPointQueue.entrySet().iterator().next();
			lm.setLogTruncationOffset(next.getKey());
		}
		else {
			lm.setLogTruncationOffset(lm.getLogEndOffset());
		}
	}
}
