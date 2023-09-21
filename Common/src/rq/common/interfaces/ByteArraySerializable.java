/**
 * 
 */
package rq.common.interfaces;

/**
 * @author Mgr. Radomir Skrabal
 *
 */
public interface ByteArraySerializable {

	/**
	 * Serializes this object to bytes
	 * @return new byte array
	 */
	public byte[] toBytes();
	
	/**
	 * Initializes value from bytes
	 * @param bytes 
	 */
	public void fromBytes(byte[] bytes);
	
	/**
	 * Returns number of bytes the serialization takes
	 * @return
	 */
	public int byteArraySize();
	
}
