/**
 * 
 */
package me.boyce.elasticsearch.dd.hash;

/**
 * @author boyce
 * 
 */
public class SafeEncoder {
	public static byte[][] encodeMany(final String... strs) throws Exception {
		byte[][] many = new byte[strs.length][];
		for (int i = 0; i < strs.length; i++) {
			many[i] = encode(strs[i]);
		}
		return many;
	}

	public static byte[] encode(final String str) throws Exception {
		return str.getBytes("UTF-8");
	}

	public static String encode(final byte[] data) throws Exception {
		return new String(data, "UTF-8");
	}
}
