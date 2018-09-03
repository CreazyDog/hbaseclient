package com.hbaseclient;

import java.io.UnsupportedEncodingException;

public class StringConvertor {
    
	public static final String CHARSET = "UTF-8";
	
	public static byte[] hexStringToBytes(String hex) {
		
	    byte[] bts = new byte[hex.length() / 2];
	    for (int i = 0; i < bts.length; i++) {
	      bts[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
	    }
	    return bts;
		
//		int length = hex.length() / 2;
//		byte[] result = new byte[length];
//		char[] achar = hex.toCharArray();
//		for (int i = 0; i < length; i++) {
//			int pos = i * 2;
//			result[i] = (byte)(toByte(achar[pos]) << 4 | toByte(achar[pos + 1]));
//		}
//		return result;
	}
	
	public static byte toByte(char c) {
		byte b = (byte)"0123456789ABCDEF".indexOf(c);
		return b;
	}

	public static final String bytesToHexString(byte[] bArray) {
		StringBuffer sb = new StringBuffer(bArray.length);
		String temp;
		for (int i = 0; i < bArray.length; i++) {
			temp = Integer.toHexString(0xFF & bArray[i]);
			if (temp.length() < 2) {
				sb.append(0);
			}
			sb.append(temp.toUpperCase());
		}
        return sb.toString();
	}
    
	public static String strToHexStr(String str) {
		if (str == null || str.length() == 0) {
			return "";
		}
		try {
			return bytesToHexString(str.getBytes(CHARSET));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
            return "";
		}
	}
    
	public static String hexStrToStr(String str) {
		if (str == null || str.length() == 0) {
			return "";
		}
		byte[] bytes = hexStringToBytes(str);
		try {
			return new String(bytes, CHARSET);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
            return "";
		}
	}
	
	public static void main(String[] args) {
//        String oriStr = "曹雪芹 . {}";
//        System.out.println("OriStr:" + oriStr);
//        String encodeStr = strToHexStr(oriStr);
//		System.out.println("encode:" + encodeStr);
		String encodeStr = "E59889E6B2BBE99A86E4B880E891972E";
        String decodeStr = hexStrToStr(encodeStr);
		System.out.println("decode:" + decodeStr);
	}
}
