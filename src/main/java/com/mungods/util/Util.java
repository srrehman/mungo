package com.mungods.util;

public class Util {
	public static String toHex(byte[] bytes){
		StringBuffer result = new StringBuffer();
		for (byte b : bytes) {
		    result.append(String.format("%02X ", b));
		    result.append(" "); // delimiter
		}
		return result.toString();
	}
}
