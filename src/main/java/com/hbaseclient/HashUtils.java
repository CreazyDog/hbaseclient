package com.hbaseclient;

import org.apache.hadoop.hbase.util.Bytes;

import java.text.ParseException;
import java.text.SimpleDateFormat;


/**
 * 
 * @author yangming5
 *
 */
public class HashUtils {

	public static long murmurHash2(String text) {
		return MurmurHash2.hash64(text);
	}

	public static long cppHash64(String text) {
		final byte[] bytes = text.getBytes(); 
		return MurmurHash2.cHash64( bytes, bytes.length);
	}

	public static String getRowKey(String raw) {
		return getRowKey(raw, 3);
	}

	public static String getRowKey(String raw, int hexnum) {
		
		// trim ' " ' 
		raw = raw.replace("\"", "");
		long lraw = Long.parseLong(raw);

		String h16full = "0000";

		int i32 = MurmurHash2.hash32(raw);
		String hex32 =  StringConvertor.bytesToHexString(Bytes.toBytes(i32));
		String h16 = hex32.substring(0, hexnum) + h16full.substring(hexnum);		
		String l64 = StringConvertor.bytesToHexString(Bytes.toBytes(lraw));
		String hex80 = (h16 + l64).toLowerCase();
		return hex80;
	}
	
	public static String getStringRowKey(String str, int hexnum) {

		String h16full = "0000";

		long i64 = MurmurHash2.hash64(str);
		String hex64 =  StringConvertor.bytesToHexString(Bytes.toBytes(i64));
		String h16 = hex64.substring(0, hexnum) + h16full.substring(hexnum);		
		String l64 = StringConvertor.bytesToHexString(Bytes.toBytes(i64));
		String hex80 = (h16 + l64).toLowerCase();

		return hex80;
	}
	
	public static void main(String[] args) {
/*		System.out.println(HashUtils.getRowKey("3904232090905204", 3));
		System.out.println(((Long.parseLong("3905131144100743") >> 22) & ((1L << 30) -1)) + 515483463);
		System.out.println(HashUtils.getStringRowKey("����", 2));
		System.out.println(HashUtils.getStringRowKey("������������һ�Ժ�����,", 2));
		
		System.out.println(MurmurHash2.hash64("������������һ�Ժ�����"));
		System.out.println(MurmurHash2.hash64("37682129994531"));*/
		String rowKey = HashUtils.getRowKey("109082338759", 3);
		System.out.printf(rowKey);

	}

	public static String getRowKeyForUnnormal(String raw, String time) {
		
		if (time == null) {
			return null;
		}
		// trim ' " ' 
		raw = raw.replace("\"", "");

		String newMid = getNewMidFromUnnormal(raw, time);		
		String hex80 = getRowKey(newMid);

		// get high 16 bit on (low 38 bit)
		long l64 = MurmurHash2.hash64(raw);
		l64 = l64 << 26;
		l64 = l64 >>> 48;
		
		String hex32 =  StringConvertor.bytesToHexString(Bytes.toBytes((int)l64));
		String hex16 = hex32.substring(4).toLowerCase();

		String hex96 = hex80 + hex16;
		return hex96;
	}

	public static String getNewMidFromUnnormal(String umid, String time) {

		long b =  getMidTimeLong(time, 22);
		if (b <= 0) {
			return null;
		}
		// get high 22 bit on (low 38 bit)
		long l64 = MurmurHash2.hash64(umid);
		l64 = l64 << 42;
		l64 = l64 >>> 42;
		b = b + l64;
		
		return "" + b;
	}

	public static long getMidTimeLong(String time, int informat) {
		long ltime = getDateTime(time, findFormat(informat).toPattern());
		// exchange time from millisecond to second
		ltime = ltime / 1000;
		long b = ltime - Global.MID_TIME_REVISION;
		b = b << 22;
		return b;
	}
	
	public static long getDateTime(String date, String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		try {
			return sdf.parse(date).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	public static SimpleDateFormat findFormat(int intFormat) {
		String strFormat = "yyyy'??'MM'??'dd'??' H:mm:ss.S";
		switch (intFormat) {
		case 0: // '\0'
			strFormat = "yyyy'??'MM'??'dd'??' H:mm:ss.S";
			break;

		case 1: // '\001'
			strFormat = "yyyy'-'MM'-'dd H:mm:ss.S";
			break;

		case 2: // '\002'
			strFormat = "yyyy'??'MM'??'dd'??'";
			break;

		case 3: // '\003'
			strFormat = "yyyy'-'MM'-'dd";
			break;

		case 4: // '\004'
			strFormat = "H:mm:ss";
			break;

		case 5: // '\005'
			strFormat = "K:mm:ss a";
			break;

		case 6: // '\006'
			strFormat = "yyyy'??'MM'??'dd'??' H:mm:ss";
			break;

		case 7: // '\007'
			strFormat = "yyyy'??'MM'??'dd'??' K:mm:ss a";
			break;

		case 8: // '\b'
			strFormat = "yyyy-MM-dd H:mm:ss";
			break;

		case 9: // '\t'
			strFormat = "yyyy-MM-dd K:mm:ss a";
			break;

		case 10: // '\n'
			strFormat = "H:mm:ss.S";
			break;

		case 11: // '\013'
			strFormat = "K:mm:ss.S a";
			break;

		case 12: // '\f'
			strFormat = "H:mm";
			break;

		case 13: // '\r'
			strFormat = "K:mm a";
			break;

		case 14: // '\r'
			strFormat = "yyyy-MM-dd H:mm";
			break;

		case 15: // '\r'
			strFormat = "yyyyMMddHHmmssS";
			break;
		case 16: // '\r'
			strFormat = "yyyyMMdd";
			break;

		case 17: // '\r'
			strFormat = "yyyy";
			break;
			
		case 18: // '\r'
			strFormat = "yyyy-MM";
			break;
			
		case 19: // '\r'
			strFormat = "yyyy-MM-dd";
			break;
			
		case 20: // '\r'
			strFormat = "yyyy-MM-dd HH";
			break;
			
		case 21: // '\r'
			strFormat = "yyyy-MM-dd HH:mm";
			break;
			
		case 22: // '\r'
			strFormat = "yyyy-MM-dd HH:mm:ss";
			break;

		default:
			strFormat = "yyyy'??'MM'??'dd'??' H:mm:ss.S";
			break;
		}
		return new SimpleDateFormat(strFormat);
	}
}
