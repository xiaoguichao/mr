package com.alibaba.middleware.race.utils;

import com.alibaba.middleware.race.db.BuyerLoader;
import com.alibaba.middleware.race.db.GoodsLoader;

public class IndexUtil {
    private static volatile long NONE = 0x0L;

    public static void initNone(){
        long i = NONE;
        while(true){
            if(GoodsLoader.INSTANCE.indexOf(i)<0 && BuyerLoader.INSTANCE.indexOf(i)<0){
                NONE = i;
                break;
            }
            i++;
        }
    }

    public static long StringIndexToLong(String value){
        if(value==null)return NONE;
        int length = value.length();
        if(length<20)return NONE;
        long r = 0;
        switch (value.substring(0, length - 18)){
            case "ap":
            case "aye":
                r = 0;
                break;
            case "al":
            case "tp":
                r = 1;
                break;
            case "wx":
            case "gd":
                r = 2;
                break;
            case"dd":
                r = 3;
                break;
            default:
                return NONE;
        }
        if(length>21 || (length==21&&r!=0))return NONE;
        r<<=62;
        r |= ((charToNumber(value.charAt(length-17))-8)<<60)
                |   (charToNumber(value.charAt(length-16))<<56)
                |   (charToNumber(value.charAt(length-15))<<52)
                |   (charToNumber(value.charAt(length-14))<<48)
                |   (charToNumber(value.charAt(length-12))<<44)
                |   (charToNumber(value.charAt(length-11))<<40)
                |   (charToNumber(value.charAt(length-10))<<36)
                |   (charToNumber(value.charAt(length-9))<<32)
                |   (charToNumber(value.charAt(length-8))<<28)
                |   (charToNumber(value.charAt(length-7))<<24)
                |   (charToNumber(value.charAt(length-6))<<20)
                |   (charToNumber(value.charAt(length-5))<<16)
                |   (charToNumber(value.charAt(length-4))<<12)
                |   (charToNumber(value.charAt(length-3))<<8)
                |   (charToNumber(value.charAt(length-2))<<4)
                |   (charToNumber(value.charAt(length-1)));
        return r;
    }

    private static long charToNumber(char ch){
        switch (ch){
            case '0': return 0x0;
            case '1': return 0x1;
            case '2': return 0x2;
            case '3': return 0x3;
            case '4': return 0x4;
            case '5': return 0x5;
            case '6': return 0x6;
            case '7': return 0x7;
            case '8': return 0x8;
            case '9': return 0x9;
            case 'a': return 0xa;
            case 'b': return 0xb;
            case 'c': return 0xc;
            case 'd': return 0xd;
            case 'e': return 0xe;
            default: return 0xf;
        }
    }

    public static long StringIndexToLong(byte[] bytes, int start, int length){
        long r = 0;
        switch (bytes[start]){
            case 'a': {
                switch (bytes[start+1]){
                    case 'p':
                    case 'y':
                        r = 0;
                        break;
                    case 'l':
                        r = 1;
                }
                break;
            }
            case 't': {
                r = 1;
                break;
            }
            case 'w': {
                r = 2;
                break;
            }
            case 'g': {
                r = 2;
                break;
            }
            case 'd':{
                r = 3;
            }
        }
        r<<=62;
        r |=       ((byteToNumber(bytes[start+length-17])-8)<<60)
                |   (byteToNumber(bytes[start+length-16])<<56)
                |   (byteToNumber(bytes[start+length-15])<<52)
                |   (byteToNumber(bytes[start+length-14])<<48)
                |   (byteToNumber(bytes[start+length-12])<<44)
                |   (byteToNumber(bytes[start+length-11])<<40)
                |   (byteToNumber(bytes[start+length-10])<<36)
                |   (byteToNumber(bytes[start+length-9])<<32)
                |   (byteToNumber(bytes[start+length-8])<<28)
                |   (byteToNumber(bytes[start+length-7])<<24)
                |   (byteToNumber(bytes[start+length-6])<<20)
                |   (byteToNumber(bytes[start+length-5])<<16)
                |   (byteToNumber(bytes[start+length-4])<<12)
                |   (byteToNumber(bytes[start+length-3])<<8)
                |   (byteToNumber(bytes[start+length-2])<<4)
                |   (byteToNumber(bytes[start+length-1]));
        return r;
    }

    private static long byteToNumber(byte b){
        switch (b){
            case 48: return 0x0;
            case 49: return 0x1;
            case 50: return 0x2;
            case 51: return 0x3;
            case 52: return 0x4;
            case 53: return 0x5;
            case 54: return 0x6;
            case 55: return 0x7;
            case 56: return 0x8;
            case 57: return 0x9;
            case 97: return 0xa;
            case 98: return 0xb;
            case 99: return 0xc;
            case 100: return 0xd;
            case 101: return 0xe;
            default: return 0xf;
        }
    }

    public static long LongIndexToLong(byte[] bytes, int start, int length){
        long result = 0;
        boolean negative = false;
        int i = start;
        long limit = -Long.MAX_VALUE;
        long multmin;
        int digit;
        if (length > 0) {
            if (bytes[i] < 48) {
                if (bytes[i] == 45) {
                    negative = true;
                    limit = Long.MIN_VALUE;
                } else if (bytes[i] != 43)
                    return 0;

                if (length == 1)return 0;
                i++;
            }
            multmin = limit / 10;
            while (i < (start+length)) {
                digit = bytes[i++] - 48;
                if (digit < 0) {
                    return 0;
                }
                if (result < multmin) {
                    return 0;
                }
                result *= 10;
                if (result < limit + digit) {
                    return 0;
                }
                result -= digit;
            }
        } else {
            return 0;
        }
        return negative ? result : -result;
    }
}