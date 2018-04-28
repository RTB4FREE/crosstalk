package com.jacamars.dsp.crosstalk.manager;

public class Normalize {
	
	public static String os(String what) {
		if (what.equalsIgnoreCase("ANDROID"))
			return "Android";
		if (what.equalsIgnoreCase("IPHONE"))
			return "iOS";
		if (what.equalsIgnoreCase("APPLE"))
			return "iOS";
		if (what.equalsIgnoreCase("WINDOWS"))
			return "Windows";
		if (what.equalsIgnoreCase("WEBOS"))
			return "WebOS";
		if (what.equalsIgnoreCase("BADA"))
			return "Bada";
		if (what.equalsIgnoreCase("Linux"))
			return "Linux";
		if (what.equalsIgnoreCase("REX"))
			return "REX";
		if (what.equalsIgnoreCase("Symbian"))
			return "Symbian";
		if (what.equalsIgnoreCase("RIM"))
			return "Rim";
		if (what.equalsIgnoreCase("NOKIA"))
			return "Nokia";
		if (what.equalsIgnoreCase("BREW"))
			return "BREW";
		return what;
	}
}
