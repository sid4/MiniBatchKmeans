package edu.iu.km;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternMatcherTester {
public static void main(String args[]){
	Pattern p=Pattern.compile("^(?<category>(?<baseCategory>\\w{1})(\\w+))\\s(?<docid>\\d+)\\s1$");
	Matcher m=p.matcher("hello 1234 1");
	
	System.out.println(m.find());
	System.out.println(m.group("category"));
	System.out.println(m.group(2));
	
}
}
