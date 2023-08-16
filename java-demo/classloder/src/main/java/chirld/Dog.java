package chirld;

import lombok.Getter;
import lombok.Setter;

public class Dog {

	public static String name="";
	public String say(String name) {
		System.out.println("Hello " + name);
		return "Hello " + name;
	}


}