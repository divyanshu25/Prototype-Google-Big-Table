package proto;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;

public class Test {

	static HashMap<String, String> hmap=new HashMap<String, String>();

	public static void main(String[] args){
		hmap.put("1","1");
		hmap.put("2","1");
		hmap.put("3","1");
		FileOutputStream fos;
		try {
			fos = new FileOutputStream("output.txt");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(hmap);
			oos.close();
			fos.close();
			
			FileInputStream fin=new FileInputStream("output.txt");
			ObjectInputStream ois=new ObjectInputStream(fin);
			
			hmap=(HashMap<String,String>) ois.readObject();
			
			System.out.println(hmap.size());
			System.out.println(hmap.get("1"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
