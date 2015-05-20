package proto;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;

public class TabletServer extends Thread
{
	private static int PORT = 2400;
	public static HashMap< String,HashMap<String, String> > table=new HashMap<String, HashMap<String,String>>();
	public static int noOfTablets=0;
	public static HashMap<String, ArrayList<Integer> > tabletNumbers= new HashMap<String, ArrayList<Integer>>();
	public static HashMap<String,Pair> rangeMapping=new HashMap<String, Pair>();
	
	public static void main(String args[]) {
		ServerSocket s;
		try {
			s = new ServerSocket(PORT);
			while(true) 
				new TabletServerThread(s.accept());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}