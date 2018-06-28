import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class SpiltData {
	
	
	/**@author Jipon
	 * @throws IOException 
	 * @throws NumberFormatException 
	 * @随机按比例分割数据集
	 */
	
	//Save userID
	static Set<Integer> userids=new TreeSet<>();
	
	//number of row of each user
	static TreeMap<Integer, Integer> idrows=new TreeMap<>();
	
	//for each id：<rownumber，row>
	static HashMap<Integer, TreeMap<Integer, String>> idrowidrows=new HashMap<>();
	
	public static void getdata(String path) throws NumberFormatException, IOException{
		

		FileInputStream inputStream=new FileInputStream(path);
		BufferedReader reader=new BufferedReader(new InputStreamReader(inputStream));
		String line;
		while((line=reader.readLine())!=null){
			String[] str=line.split("	");
			int userid=Integer.parseInt(str[0]);
			userids.add(userid);
			if (!idrows.containsKey(userid)) {
				
				idrows.put(userid,1);
				TreeMap<Integer, String> map=new TreeMap<>();
				map.put(1, line);
				idrowidrows.put(userid, map);
				
			}else {
				int count=idrows.get(userid)+1;
				idrows.put(userid, count);
				
				TreeMap<Integer, String> map=idrowidrows.get(userid);
				map.put(count, line);
				idrowidrows.put(userid, map);
			}
			
		}		
		reader.close();
	}
	
	
	
	public static void splitData(double ratio,String path,String path1) throws IOException {
		
		//for test dataset
		OutputStream outputStream=new FileOutputStream(path);
		BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(outputStream));
		
		//for train dataset
		OutputStream outputStream1=new FileOutputStream(path1);
		BufferedWriter writer1=new BufferedWriter(new OutputStreamWriter(outputStream1));
		
		//for each user
		for (Integer userid : userids) {
							
				//get row number of each user
				int rows=idrows.get(userid);
				//get the ratio
				int testrows=rows-(int) (rows*ratio);
	
				Set<Integer> ir=randomSet(1, rows, testrows, new HashSet<Integer>());
		
				for (Integer rowid : ir) {
					//write test dataset in to files
					String row=idrowidrows.get(userid).get(rowid);
					writer.write(row);
					writer.newLine();
					//delete test, the other is train dataset
					idrowidrows.get(userid).remove(rowid);
					
				}
		}
		//close connection
		writer.close();
		outputStream.close();
		
		//write train dataset in to files
		for (Integer userid : userids) {
			
			for (Map.Entry<Integer, String> useridrows : idrowidrows.get(userid).entrySet()) {
					writer1.write(useridrows.getValue());
					writer1.newLine();
				
				}
			}
		writer1.close();
		outputStream1.close();
	}
	

	
	
   
    public static Set<Integer> randomSet(int min, int max, int n, HashSet<Integer> set) {
        if (n > (max - min + 1) || max < min) {
            return set;
        }
        for (int i = 0; i < n; i++) {
           
            int num = (int) (Math.random() * (max - min)) + min;
            set.add(num);
        }
        int setSize = set.size();
        
        if (setSize < n) {
            randomSet(min, max, n - setSize, set);// 递归
        }
		return set;
    }
    public static void main(String[] args) {
    //the ratio
    	double ratio=0.7;
		String path="after_ratings.data";
		String testpath="test.csv";
		String trainpath="train.csv";
		try {
			System.out.println("===Start get raw dataset===");
			getdata(path);
			System.out.println("===get completed===");
			System.out.println("===Start split dataset===");
			splitData(ratio, testpath, trainpath);
			System.out.println("===split completed=====");
		} catch (NumberFormatException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
}
