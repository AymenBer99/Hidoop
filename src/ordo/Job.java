package ordo;


import java.io.File;
import java.io.FileInputStream;
import java.rmi.Naming;
import java.util.Map;
import java.util.Properties;

import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KVFormatS;
import formats.LineFormatS;
import hdfs.HdfsClient;
import hdfs.NameNode;
import map.MapReduce;

public class Job implements JobInterface {

	private Type InputFormat;
	private String InputFname;
	private static int nameNode;
	private static String machine;
	public static String defaultPath = "./config/nameNode.xml";

	public Job() {
		
	}
	
	public Job(Type t, String name) {
		this.InputFormat = t;
		this.InputFname = name;
	}
	
	@Override
	public void setInputFormat(Type ft) {
		this.InputFormat = ft;
		
	}

	@Override
	public void setInputFname(String fname) {
		this.InputFname = fname;
	}

	@Override
	public void startJob(MapReduce mr) {
		Thread launchWork = new work(mr,this.InputFname,this.InputFormat);
		launchWork.start();
		try {
			launchWork.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
    public static void chargerParametresConfig(String path) {
    	try {
    		Properties p = new Properties();
    		p.loadFromXML(new FileInputStream(path));
    		nameNode = Integer.parseInt(p.getProperty("NameNodePort"));
    		machine = p.getProperty("NameNodeName");
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
	public class work extends Thread {
		
		MapReduce mr;
		String file;
		Type format;
		
		public work(MapReduce mprd, String name,Type InFormat) {
			this.mr = mprd;
			this.file = name;
			this.format = InFormat;
		}
		public void run(){
			try {
	    		chargerParametresConfig(defaultPath);
				Format reader = null;
				Format writer = null;
				CallBack cb = new CallBackImpl();
				NameNode nNode = (NameNode) Naming.lookup("//"+machine+":"+nameNode+"/NameNode");
				Map<String,Integer> workers = nNode.getWorkers();
				int position = 0;
				for (String i:workers.keySet()) {
					String URL = "//"+i+":"+workers.get(i)+"/worker";
					Worker w = (Worker) Naming.lookup(URL);
					switch (this.format) {
						case KV:
							reader = new KVFormatS("/tmp/partie"+position+"_"+this.file);
							File create = new File("/tmp/partie"+position+"_map_"+this.file);
							create.createNewFile();
							writer = new KVFormatS("/tmp/partie"+position+"_map_"+this.file);
							break;
						case LINE:
							reader = new LineFormatS("/tmp/partie"+position+"_"+this.file);
							File createL = new File("/tmp/partie"+position+"_map_"+this.file);
							createL.createNewFile();
							writer = new KVFormatS("/tmp/partie"+position+"_map_"+this.file);
							break;
					}
					w.runMap(mr, reader, writer, cb);
					position+=1;
	
				}
				cb.waitForRunMap(workers.size());
				HdfsClient.HdfsRead("map_"+this.file, "ResultatMap_"+this.file);
				reader = new KVFormatS("ResultatMap_"+this.file);
				reader.open(OpenMode.R);
				File createL = new File(this.file+"-test");
				createL.createNewFile();
				writer = new KVFormatS(this.file+"-test");
				writer.open(OpenMode.W);
				mr.reduce(reader, writer);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
