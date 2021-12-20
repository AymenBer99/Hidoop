/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import java.io.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.OpenMode;
import formats.Format.Type;

public class HdfsClient {

	public static int nameNode;
	public static String defaultPath = "./config/HdfsClient.xml";
	public static String machine;

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
	
    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void HdfsDelete(String hdfsFname) {
    	try {
    		chargerParametresConfig(defaultPath);
			NameNode nNode = (NameNode) Naming.lookup("//"+machine+":"+nameNode+"/NameNode");
			Map<String,Integer> dataNodes = nNode.getDataNodes();
			int partie = 0;
    		for(String i:dataNodes.keySet()) {
        		Socket con = new Socket(i,dataNodes.get(i));
        		OutputStream os1 = con.getOutputStream();
        		ObjectOutputStream oos1 = new ObjectOutputStream(os1);
        		InputStream is1 = con.getInputStream();
        		ObjectInputStream ois1 = new ObjectInputStream(is1);
        		oos1.writeObject("DELETE");
        		String[] path = hdfsFname.split("/");
        		oos1.writeObject("partie"+partie+"_"+path[path.length-1]);
        		partie+=1;
    		}		
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor) {
    	ArrayList<KV> lignes = new ArrayList<KV>();
    	try {
		      File file = new File(localFSSourceFname); 
		      // Initialiser le compteur à zéro
		      int nbrLine = 0;            
		      // Créer l'objet File Reader
		      FileReader fr = new FileReader(file);
		      // Créer l'objet BufferedReader 
		      BufferedReader br = new BufferedReader(fr);  
		      String str;
		      // Lire le contenu du fichier
		      while((str = br.readLine()) != null)
		      {
			 //Pour chaque ligne, incrémentez le nombre de lignes
			 nbrLine++;               
			    
		      }
		      fr.close();
			System.out.println(nbrLine);
    		int positionDansLignes = 0;
    		chargerParametresConfig(defaultPath);
			NameNode nNode = (NameNode) Naming.lookup("//"+machine+":"+nameNode+"/NameNode");
			Map<String,Integer> dataNodes = nNode.getDataNodes();
			int nbrServeurs = dataNodes.size();
    		Format fichier = null;
    		if (fmt == Type.LINE) {
        		fichier = new LineFormat(localFSSourceFname);
    		} else if (fmt == Type.KV) {
        		fichier = new KVFormat(localFSSourceFname);
    		} else {
				throw new RuntimeException("Erreur type de fichier");
    		}
			//KV save;
			fichier.open(OpenMode.R);
			//while ((save = fichier.read()) != null) {
				//lignes.add(new KV(save.k,save.v));
			//}	      // Le fichier d'entrée
            int nbLignesFrag = nbrLine / nbrServeurs;
            int nbLignesRest = nbrLine % nbrServeurs;
            int positionDansMapDataNode = 1;
            Iterator<String> keysDataNodes = dataNodes.keySet().iterator();
    		while(keysDataNodes.hasNext()) {
    			String key = keysDataNodes.next();
        		Socket con = new Socket(key,dataNodes.get(key));
        		OutputStream os1 = con.getOutputStream();
        		ObjectOutputStream oos1 = new ObjectOutputStream(os1);
        		oos1.writeObject("WRITE");
        		oos1.writeObject(fmt);
        		String[] path = localFSSourceFname.split("/");
        		oos1.writeObject("/tmp/partie"+(positionDansMapDataNode-1)+"_"+path[path.length-1]);
        		int ajout = 0;
        		if (positionDansMapDataNode <= nbLignesRest) {ajout = 1;};
				KV save;
        		for(int i=positionDansLignes;i<positionDansLignes+nbLignesFrag+ajout;i++) {
					save = fichier.read();
        			oos1.writeObject(new KV(save.k,save.v));
        		}
        		positionDansLignes+=(nbLignesFrag+ajout);
        		oos1.writeObject(null);
        		nNode.ajoutFichier(path[path.length-1],fmt);
        		positionDansMapDataNode+=1;
        		
        		//os1.close();
        		//oos1.close();
        		//con.close();
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }

    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
    	ArrayList<KV> files = new ArrayList<KV>();
    	try {
    		chargerParametresConfig(defaultPath);
			NameNode nNode = (NameNode) Naming.lookup("//"+machine+":"+nameNode+"/NameNode");
    		Type typeFichier = nNode.getType(hdfsFname); 
			Map<String,Integer> dataNodes = nNode.getDataNodes();
            int positionDansMapDataNode = 1;
            Iterator<String> keysDataNodes = dataNodes.keySet().iterator();
    		while(keysDataNodes.hasNext()) {
    			String key = keysDataNodes.next();
        		Socket con = new Socket(key,dataNodes.get(key));
        		OutputStream os1 = con.getOutputStream();
        		ObjectOutputStream oos1 = new ObjectOutputStream(os1);
        		InputStream is1 = con.getInputStream();
        		ObjectInputStream ois1 = new ObjectInputStream(is1);
        		oos1.writeObject("READ");
        		oos1.writeObject(typeFichier);
        		String[] path = hdfsFname.split("/");
        		oos1.writeObject("/tmp/partie"+(positionDansMapDataNode-1)+"_"+path[path.length-1]);
        		Object messageSent;   		
        		while ((messageSent = ois1.readObject()) != null) {
        			files.add((KV) messageSent);
        		}
        		positionDansMapDataNode+=1;
        	}
			Format createdFile = null;
			String nameFile = null;
    		if (localFSDestFname != null) {
    			nameFile = localFSDestFname;
    		} else {
    			nameFile = hdfsFname;

    		}
    		switch(typeFichier) {
    			case KV :
    				createdFile = new KVFormat(nameFile);
    				break;
    			case LINE :
    				createdFile = new LineFormat(nameFile);
    				break;
    			default :
    				throw new RuntimeException ("Erreur format fichier");
    		}
			File create = new File(nameFile);
			create.createNewFile();
			createdFile.open(OpenMode.W);
    		for(int i=0;i<files.size();i++) {
				createdFile.write(files.get(i));
    		}
    		createdFile.close();
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }

	
    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": HdfsRead(args[1],null); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
