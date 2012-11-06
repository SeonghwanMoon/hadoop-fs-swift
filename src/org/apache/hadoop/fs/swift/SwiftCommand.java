package org.apache.hadoop.fs.swift;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SwiftCommand {
	
	public static void exec(String cmd, Path inFile, Path outFile) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		String storageUrl = conf.get("fs.swift.storageUrl");
		if(storageUrl == null) {
				throw new IllegalArgumentException("Url " +
						"pointing to a swift proxy server " +
						"must be specified. "
				);
		}
		SwiftFileSystem swift = new SwiftFileSystem();
		swift.initialize(URI.create(conf.get("fs.swift.storageUrl")), conf);
		
		FSDataInputStream in;
		FSDataOutputStream out;
		
		if(cmd == "mvSwift" || cmd == "cpSwift") {
			in = hdfs.open(inFile);
			out = swift.create(outFile);
		}
		else {
			in = swift.open(inFile);
			out = hdfs.create(outFile);
		}
		
		
		byte[] buffer = new byte[1024];
		int bytesRead;
		
		while ((bytesRead = in.read(buffer)) > 0) {
		  out.write(buffer, 0, bytesRead);
		}
		
		if(cmd == "mvSwift") {
			hdfs.delete(inFile,false);
		} else if (cmd == "mvFs") {
			swift.delete(inFile,false);
		}
		in.close();
		out.close();
		swift.close();
	}
}
