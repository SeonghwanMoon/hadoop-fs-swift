package org.apache.hadoop.fs.swift;

import org.apache.hadoop.fs.Path;

public class CopyToFs {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		      System.err.println("Usage: CopyToFs <in> <out>");
		      System.exit(2);
		}
		
		SwiftCommand.exec("cpFs", new Path(args[0]), new Path(args[1]));
	}
}
