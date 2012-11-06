package org.apache.hadoop.fs.swift;

import org.apache.hadoop.fs.Path;

public class CopyToSwift {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		      System.err.println("Usage: CopyToSwift <in> <out>");
		      System.exit(2);
		}
		
		SwiftCommand.exec("cpSwift", new Path(args[0]), new Path(args[1]));
	}
}
