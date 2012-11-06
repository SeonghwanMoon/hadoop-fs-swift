package org.apache.hadoop.fs.swift;

import org.apache.hadoop.fs.Path;

public class MoveToFs {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		      System.err.println("Usage: MoveToFs <in> <out>");
		      System.exit(2);
		}
		SwiftCommand.exec("mvFs", new Path(args[0]), new Path(args[1]));
	}
}
