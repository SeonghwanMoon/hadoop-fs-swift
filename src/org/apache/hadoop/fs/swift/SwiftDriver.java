package org.apache.hadoop.fs.swift;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;
/**
 * A description of a command based on its class and a 
 * human-readable description.
 */
public class SwiftDriver {
  
	 Map<String, ProgramDescription> programs;
     
	  public SwiftDriver(){
	    programs = new TreeMap<String, ProgramDescription>();
	  }
	     
	  static private class ProgramDescription {
		
	    static final Class<?>[] paramTypes = new Class<?>[] {String[].class};
		
	    /**
	     * Create a description of a command.
	     * @param mainClass the class with the main for the command
	     * @param description a string to display to the user in help messages
	     * @throws SecurityException if we can't use reflection
	     * @throws NoSuchMethodException if the class doesn't have a main method
	     */
	    public ProgramDescription(Class<?> mainClass, 
	                              String description)
	      throws SecurityException, NoSuchMethodException {
	      this.main = mainClass.getMethod("main", paramTypes);
	      this.description = description;
	    }
		
	    /**
	     * Invoke the example application with the given arguments
	     * @param args the arguments for the application
	     * @throws Throwable The exception thrown by the invoked method
	     */
	    public void invoke(String[] args)
	      throws Throwable {
	      try {
	        main.invoke(null, new Object[]{args});
	      } catch (InvocationTargetException except) {
	        throw except.getCause();
	      }
	    }
		
	    public String getDescription() {
	      return description;
	    }
		
	    private Method main;
	    private String description;
	  }
	    
	  private static void printUsage(Map<String, ProgramDescription> programs) {
	    System.out.println("Valid commands are:");
	    for(Map.Entry<String, ProgramDescription> item : programs.entrySet()) {
	      System.out.println("  " + item.getKey() + ": " +
	                         item.getValue().getDescription());         
	    } 
	  }
	    
	  /**
	   * This is the method that adds the classed to the repository
	   * @param name The name of the string you want the class instance to be called with
	   * @param mainClass The class that you want to add to the repository
	   * @param description The description of the class
	   * @throws NoSuchMethodException 
	   * @throws SecurityException 
	   */
	  public void addClass (String name, Class mainClass, String description) throws Throwable {
	    programs.put(name , new ProgramDescription(mainClass, description));
	  }
	    
	  /**
	   * This is a driver for the commands.
	   * It looks at the first command line argument and tries to find a
	   * command with that name.
	   * If it is found, it calls the main method in that class with the rest 
	   * of the command line arguments.
	   * @param args The argument from the user. args[0] is the command to run.
	   * @throws NoSuchMethodException 
	   * @throws SecurityException 
	   * @throws IllegalAccessException 
	   * @throws IllegalArgumentException 
	   * @throws Throwable Anything thrown by the command's main
	   */
	  public void driver(String[] args) 
	    throws Throwable 
	  {
	    // Make sure they gave us a program name.
	    if (args.length == 0) {
	      System.out.println("A command must be provided as the " + 
	                         " first argument.");
	      printUsage(programs);
	      System.exit(-1);
	    }
		
	    // And that it is good.
	    ProgramDescription pgm = programs.get(args[0]);
	    if (pgm == null) {
	      System.out.println("Unknown program '" + args[0] + "' chosen.");
	      printUsage(programs);
	      System.exit(-1);
	    }
		
	    // Remove the leading argument and call main
	    String[] new_args = new String[args.length - 1];
	    for(int i=1; i < args.length; ++i) {
	      new_args[i-1] = args[i];
	    }
	    pgm.invoke(new_args);
  }
  public static void main(String argv[]){
	    int exitCode = -1;
	    SwiftDriver pgd = new SwiftDriver();
	    try {
		      pgd.addClass("copyToFs", CopyToFs.class, 
		                   "Copies files from Swift to the FileSystem");
		      pgd.addClass("copyToSwift", CopyToSwift.class, 
		              	   "Copies files from the FileSystem to Swift");
		      pgd.addClass("moveToFs", CopyToFs.class, 
		    		  	   "Moves files from Swift to the FileSystem");
		      pgd.addClass("moveToSwift", CopyToSwift.class, 
		         	   	   "Moves files from the FileSystem to Swift");
		      pgd.driver(argv);
		      
		      // Success
		      exitCode = 0;
	    }
	    catch(Throwable e){
	    	e.printStackTrace();
	    }
    
    System.exit(exitCode);
  }
}
	

