package com.course.mapreduce;
import java.util.*;


public class Node {

public static enum Color {WHITE, GRAY, BLACK};
  
  private int id;
  private int parent = Integer.MAX_VALUE;
  private int distance = Integer.MAX_VALUE;
  private List<Integer> edges = null;
  private Color color = Color.WHITE;
  
  public Node(int id) {
    this.id = id;
  }
  
  public Node(String str) {
	  String[] sepStrings = str.split("\t");
	  id = Integer.parseInt(sepStrings[0]);
	  String valueString = sepStrings[1];
	  
	  String[] params = valueString.split("|"); 
	  String[] toEdges = params[0].split(",");
	  edges = new ArrayList<Integer>();
	  for(String aString : toEdges) {
		  edges.add(Integer.parseInt(aString));
	  }
	  distance = Integer.parseInt(params[1]);
	  if(params[2].equals("WHITE"))
		  color = Color.WHITE;
	  else if(params[2].equals("GRAY"))
		  color = Color.GRAY;
	  else if(params[2].equals("BLACK"))
		  color = Color.BLACK;
  }
  
  
  public int getId(){
    return this.id;
  }
  
  public int getParent() {
    return this.parent;
  }
  
  public void setParent(int parent) {
    this.parent = parent;
  }
  
  public int getDistance(){
    return this.distance;
  }
  
  public void setDistance(int distance) {
    this.distance = distance;
  }
  
  public Color getColor(){
    return this.color;
  }
  
  public void setColor(Color color){
    this.color = color;
  }
  
  public List<Integer> getEdges(){
    return this.edges;
  }
  
  public void setEdges(List<Integer> vertices) {
    this.edges = vertices;
  }
  
  public String getLine() {
	  	return "";
  }
  
}