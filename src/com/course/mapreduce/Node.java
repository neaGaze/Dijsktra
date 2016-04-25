package com.course.mapreduce;
import java.util.*;


public class Node {

public static enum Color {WHITE, GRAY, BLACK};
  
  private int id;
  private int parent = Integer.MAX_VALUE;
  private int distance = Integer.MAX_VALUE;
  private List<Integer> edges = null;
  private List<Integer> weights = null;
  private Color color = Color.WHITE;
  
  public Node(int id) {
    this.id = id;
  }
  
  public Node(String str) {
	  edges = new ArrayList<Integer>();
	  weights = new ArrayList<Integer>();
	  
	  String[] sepStrings = str.split("\t"); //sepStrings format: 1 2,3|10,5|0|GRAY
	  
	  // for id param
	  id = Integer.parseInt(sepStrings[0]);
	  
	  String valueString = sepStrings[1]; // valueString format: 2,3|10,5|0|GRAY
	  
	  // for edges param
	  String[] params = valueString.split("_");  // param[0] format: 2,3
	  											 // param[1] format: 10,5
	  											 // param[2] format: 0
		 										 // param[3] format: GRAY
	  
	  if(!params[0].isEmpty()) {
		  String[] toEdges = params[0].split(",");	// toEdges[0] format: 2 and toEdges[1] format: 3
	  	  for(String aString : toEdges) {			
	  		  edges.add(Integer.parseInt(aString));
	  	  }
	  }
	  
	  // for weights param
	  if(!params[1].isEmpty()) {
		  	String[] toEdges1 = params[1].split(","); // toEdges1[0] format: 10 and toEdges1[1] format: 5	  
	  		for(String aString : toEdges1) {
	  			weights.add(Integer.parseInt(aString));
	  		}
	  }
	  
	  // for distance param
	  distance = Integer.parseInt(params[2]);

	  // for color param
	  if(params[3].equals("WHITE"))
		  color = Color.WHITE;
	  else if(params[3].equals("GRAY"))
		  color = Color.GRAY;
	  else if(params[3].equals("BLACK"))
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
  
  public List<Integer> getWeights(){
    return this.weights;
  }
  
  public void setWeights(List<Integer> vertices) {
    this.weights = vertices;
  }
  
  public String getLine() {
	  	return listToString(edges) + "_" + listToString(weights) + "_" + distance + "_" + colorToString(color);
  }
  
  public String colorToString(Color color) {
	  if(color == Color.WHITE)
		  return "WHITE";
	  
	  if(color == Color.GRAY)
		  return "GRAY";
	  
	  if(color == Color.BLACK)
		  return "BLACK";
	  return "";
  }
  
  public String listToString(List<Integer> values) {
	  StringBuilder edgesString = new StringBuilder();
	  
	  if(values == null)
		  return "";
	  
	  for(int edge : values) {
		  edgesString.append(edge);
		  edgesString.append(",");
	  }
	  String str = edgesString.toString();
	  
	  if (str != null && str.length() > 0 && str.charAt(str.length()-1)==',') {
	      str = str.substring(0, str.length()-1);
	    }
	  return str;
  }
  
}