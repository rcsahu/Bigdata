//Author : Ramesh Chandra Sahu <mr.rcsahu@gmail.com>
// This Bigdata program is developed to split a csv file weather.csv in to two ( yes.csv and no.csv ) depending up on tommorow's
// weather condition. the input and output files are accessed from the distributed file system  of hadoop
// the sample weather.csv is taken from the url https://www.kaggle.com/zaraavagyan/weathercsv/download
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

public class Source
{
 public static void main(String[] args) throws IOException 
 {
  Configuration conf = new Configuration();
  String uri = "hdfs://nameservice1";
  FileSystem fs = FileSystem.get(URI.create(uri), conf);
  InputStream in = fs.open(new Path("input/weather.csv"));
  FSDataOutputStream yes = fs.create(new Path("output/yes.csv"));
  FSDataOutputStream no = fs.create(new Path("output/no.csv"));
  Scanner sc = new Scanner(in);
  //first line is header line, read it
  String headerLine = sc.next();
  // write heade line to bothe files
  yes.writeBytes(headerLine+'\n');
  no.writeBytes(headerLine+'\n');
  while ( sc.hasNext() )
  {
   // read a line next from the input file.
   String data = sc.next();
   // split into columns and store to an array
   String[] columns =  data.split(","); 
   // Check the weather status of tommrow and write to appropriate file
   if ( columns[21].compareTo("Yes")==0 ) 
   {
   // add a line feed to each line
    yes.writeBytes(data+'\n'); 
   }
   else
   {
    no.writeBytes(data+'\n');
   } //if statements ends
  
  }//while loop ends
  // close the IO channels
  yes.close();
  no.close();
  in.close();
  sc.close();
 } // main method ends
}//main class ends

