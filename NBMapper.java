import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.lang.Math.*;


public class NBMapper extends MapReduceBase implements 
	Mapper<LongWritable, Text, Text, DoubleWritable> {

	
				 static int cneg,cpos;
				 static double cntpos,cntneg;
				static int nb,lb;
				int fg=0;
				
  @Override
  public void map(LongWritable key, Text value,OutputCollector<Text, DoubleWritable> output, Reporter reporter) 
	throws IOException { 
                                cpos=cneg=0;//lex variable
				cntpos=cntneg=1;//naive 
				int len;
				String s = value.toString().toLowerCase();
				if(s.length()>0){
				len=s.length();
				len=len-1;
				/*while(s.charAt(len-1)==' '){
					len--;
				}*/
				if(s.charAt(len)=='?'){
					output.collect(new Text(s), new DoubleWritable(0));
				}
				else if(s.contains("but")){
					//output.collect(new Text(s), new DoubleWritable(3));
					String[] tempbut = s.split("but");
					int resbut1=process(tempbut[0]);
					//output.collect(new Text(tempbut[0]), new DoubleWritable(resbut1));
					int resbut2=process(tempbut[1]);
					//output.collect(new Text(tempbut[1]), new DoubleWritable(resbut2));
					int fresult=resbut1+resbut2;
					if(fresult==2)
						fresult=1;
					if(fresult==-2)
						fresult=-1;
					output.collect(new Text(s), new DoubleWritable(fresult));

				}
				else if(s.contains("not")){
					//output.collect(new Text(s), new DoubleWritable(3));
					String[] tempnot = s.split("not");
					int resnot1=process(tempnot[0]);
					//output.collect(new Text(tempnot[0]), new DoubleWritable(resnot1));
					int resnot2=process(tempnot[1]);
					//output.collect(new Text(tempnot[1]), new DoubleWritable(resnot2));
					int fresult=resnot1+(-1*resnot2);
					if(fresult==2)
						fresult=1;
					if(fresult==-2)
						fresult=-1;
					
					output.collect(new Text(s), new DoubleWritable(fresult));
					
					
					/*for (String word : tempnot[1].split("\\W+")){
						if (word.length() > 0){
							
      							cpos=cpos+poslexnot(word);
      			 				cneg=cneg+neglexnot(word);
							
							
      						}					
					}

					
					if(cpos>cneg){
						lb=1;
					}
					
					else if(cpos<cneg){
						lb=-1;
					}
					
					output.collect(new Text(s), new DoubleWritable(resnot1+lb));
				*/

				}
				
				else{
					int result=process(s);
					
					output.collect(new Text(s), new DoubleWritable(result));
				}
}
}	


public int process(String s){

	for (String word : s.split("\\W+")){
					if (word.length() > 0){
      						cpos=cpos+poslex(word);
      			 			cneg=cneg+neglex(word);
      					}					
				}
				
				/*lexicon neutral
				if(cpos==cneg){
					
				}*/
				//lexicon pos
				if((cpos!=0)||(cneg!=0)){
					if(cpos>cneg){
						lb=1;
					}
					//lexicon neg 
					else if(cpos<cneg){
						lb=-1;
					}
					if(cntpos>cntneg){
						nb=1;
					}
					else if(cntpos<cntneg){
						nb=-1;
					}
					if(nb==lb)
					{
							
							return nb;
					}
					else
						
						return 0;
				}
				else{
					double sp,sn;
    					sp=sn=0.5;
    					//String s = value.toString().toLowerCase();
   					for (String word : s.split("\\W+")) {
      						//Naive Bayes Classifier for analysis of neutral
						if (word.length() > 0) {
      						sp=sp+pos(word);
      			 			sn=sn+neg(word);
      						}
   			 		}

					if(sp>sn)
						return(-1);
					else
						return(1);
				}

}

public int poslex(String word){
	String strRead;
	int cp=0;
	
 	try {
 		//read file from Distributed CachE
 	        BufferedReader reader = new BufferedReader(new FileReader("lexpositive"));
		try{
			while ((strRead=reader.readLine()) != null){     
  				String[] word1=strRead.split("\\W+"); 
                                if (word.equals(word1[0])){
                            		cp++;
                            		break;   
                            	}
                	}
			if(cp==1){
			cntpos=cntpos*pnaive(word);
			cntneg=cntneg*nnaive(word);
			}

		}
		catch(IOException e){
              		e.printStackTrace();
              	}
	}
    	catch (FileNotFoundException e){
              e.printStackTrace();
        }
	
  	return cp;
}

public int neglex(String word){
	String strRead;
	int cn=0;
 	try {
 		//read file from Distributed CachE
 	        BufferedReader reader = new BufferedReader(new FileReader("lexnegative"));
		try{
			while ((strRead=reader.readLine()) != null){     
  				String[] word1=strRead.split("\\W+"); 
                                if (word.equals(word1[0])){
                            		cn++;
                            		break;   
                            	}
                	}
			if(cn==1){
			
				cntpos=cntpos*pnaive(word);
				cntneg=cntneg*nnaive(word);
			}

		}
		catch(IOException e){
              		e.printStackTrace();
              	}
	}
    	catch (FileNotFoundException e){
              e.printStackTrace();
        }
	
  	return cn;
}

public double pnaive(String word)
{ int count=1200000;
  double sp=0;
  String strRead;
 		try {
 		             //read file from Distributed Cache
 	                     BufferedReader reader = new BufferedReader(new FileReader("pos"));
			     try{

                     			while ((strRead=reader.readLine()) != null){     
  						String[] word1=strRead.split("\\W+"); 
                                             
                            			if (word.equals(word1[0])){
                            				sp=Double.parseDouble(word1[1]);
                            				break;   
                            			}
                            			else{			
							sp=1;
                            			} 
                 
               				}

				}
				catch(IOException e){
              				e.printStackTrace();
              			}

		}
    		catch (FileNotFoundException e){
              		e.printStackTrace();
       		}

  //if(sp==0)
  //sp=1;
  //else
  sp=sp/count;

  return sp;
}



public double nnaive(String word)
{ int count=1200000;
  double sn=0;
  String strRead;
 		try {
 		             //read file from Distributed Cache
 	                     BufferedReader reader = new BufferedReader(new FileReader("neg"));
			     try{

                     			while ((strRead=reader.readLine()) != null){     
  						String[] word1=strRead.split("\\W+"); 
                                             
                            			if (word.equals(word1[0])){
                            				sn=Double.parseDouble(word1[1]);
                            				break;   
                            			}
                            			else{
							sn=1;
                            			} 
                 
               				}

				}
				catch(IOException e){
              				e.printStackTrace();
              			}

		}
    		catch (FileNotFoundException e){
              		e.printStackTrace();
       		}

  //if(sp==0)
  //sp=1;
  //else
  sn=sn/count;

  return sn;
}

public int poslexnot(String word){
	String strRead;
	int cp=0;
	
 	try {
 		//read file from Distributed CachE
 	        BufferedReader reader = new BufferedReader(new FileReader("lexpositive"));
		try{
			while ((strRead=reader.readLine()) != null){     
  				String[] word1=strRead.split("\\W+"); 
                                if (word.equals(word1[0])){
                            		cp++;
                            		break;   
                            	}
                	}
			if(cp==1 && fg==1)
			{cp=-1;
			fg=0;}
			//cntpos=cntpos*pnaive(word);
			//cntneg=cntneg*nnaive(word);
		}
		catch(IOException e){
             		e.printStackTrace();
        	}
	}
    	catch (FileNotFoundException e){
              e.printStackTrace();
        }
	
  	return cp;
}

public int neglexnot(String word){
	String strRead;
	int cn=0;
 	try {
 		//read file from Distributed CachE
 	        BufferedReader reader = new BufferedReader(new FileReader("lexnegative"));
		try{
			while ((strRead=reader.readLine()) != null){     
  				String[] word1=strRead.split("\\W+"); 
                                if (word.equals(word1[0])){
                            		cn++;
                            		break;   
                            	}
                	}
			if(cn==1 && fg==1)
				{cn=-1;
				fg=0;}
				//cntpos=cntpos*pnaive(word);
				//cntneg=cntneg*nnaive(word);

		}
		catch(IOException e){
              		e.printStackTrace();
              	}
	}
    	catch (FileNotFoundException e){
              e.printStackTrace();
        }
	
  	return cn;
}




/*    				double sp,sn;
    				sp=sn=0.5;
    				String s = value.toString().toLowerCase();
   				for (String word : s.split("\\W+")) {
      				//Naive Bayes Classifier for analysis of neutral
				if (word.length() > 0) {
      				sp=sp+pos(word);
      			 	sn=sn+neg(word);
      				}
   			 }

			if(sp>sn)
				output.collect(new Text(s), new DoubleWritable(sp-sn));
			else
				output.collect(new Text(s), new DoubleWritable(sp-sn));*/
			
//}


//to get probability of occurance in positive dictionary
public double pos(String word)
{ int count=446733;
  double sp=0;
  String strRead;
 		try {
 		             //read file from Distributed Cache
 	                     BufferedReader reader = new BufferedReader(new FileReader("pos"));
			     try{

                     		while ((strRead=reader.readLine()) != null)
                     		{     
  				String[] word1=strRead.split("\\W+"); 
                                             
                            	if (word.equals(word1[0])) {
                            		sp=Double.parseDouble(word1[1]);
                            		break;   
                            	}
                            	else
                            	{sp=1;
                            	} 
                 
               			}




		}
		catch(IOException e) {
              	e.printStackTrace();
              	}

	}
    	catch (FileNotFoundException e) {
              e.printStackTrace();
       }

  if(sp==0)
  sp=1;
  else
  sp=(sp+1)/count;

  return sp;
}

//to get probability of occurance in positive dictionary
public double neg(String word)
{ int count=398937;
  double sn=0;
  String strRead;
 	try {
              		//read file from Distributed Cache
                   	  BufferedReader reader = new BufferedReader(new FileReader("neg"));
			try{

                    		 while ((strRead=reader.readLine()) != null)
                     		{     
  					String[] word1=strRead.split("\\W+"); 
                                             
                         		if (word.equals(word1[0])) {
                          			sn=Double.parseDouble(word1[1]);
                            			break;   
                            		}
                            		else
                            		{sn=0;
                           		 } 
                 
               			}
		
			}
			catch(IOException e) {
              			e.printStackTrace();
              		}

		}
    		catch (FileNotFoundException e) {
              		e.printStackTrace();
              	}

if(sn==0)
sn=1;
else
sn=(sn+1)/count;

 return sn;
}



}
