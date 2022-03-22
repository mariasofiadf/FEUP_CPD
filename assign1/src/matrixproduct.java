import java.io.PrintStream;
import java.util.*; 

class MatrixProduct{  
    public static void main(String args[]){  
        
        System.out.println("1. Multiplication");
		System.out.println("2. Line Multiplication");
		System.out.println("4. Run Timed Tests");
		System.out.println("Selection?: ");

        Scanner sc = new Scanner(System.in);    //System.in is a standard input stream
        int op = sc.nextInt(); 
        int lin  = 0, col = 0;
        int runs = 4;
		if(op !=4){
			System.out.println("Dimensions: lins=cols ? ");
			lin = sc.nextInt();  
			col = lin;
		}

		switch (op){
			case 1: 
				OnMult(lin, col, 1);
				break;
			case 2:
				OnMultLine(lin, col, 1);  
				break;
			case 4:
				runTests(4);
				break;

		}

    }  

    public static void OnMult(int m_ar, int m_br, int runs){
        
        //creating 3 matrices    
        double[] pha =new double[m_ar*m_br]; 
        double[] phb =new double[m_ar*m_br]; 
        double[] phc =new double[m_ar*m_br];   
        double temp; 
	    int i, j, k;
            
        //filling matrix A
        for(i=0; i<m_ar; i++)
            for(j=0; j<m_ar; j++)
                pha[i*m_ar + j] = (double)1.0;

        //filling matrix B
        for(i=0; i<m_br; i++)
            for(j=0; j<m_br; j++)
                phb[i*m_br + j] = (double)(i+1);


        double Time1, Time2;

        int r = 0;
        Time1 = System.nanoTime();
        //Product Calculation
        while(runs > r){
            for(i=0; i<m_ar; i++)
            {	for( j=0; j<m_br; j++)
                {	temp = 0;
                    for( k=0; k<m_ar; k++)
                    {	
                        temp += pha[i*m_ar+k] * phb[k*m_br+j];
                    }
                    phc[i*m_ar+j]=temp;
                }
            }
            r++;
        }

        Time2 = System.nanoTime();

        double TimeElapsed = (Time2-Time1)/(Math.pow(10,9))/runs;

        System.out.println("Time: " + TimeElapsed + " seconds");

        //Output Result Matrix
        for(i=0; i<1; i++){	
            for(j=0; j< Math.min(10,m_br); j++){

                System.out.print(phc[j]);
                System.out.print(" ");
            }
        }

        System.out.println("");

    }

    public static void OnMultLine(int m_ar, int m_br, int runs){
         //creating 3 matrices    
        double[] pha =new double[m_ar*m_br]; 
        double[] phb =new double[m_ar*m_br]; 
        double[] phc =new double[m_ar*m_br];   
        double temp; 
	    int i, j, k;
            
        //filling matrix A
        for(i=0; i<m_ar; i++)
            for(j=0; j<m_ar; j++)
                pha[i*m_ar + j] = (double)1.0;

        //filling matrix B
        for(i=0; i<m_br; i++)
            for(j=0; j<m_br; j++)
                phb[i*m_br + j] = (double)(i+1);


        double Time1, Time2;

        int r = 0;
        Time1 = System.nanoTime();
        //Product Calculation
        while(r < runs){
            for(i=0; i<m_ar; i++)
            {	for( k=0; k<m_ar; k++)
                {	
                    for( j=0; j<m_br; j++)
                    {	
                        phc[i*m_ar+j] += pha[i*m_ar+k] * phb[k*m_br+j];
                    }
                }
            }
            r++;
        }

        Time2 = System.nanoTime();

        double TimeElapsed = (Time2-Time1)/(Math.pow(10,9))/runs;

        System.out.println("Time: " + TimeElapsed + " seconds");

        //Output Result Matrix
        for(i=0; i<1; i++){	
            for(j=0; j< Math.min(10,m_br); j++){

                System.out.print(phc[j]);
                System.out.print(" ");
            }
        }

        System.out.println("");
    }


    public static void runTests(int runs){
        
        //1.
        int initial_size = 600;
        int increment = 400;
        int final_size = 3000;
        int ret;
        
        try{
            PrintStream fileOut = new PrintStream("./javatests/1.txt");
            // Redirect standard out to file.
            System.setOut(fileOut);
        }catch(Exception e){
            System.out.println("Error: " + e);
        }

        for(int i = initial_size; i <= final_size; i+=increment){
            System.out.println("Size: " + i + " by " + i);
            OnMult(i,i, runs);
            System.out.println("");
        }


        //2a.
        initial_size = 600;
        increment = 400;
        final_size = 3000;
        try{
            PrintStream fileOut = new PrintStream("./javatests/2a.txt");
            // Redirect standard out to file.
            System.setOut(fileOut);
        }catch(Exception e){
            System.out.println("Error: " + e);
        }

        for(int i = initial_size; i <= final_size; i+=increment){

            System.out.println("Size: " + i + " by " + i);
            OnMultLine(i,i, runs);
            System.out.println("");
        }


    }

}  
