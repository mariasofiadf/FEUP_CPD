#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <time.h>
#include <cstdlib>
#include <papi.h>
#include <string>

using namespace std;

#define SYSTEMTIME clock_t


void OnMult(int m_ar, int m_br, int runs = 1) 
{
	
	SYSTEMTIME Time1, Time2;
	
	char st[100];
	double temp;
	int i, j, k;

	double *pha, *phb, *phc;
	

    pha = (double *)malloc((m_ar * m_ar) * sizeof(double));
	phb = (double *)malloc((m_ar * m_ar) * sizeof(double));
	phc = (double *)malloc((m_ar * m_ar) * sizeof(double));

	for(i=0; i<m_ar; i++)
		for(j=0; j<m_ar; j++)
			pha[i*m_ar + j] = (double)1.0;



	for(i=0; i<m_br; i++)
		for(j=0; j<m_br; j++)
			phb[i*m_br + j] = (double)(i+1);


	int r = 0;
    Time1 = clock();
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


    Time2 = clock();
	sprintf(st, "Time: %3.3f seconds\n", (double)(Time2 - Time1) / CLOCKS_PER_SEC/runs);
	cout << st;

	// display 10 elements of the result matrix tto verify correctness
	cout << "Result matrix: " << endl;
	for(i=0; i<1; i++)
	{	for(j=0; j<min(10,m_br); j++)
			cout << phc[j] << " ";
	}
	cout << endl;

    free(pha);
    free(phb);
    free(phc);
	
	
}

// add code here for line x line matriz multiplication
void OnMultLine(int m_ar, int m_br, int runs = 1)
{
	SYSTEMTIME Time1, Time2;
	
	char st[100];
	double temp;
	int i, j, k;

	double *pha, *phb, *phc;
	

    pha = (double *)malloc((m_ar * m_ar) * sizeof(double));
	phb = (double *)malloc((m_ar * m_ar) * sizeof(double));
	phc = (double *)malloc((m_ar * m_ar) * sizeof(double));

	for(i=0; i<m_ar; i++)
		for(j=0; j<m_ar; j++)
			pha[i*m_ar + j] = (double)1.0;



	for(i=0; i<m_br; i++)
		for(j=0; j<m_br; j++)
			phb[i*m_br + j] = (double)(i+1);


	int r = 0;
    Time1 = clock();
	while(runs > r){

		for(i=0; i<m_ar; i++)
		{	for( k=0; k<m_br; k++)
			{	
				for( j=0; j<m_ar; j++)
				{	
					phc[i*m_ar+j] += pha[i*m_ar+k] * phb[k*m_br+j];
				}
			}
		}
		r++;
	}


    Time2 = clock();
	sprintf(st, "Time: %3.3f seconds\n", (double)(Time2 - Time1) / CLOCKS_PER_SEC/runs);
	cout << st;

	// display 10 elements of the result matrix tto verify correctness
	cout << "Result matrix: " << endl;
	for(i=0; i<1; i++)
	{	for(j=0; j<min(10,m_br); j++)
			cout << phc[j] << " ";
	}
	cout << endl;

    free(pha);
    free(phb);
    free(phc);
	
   
    
}

// add code here for block x block matriz multiplication
void OnMultBlock(int m_ar, int m_br, int bkSize, int runs = 1)
{
    SYSTEMTIME Time1, Time2;
	
	char st[100];
	double temp;
	int i, j, k;

	double *pha, *phb, *phc;
	

    pha = (double *)malloc((m_ar * m_ar) * sizeof(double));
	phb = (double *)malloc((m_ar * m_ar) * sizeof(double));
	phc = (double *)malloc((m_ar * m_ar) * sizeof(double));

	for(i=0; i<m_ar; i++)
		for(j=0; j<m_ar; j++)
			pha[i*m_ar + j] = (double)1.0;



	for(i=0; i<m_br; i++)
		for(j=0; j<m_br; j++)
			phb[i*m_br + j] = (double)(i+1);


	int r = 0;
    Time1 = clock();

	while (runs > r){

		for(int ii = 0; ii < m_ar; ii += bkSize){
			for(int jj=0;jj<m_ar;jj+= bkSize){
				for(int kk=0;kk<m_ar;kk+= bkSize){
					for(int i=0;i<bkSize;i++){
						for(int k = 0; k<bkSize; k++){
							for(int j = 0; j<bkSize; j++){
							
								phc[(ii+i)*m_ar+ (jj+j)]  += pha[(ii+i)*m_ar+(kk+k)]* phb[(kk+k)*m_ar+(jj+j)];
							}
						}
					}
				}
			}
		}
		r++;
	}
    Time2 = clock();
	sprintf(st, "Time: %3.3f seconds\n", (double)(Time2 - Time1) / CLOCKS_PER_SEC/runs);
	cout << st;

	// display 10 elements of the result matrix tto verify correctness
	cout << "Result matrix: " << endl;
	for(i=0; i<1; i++)
	{	for(j=0; j<min(10,m_br); j++)
			cout << phc[j] << " ";
	}
	cout << endl;

    free(pha);
    free(phb);
    free(phc);
	

    
}

void printDCMs(int EventSet, int runs){

  	long long values[2];
	int ret;
	ret = PAPI_stop(EventSet, values);
	if (ret != PAPI_OK) cout << "ERROR: Stop PAPI" << endl;
	printf("L1 DCM: %lld \n",values[0]/runs);
	printf("L2 DCM: %lld \n",values[1]/runs);

	ret = PAPI_reset( EventSet );
	if ( ret != PAPI_OK )
		std::cout << "FAIL reset" << endl; 
}

void runTests(int EventSet, int runs){
	
	//1.
	int initial_size = 600;
	int increment = 400;
	int final_size = 3000;
	int ret;
	freopen("./ctests/1.txt", "w", stdout); 
	for(int i = initial_size; i <= final_size; i+=increment){
		ret = PAPI_start(EventSet);
		if (ret != PAPI_OK) cout << "ERROR: Start PAPI" << endl;

		cout << "Size: " << i << " by " << i << endl;
		OnMult(i,i, runs);

		printDCMs(EventSet,runs);
		cout << endl << endl;
	}


	//2a.
	initial_size = 600;
	increment = 400;
	final_size = 3000;
	freopen("./ctests/2a.txt", "w", stdout); 
	for(int i = initial_size; i <= final_size; i+=increment){
		ret = PAPI_start(EventSet);
		if (ret != PAPI_OK) cout << "ERROR: Start PAPI" << endl;

		cout << "Size: " << i << " by " << i << endl;
		OnMultLine(i,i,runs);

		printDCMs(EventSet,runs);
		cout << endl << endl;
	}

	//2b.
	initial_size = 4096;
	increment = 2048;
	final_size = 10240;
	freopen("./ctests/2b.txt", "w", stdout); 
	for(int i = initial_size; i <= final_size; i+=increment){
		ret = PAPI_start(EventSet);
		if (ret != PAPI_OK) cout << "ERROR: Start PAPI" << endl;

		cout << "Size: " << i << " by " << i << endl;
		OnMultLine(i,i,runs);

		printDCMs(EventSet,runs);
		cout << endl << endl;
	}


	//3.
	initial_size = 4096;
	increment = 2048;
	final_size = 10240;
	int block = 128;
	int max_block = 512;
	freopen("./ctests/3.txt", "w", stdout); 
	for(int i = initial_size; i <= final_size; i+=increment){
		for(int j = block; j <= max_block; j = 2*j){
		ret = PAPI_start(EventSet);
		if (ret != PAPI_OK) cout << "ERROR: Start PAPI" << endl;
			cout << "Size: " << i << " by " << i << endl;
			cout << "Block: " << j << endl;
			OnMultBlock(i,i,j,runs);

			printDCMs(EventSet,runs);
			cout << endl << endl;
		}


	}


}

void handle_error (int retval)
{
  printf("PAPI error %d: %s\n", retval, PAPI_strerror(retval));
  exit(1);
}

void init_papi() {
  int retval = PAPI_library_init(PAPI_VER_CURRENT);
  if (retval != PAPI_VER_CURRENT && retval < 0) {
    printf("PAPI library version mismatch!\n");
    exit(1);
  }
  if (retval < 0) handle_error(retval);

  std::cout << "PAPI Version Number: MAJOR: " << PAPI_VERSION_MAJOR(retval)
            << " MINOR: " << PAPI_VERSION_MINOR(retval)
            << " REVISION: " << PAPI_VERSION_REVISION(retval) << "\n";
}


int main (int argc, char *argv[])
{

	char c;
	int lin, col, blockSize;
	int op;
	
	int EventSet = PAPI_NULL;
  	long long values[2];
  	int ret;
	int runs = 4;
	

	ret = PAPI_library_init( PAPI_VER_CURRENT );
	if ( ret != PAPI_VER_CURRENT )
		std::cout << "FAIL" << endl;


	ret = PAPI_create_eventset(&EventSet);
		if (ret != PAPI_OK) cout << "ERROR: create eventset" << endl;


	ret = PAPI_add_event(EventSet,PAPI_L1_DCM );
	if (ret != PAPI_OK) cout << "ERROR: PAPI_L1_DCM" << endl;


	ret = PAPI_add_event(EventSet,PAPI_L2_DCM);
	if (ret != PAPI_OK) cout << "ERROR: PAPI_L2_DCM" << endl;


	op=1;
	do {
		cout << endl << "1. Multiplication" << endl;
		cout << "2. Line Multiplication" << endl;
		cout << "3. Block Multiplication" << endl;
		cout << "4. Run Timed Tests" << endl;
		cout << "Selection?: ";
		cin >>op;
		if (op == 0)
			break;
		if(op !=4){
			printf("Dimensions: lins=cols ? ");
			cin >> lin;
			col = lin;
		}


		// Start counting
		ret = PAPI_start(EventSet);
		if (ret != PAPI_OK) cout << "ERROR: Start PAPI" << endl;

		switch (op){
			case 1: 
				OnMult(lin, col);
				break;
			case 2:
				OnMultLine(lin, col);  
				break;
			case 3:
				cout << "Block Size? ";
				cin >> blockSize;
				OnMultBlock(lin, col, blockSize);  
				break;
			case 4:
				runTests(EventSet,runs);
				break;

		}

  		ret = PAPI_stop(EventSet, values);
  		if (ret != PAPI_OK) cout << "ERROR: Stop PAPI" << endl;
  		printf("L1 DCM: %lld \n",values[0]/runs);
  		printf("L2 DCM: %lld \n",values[1]/runs);

		ret = PAPI_reset( EventSet );
		if ( ret != PAPI_OK )
			std::cout << "FAIL reset" << endl; 



	}while (op != 0 && op != 4);

	ret = PAPI_remove_event( EventSet, PAPI_L1_DCM );
	if ( ret != PAPI_OK )
		std::cout << "FAIL remove event" << endl; 

	ret = PAPI_remove_event( EventSet, PAPI_L2_DCM );
	if ( ret != PAPI_OK )
		std::cout << "FAIL remove event" << endl; 

	ret = PAPI_destroy_eventset( &EventSet );
	if ( ret != PAPI_OK )
		std::cout << "FAIL destroy" << endl;

}