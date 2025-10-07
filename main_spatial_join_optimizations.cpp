#include "def.h"
#include "getopt.h"
#include "./containers/relation.h"
#include "two_layer.h"
#include "partition.h"
#include "utils.h"


void usage()
{
    cerr << "NAME" << endl;
    cerr << "       ./sj_opt- spatial join using the 2-layer algorithm, different optimizations" << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./sj_opt [OPTION]... [FILE1] [FILE2]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       Mandatory arguments" << endl << endl;
    cerr << "       -p" << endl;
    cerr << "              number of partions per dimension" << endl;
    cerr << "       -b" << endl;
    cerr << "              mini joins baseline"  << endl;
    cerr << "       -u" << endl;
    cerr << "              mini joins and unnecessary comparisons"  << endl;
    cerr << "       -r" << endl;
    cerr << "              mini joins and redundant comparisons"  << endl;
    cerr << "       -c" << endl;
    cerr << "              mini joins, unnecessary and redundant comparisons"  << endl;
    cerr << "       -a" << endl;
    cerr << "              mini joins, unnecessary and redundant comparisons and storage optimizations"  << endl;
    cerr << "       Other arguments" << endl << endl;
    cerr << "       -h" << endl;
    cerr << "              display this help message and exit" << endl << endl;
    cerr << "EXAMPLES" << endl;
    cerr << "       Spatial join query using the 2-layer algorithm with mini joins baseline optimization with 2000 partitions per dimension." << endl;
    cerr << "              /sj_opt -p 2000 -b dataset_file1.csv dataset_file2.csv" << endl;
    cerr << "       Spatial join query using the 2-layer algorithm with mini joins and redundant comparisons with 2000 partitions per dimension." << endl;
    cerr << "              /sj_opt -p 2000 -r dataset_file1.csv dataset_file2.csv" << endl;
    cerr << "\n" << endl;
    exit(1);
}


int main(int argc, char **argv)
{
    char c;
    int runNumPartitionsPerRelation = -1, runProcessingMethod = -1, runNumPartitions = -1;
    Timer tim;
    double timeSorting = 0, timeIndexingOrPartitioning = 0, timeJoining = 0;
    Relation R, S, *pR, *pS;
    Relation *pRA, *pSA, *pRB, *pSB, *pRC, *pSC, *pRD, *pSD;
    size_t *pRA_size, *pSA_size, *pRB_size, *pSB_size, *pRC_size, *pSC_size, *pRD_size, *pSD_size;
    
    unsigned long long result = 0;


    while ((c = getopt(argc, argv, "bucharp:")) != -1)
    {
        switch (c)
        {
            case 'p':
                runNumPartitionsPerRelation = atoi(optarg);
                break;
            case 'b':
                runProcessingMethod = MINI_JOIN_ΒΑSELINE;
                break;
            case 'u':    
                runProcessingMethod = MINI_JOIN_UNNECESSARY_COMPARISONS ;
                break;
            case 'r':    
                runProcessingMethod = MINI_JOIN_REDUNDANT_COMPARISONS ;
                break;
            case 'c':
                runProcessingMethod = MINI_JOIN_UNNECESSARY_REDUNDANT_COMPARISONS;
                break;
            case 'a':
                runProcessingMethod = MINI_JOIN_UNNECESSARY_REDUNDANT_COMPARISONS_STORE_OPT;
                break;
            case 'h':
                usage();
            default:
                cerr << "Wrong arguments! ";
                usage();
                break;
        }
    }

    if(runNumPartitionsPerRelation == -1)
    {
        cerr << "Number of partitions is missing." << endl;
        usage();
    }

    if (runProcessingMethod == -1 ){
        cerr<<"Spatial optimization method value is missing."<<endl;
        usage();
    }

    if (argv[optind] == nullptr || argv[optind+1] == nullptr){
        cerr<<"Input datasets are missing."<<endl;
        usage();
    }

    // Load inputs
    #pragma omp parallel sections
    {
        #pragma omp section
        {
            R.load(argv[optind]);
        }
        #pragma omp section
        {
            S.load(argv[optind+1]);
        }
    }

    //exit(1);

    Coord minX = min(R.minX, S.minX);
    Coord maxX = max(R.maxX, S.maxX);
    Coord minY = min(R.minY, S.minY);
    Coord maxY = max(R.maxY, S.maxY);
    Coord diffX = maxX - minX;
    Coord diffY = maxY - minY;
    Coord maxExtend = (diffX<diffY)?diffY:diffX;

    R.normalize(minX, maxX, minY, maxY, maxExtend);
    S.normalize(minX, maxX, minY, maxY, maxExtend);
    
    runNumPartitions = runNumPartitionsPerRelation * runNumPartitionsPerRelation;
    
    if (runProcessingMethod == MINI_JOIN_ΒΑSELINE)
    {
        // Single-threaded processing.
        pRA_size = new size_t[runNumPartitions];
        pRB_size = new size_t[runNumPartitions];
        pRC_size = new size_t[runNumPartitions];
        pRD_size = new size_t[runNumPartitions];

        pSA_size = new size_t[runNumPartitions];
        pSB_size = new size_t[runNumPartitions];
        pSC_size = new size_t[runNumPartitions];
        pSD_size = new size_t[runNumPartitions];

        pR = new Relation[runNumPartitions];
        pS = new Relation[runNumPartitions];

        memset(pRA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRD_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSD_size, 0, runNumPartitions*sizeof(size_t));

        tim.start();
        partition::spatial_join::baseline::PartitionTwoDimensional(R, S, pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitionsPerRelation);
        timeIndexingOrPartitioning = tim.stop();
        
        tim.start();
        utils::spatial_join::baseline::SortYStartOneArray(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size ,pRD_size, pSD_size, runNumPartitions);
        timeSorting = tim.stop();
        
        tim.start();
        result = twoLayer::spatial_join::baseline::ForwardScanBased_PlaneSweep_CNT(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitions);
        timeJoining = tim.stop();


        cout<<"MINI-JOIN-BASELINE-result = " <<result<<"\ttimeIndexingOrPartitioning = " << timeIndexingOrPartitioning<<"\ttimeSorting = "<<timeSorting<<"\ttimeJoining = "<< timeJoining <<"\tfirst_dataset = "<<argv[optind]  <<"\tsecond_dataset = "<< argv[optind+1] <<"\trunNumPartitionPerReltion = "<< runNumPartitionsPerRelation<<endl;                        


        delete[] pR;
        delete[] pS;

        delete[] pRA_size;
        delete[] pRB_size;
        delete[] pRC_size;
        delete[] pRD_size;

        delete[] pSA_size;
        delete[] pSB_size;
        delete[] pSC_size;
        delete[] pSD_size;
    }
    else if (runProcessingMethod == MINI_JOIN_UNNECESSARY_COMPARISONS) 
    {
        pRA_size = new size_t[runNumPartitions];
        pRB_size = new size_t[runNumPartitions];
        pRC_size = new size_t[runNumPartitions];
        pRD_size = new size_t[runNumPartitions];

        pSA_size = new size_t[runNumPartitions];
        pSB_size = new size_t[runNumPartitions];
        pSC_size = new size_t[runNumPartitions];
        pSD_size = new size_t[runNumPartitions];

        memset(pRA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRD_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSD_size, 0, runNumPartitions*sizeof(size_t));

        pR = new Relation[runNumPartitions];
        pS = new Relation[runNumPartitions];

        tim.start();
        partition::spatial_join::baseline::PartitionTwoDimensional(R, S, pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitionsPerRelation);
        timeIndexingOrPartitioning = tim.stop();

        tim.start();
        utils::spatial_join::baseline::SortYStartOneArray(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size , pRD_size, pSD_size, runNumPartitions);
        timeSorting = tim.stop();

        tim.start();
        result = twoLayer::spatial_join::unnecessary_comparisons::ForwardScanBased_PlaneSweep_CNT_Less(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, /*runPlaneSweepOnX,*/ runNumPartitions);
        //result = fs_2d_multi_way::single::ForwardScanBased_PlaneSweep_CNT(pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runPlaneSweepOnX, runNumPartitionsPerRelation);
        timeJoining = tim.stop();
        
        delete[] pR;
        delete[] pS;

        delete[] pRA_size;
        delete[] pRB_size;
        delete[] pRC_size;
        delete[] pRD_size;

        delete[] pSA_size;
        delete[] pSB_size;
        delete[] pSC_size;
        delete[] pSD_size;
    
        cout<<"MINI-JOIN-UNNECESSARY_COMPARISONS-result = "<<result<<"\ttimeIndexingOrPartitioning = " << timeIndexingOrPartitioning<<"\ttimeSorting = "<<timeSorting<<"\ttimeJoining = "<< timeJoining  <<"\tfirst_dataset = "<<argv[optind]  <<"\tsecond_dataset = "<< argv[optind+1] <<"\trunNumPartitionPerReltion = "<< runNumPartitionsPerRelation<<endl;

    }
    else if (runProcessingMethod == MINI_JOIN_REDUNDANT_COMPARISONS)
    {
        pRA_size = new size_t[runNumPartitions];
        pRB_size = new size_t[runNumPartitions];
        pRC_size = new size_t[runNumPartitions];
        pRD_size = new size_t[runNumPartitions];

        pSA_size = new size_t[runNumPartitions];
        pSB_size = new size_t[runNumPartitions];
        pSC_size = new size_t[runNumPartitions];
        pSD_size = new size_t[runNumPartitions];

        memset(pRA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRD_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSD_size, 0, runNumPartitions*sizeof(size_t));

        pR = new Relation[runNumPartitions];
        pS = new Relation[runNumPartitions];

        tim.start();
        partition::spatial_join::baseline::PartitionTwoDimensional(R, S, pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitionsPerRelation);
        timeIndexingOrPartitioning = tim.stop();


        tim.start();
        utils::spatial_join::redundant_comparisons::SortYStartOneArrayApproach1(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size , pRD_size, pSD_size, runNumPartitions);
        timeSorting = tim.stop();


        tim.start();
        result = twoLayer::spatial_join::redundant_comparisons::ForwardScanBased_PlaneSweep_CNT_Appoach1(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, /*runPlaneSweepOnX,*/ runNumPartitions);
        timeJoining = tim.stop();

        cout<<"MINI-JOIN-REDUNDANT_COMPARISONS-result = "<<result<<"\ttimeIndexingOrPartitioning = " << timeIndexingOrPartitioning<<"\ttimeSorting = "<<timeSorting<<"\ttimeJoining = "<< timeJoining  <<"\tfirst_dataset = "<<argv[optind]  <<"\tsecond_dataset = "<< argv[optind+1] <<"\trunNumPartitionPerReltion = "<< runNumPartitionsPerRelation<<endl;

        delete[] pR;
        delete[] pS;

        delete[] pRA_size;
        delete[] pRB_size;
        delete[] pRC_size;
        delete[] pRD_size;

        delete[] pSA_size;
        delete[] pSB_size;
        delete[] pSC_size;
        delete[] pSD_size;

    }
    else if (runProcessingMethod == MINI_JOIN_UNNECESSARY_REDUNDANT_COMPARISONS)
    {
        pRA_size = new size_t[runNumPartitions];
        pRB_size = new size_t[runNumPartitions];
        pRC_size = new size_t[runNumPartitions];
        pRD_size = new size_t[runNumPartitions];

        pSA_size = new size_t[runNumPartitions];
        pSB_size = new size_t[runNumPartitions];
        pSC_size = new size_t[runNumPartitions];
        pSD_size = new size_t[runNumPartitions];

        pR = new Relation[runNumPartitions];
        pS = new Relation[runNumPartitions];

        memset(pRA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRD_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSD_size, 0, runNumPartitions*sizeof(size_t));


        tim.start();
        partition::spatial_join::baseline::PartitionTwoDimensional(R, S, pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitionsPerRelation);
        timeIndexingOrPartitioning = tim.stop();
        
        tim.start();
        utils::spatial_join::redundant_comparisons::SortYStartOneArrayApproach1(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size , pRD_size, pSD_size, runNumPartitions);
        timeSorting = tim.stop();
    
        tim.start();
        result = twoLayer::spatial_join::unnecessary_redundant_comparisons::ForwardScanBased_PlaneSweep_CNT_Less_Appoach1(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size,  runNumPartitions);
        timeJoining = tim.stop();
       
        cout<<"MINI-JOIN-UNNECESSARY_REDUNDANT_COMPARISONS-result = "<<result<<"\ttimeIndexingOrPartitioning = " << timeIndexingOrPartitioning<<"\ttimeSorting = "<<timeSorting<<"\ttimeJoining = "<< timeJoining  <<"\tfirst_dataset = "<<argv[optind]  <<"\tsecond_dataset = "<< argv[optind+1] <<"\trunNumPartitionPerReltion = "<< runNumPartitionsPerRelation<<endl;


        delete[] pR;
        delete[] pS;

        delete[] pRA_size;
        delete[] pRB_size;
        delete[] pRC_size;
        delete[] pRD_size;

        delete[] pSA_size;
        delete[] pSB_size;
        delete[] pSC_size;
        delete[] pSD_size;
    }
    else if (runProcessingMethod == MINI_JOIN_UNNECESSARY_REDUNDANT_COMPARISONS_STORE_OPT)
    {
        vector <ABrec2> *pRABdec2 , *pSABdec2;
        vector<Crec2> *pRCdec2, *pSCdec2;
        vector<Drec> *pRDdec, *pSDdec;

        pRA_size = new size_t[runNumPartitions];
        pRB_size = new size_t[runNumPartitions];
        pRC_size = new size_t[runNumPartitions];
        pRD_size = new size_t[runNumPartitions];
        
        pSA_size = new size_t[runNumPartitions];
        pSB_size = new size_t[runNumPartitions];
        pSC_size = new size_t[runNumPartitions];
        pSD_size = new size_t[runNumPartitions];

        memset(pRA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRD_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSD_size, 0, runNumPartitions*sizeof(size_t));

        pR = new Relation[runNumPartitions];
        pS = new Relation[runNumPartitions];

        pRABdec2 = new vector<ABrec2>[runNumPartitions];
        pSABdec2 = new vector<ABrec2>[runNumPartitions];
        
        pRCdec2 = new vector<Crec2>[runNumPartitions];
        pSCdec2 = new vector<Crec2>[runNumPartitions];
        
        pRDdec = new vector<Drec>[runNumPartitions];
        pSDdec = new vector<Drec>[runNumPartitions];  

        tim.start();
        partition::spatial_join::all_opts::PartitionTwoDimensionalDec2(R, S, pRABdec2, pSABdec2, pRCdec2, pSCdec2, pRDdec, pSDdec, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitionsPerRelation);
        timeIndexingOrPartitioning = tim.stop();
       
        tim.start();
        utils::spatial_join::all_opts::SortYStartOneArray2(pRABdec2, pSABdec2, pRCdec2, pSCdec2, pRDdec, pSDdec, pRB_size, pSB_size,  runNumPartitions);
        timeSorting = tim.stop();

        tim.start();
        result = twoLayer::spatial_join::all_opts::ForwardScanBased_PlaneSweep_CNT_Less_Dec_Approach1(pRABdec2, pSABdec2, pRCdec2, pSCdec2, pRDdec, pSDdec, pRB_size, pSB_size, runNumPartitions);
        timeJoining = tim.stop();
        
        cout<<"MINI_JOIN_UNNECESSARY_REDUNDANT_COMPARISONS_STORE_OPT result = "<<result<<"\tpartitioning = "<<runNumPartitionsPerRelation<<"\ttimeIndexingOrPartitioning = "<<timeIndexingOrPartitioning<<"\ttimeSorting = "<< timeSorting<<"\ttimeJoining = " << timeJoining<<"\tfirst_dataset = "<<argv[optind]  <<"\tsecond_dataset = "<< argv[optind+1] <<"\trunNumPartitionPerReltion = "<< runNumPartitionsPerRelation<<endl;

        delete[] pRABdec2;
        delete[] pSABdec2;

        delete[] pRCdec2;
        delete[] pSCdec2;

        delete[] pRDdec;
        delete[] pSDdec;

        delete[] pSA_size;
        delete[] pSB_size;
        delete[] pSC_size;
        delete[] pSD_size;
        delete[] pS;        

        delete[] pRA_size;
        delete[] pRB_size;
        delete[] pRC_size;
        delete[] pRD_size;
        delete[] pR;
    }
}