#include "def.h"
#include "getopt.h"
#include "./containers/relation.h"
#include "two_layer.h"
#include "partition.h"
#include "utils.h"

void usage()
{
    cerr << "NAME" << endl;
    cerr << "       ./sj_transf - spatial join using the 2-layer algorithm, (strategy2: online trasformation, we have alreadt 2-layer indexes)   " << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./sj_transf [OPTION]... [FILE1] [FILE2]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       Mandatory arguments" << endl << endl;
    cerr << "       -p" << endl;
    cerr << "              number of partions for the first dataset per dimension" << endl;
    cerr << "       -s" << endl;
    cerr << "              number of partions for the second dataset per dimension"  << endl;
    cerr << "       -m" << endl;
    cerr << "              materialized algorithm"  << endl;
    cerr << "       -n" << endl;
    cerr << "              non materialized algorithm"  << endl;
    cerr << "       Other arguments" << endl << endl;
    cerr << "       -h" << endl;
    cerr << "              display this help message and exit" << endl << endl;
    cerr << "EXAMPLES" << endl;
    cerr << "       Spatial join query using the 2-layer algorithm materialized with 2048 partitions per dimension for the first dataset and 256 for the second." << endl;
    cerr << "              ./sj_transf -p 2048 -s 256 -m dataset_file1.csv dataset_file2.csv" << endl;
    cerr << "       Spatial join query using the 2-layer algorithm non materialized with 2048 partitions per dimension for the first dataset and 256 for the second." << endl;
    cerr << "              ./sj_transf -p 2048 -s 256 -n dataset_file1.csv dataset_file2.csv" << endl;
    cerr << "\n" << endl;
    exit(1);
}


int main (int argc, char ** argv)
{
    char c;
    int runNumPartitionsPerRelation = -1, runNumPartitions = -1, runNumSecondPartitionsPerRelation = -1, runNumSecondPartitions = -1;
    int runNumPartitionPerRelation_mater = -1, runNumPartition_mater = -1;
    int runProcessingMethod = -1, cell_id_mater = 0, xStartCell = 0, yStartCell = 0;
    Relation R, S, *pR, *pS, *pR_mater;
    size_t *pRA_size, *pSA_size, *pRB_size, *pSB_size, *pRC_size, *pSC_size, *pRD_size, *pSD_size, *pR_size, *pS_size;
    size_t *pRA_mater_size, *pRB_mater_size, *pRC_mater_size, *pRD_mater_size;
    double timeSorting = 0, timeIndexingOrPartitioning = 0, timeJoining = 0;
    Timer tim;
    unsigned long long result = 0;

    while ((c = getopt(argc, argv, "nmhp:s:")) != -1)
    {
        switch (c)
        {
            case 'p':
                runNumPartitionsPerRelation = atoi(optarg);
                break;

            case 's':
                runNumSecondPartitionsPerRelation = atoi(optarg);
                break;

            case 'm':
                runProcessingMethod = MATERIALIZED_PARTITIONS;
                break;
            
            case 'n':
                runProcessingMethod = NON_MATERIALIZED_PARTITIONS;
                break;
            case 'h':
                usage();
                break;
            default:
                cerr << "Wrong arguments! ";
                usage();
                break;
        }
    }

    if(runNumPartitionsPerRelation == -1)
    {
        cerr << "Number of first partition is missing." << endl;
        usage();
    }

    if(runNumSecondPartitionsPerRelation == -1)
    {
        cerr << "Number of second partition is missing." << endl;
        usage();
    }

    if (runProcessingMethod == -1 ){
        cerr<<"Tranformation method value is missing."<<endl;
        usage();
    }

    if (argv[optind] == nullptr || argv[optind+1] == nullptr){
        cerr<<"Input datasets are missing."<<endl;
        usage();
    }


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

    Coord minX = min(R.minX, S.minX);
    Coord maxX = max(R.maxX, S.maxX);
    Coord minY = min(R.minY, S.minY);
    Coord maxY = max(R.maxY, S.maxY);
    Coord diffX = maxX - minX;
    Coord diffY = maxY - minY;
    Coord maxExtend = (diffX<diffY)?diffY:diffX;

    R.normalize(minX, maxX, minY, maxY, maxExtend);
    S.normalize(minX, maxX, minY, maxY, maxExtend);

    //partitions size
    runNumPartitions = runNumPartitionsPerRelation * runNumPartitionsPerRelation;
    runNumSecondPartitions = runNumSecondPartitionsPerRelation * runNumSecondPartitionsPerRelation;
    
    //materialized partitions size
    runNumPartitionPerRelation_mater = runNumPartitionsPerRelation/runNumSecondPartitionsPerRelation;
    runNumPartition_mater = runNumPartitionPerRelation_mater * runNumPartitionPerRelation_mater;

    switch (runProcessingMethod)
    {
        case MATERIALIZED_PARTITIONS:
           pR = new Relation[runNumPartitions];

            pRA_size = new size_t[runNumPartitions];
            pRB_size = new size_t[runNumPartitions];
            pRC_size = new size_t[runNumPartitions];
            pRD_size = new size_t[runNumPartitions];
            memset(pRA_size, 0, runNumPartitions*sizeof(size_t));
            memset(pRB_size, 0, runNumPartitions*sizeof(size_t));
            memset(pRC_size, 0, runNumPartitions*sizeof(size_t));
            memset(pRD_size, 0, runNumPartitions*sizeof(size_t));   

            pS = new Relation[runNumSecondPartitions];

            pSA_size = new size_t[runNumSecondPartitions];
            pSB_size = new size_t[runNumSecondPartitions];
            pSC_size = new size_t[runNumSecondPartitions];
            pSD_size = new size_t[runNumSecondPartitions];
            memset(pSA_size, 0, runNumSecondPartitions*sizeof(size_t));
            memset(pSB_size, 0, runNumSecondPartitions*sizeof(size_t));
            memset(pSC_size, 0, runNumSecondPartitions*sizeof(size_t));
            memset(pSD_size, 0, runNumSecondPartitions*sizeof(size_t));   

            partition::window_disk::PartitionTwoDimensional(R, pR, pRA_size, pRB_size, pRC_size, pRD_size, runNumPartitionsPerRelation);
            partition::window_disk::PartitionTwoDimensional(S, pS, pSA_size, pSB_size, pSC_size, pSD_size, runNumSecondPartitionsPerRelation);

            tim.start();
            pR_mater = new Relation[runNumSecondPartitions];

            pRA_mater_size = new size_t[runNumSecondPartitions];
            pRB_mater_size = new size_t[runNumSecondPartitions];
            pRC_mater_size = new size_t[runNumSecondPartitions];
            pRD_mater_size = new size_t[runNumSecondPartitions];
            memset(pRA_mater_size, 0, runNumSecondPartitions*sizeof(size_t));
            memset(pRB_mater_size, 0, runNumSecondPartitions*sizeof(size_t));
            memset(pRC_mater_size, 0, runNumSecondPartitions*sizeof(size_t));
            memset(pRD_mater_size, 0, runNumSecondPartitions*sizeof(size_t));   

            xStartCell = 0;
            yStartCell = 0;
            cell_id_mater = 0;
            for (yStartCell = 0 ; yStartCell <  runNumPartitionsPerRelation ; yStartCell = yStartCell + runNumPartitionPerRelation_mater)
            {
                for (xStartCell = 0 ; xStartCell <  runNumPartitionsPerRelation ; xStartCell = xStartCell + runNumPartitionPerRelation_mater)
                {
                    int yEndCell = 0, xEndCell = 0, cell_id = 0;

                    yEndCell = yStartCell + runNumPartitionPerRelation_mater - 1;
                    xEndCell = xStartCell + runNumPartitionPerRelation_mater - 1;

                    //get the appropriate tile
                    cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                    //first row
                    //first tile in the row
                    pRA_mater_size[cell_id_mater] += pRB_size[cell_id];
                    pRB_mater_size[cell_id_mater] += pRC_size[cell_id] - pRB_size[cell_id];
                    pRC_mater_size[cell_id_mater] += pRD_size[cell_id] - pRC_size[cell_id];
                    pRD_mater_size[cell_id_mater] += pR[cell_id].size() - pRD_size[cell_id];
                    cell_id ++;  

                    //intermidiate tiles in the row
                    for (int i = xStartCell + 1; i < xEndCell ; i ++)
                    {
                        pRA_mater_size[cell_id_mater] += pRB_size[cell_id];
                        pRB_mater_size[cell_id_mater] += pRC_size[cell_id] - pRB_size[cell_id];
                        cell_id ++;   
                    }

                    //end tile in the row
                    pRA_mater_size[cell_id_mater] += pRB_size[cell_id];
                    pRB_mater_size[cell_id_mater] += pRC_size[cell_id] - pRB_size[cell_id];

                    //intermidiate rows
                    for (int y = yStartCell + 1 ; y < yEndCell ; y += 1)
                    {
                        //get the appropriate tile
                        cell_id = utils::getCellId(xStartCell, y, runNumPartitionsPerRelation);

                        //first tile in the row
                        pRA_mater_size[cell_id_mater] += pRB_size[cell_id];
                        pRC_mater_size[cell_id_mater] += pRD_size[cell_id] - pRC_size[cell_id];
                        cell_id ++;  

                        //intermidiate tiles in the row
                        for ( int i = xStartCell + 1; i < xEndCell ; i ++){
                            pRA_mater_size[cell_id_mater] += pRB_size[cell_id];
                            cell_id ++;
                        }

                        //end tile in the row
                        pRA_mater_size[cell_id_mater] += pRB_size[cell_id];
                    }

                    //last row 
                    //get the appropriate tile
                    cell_id = utils::getCellId(xStartCell, yEndCell, runNumPartitionsPerRelation);

                    //first tile in the row
                    pRA_mater_size[cell_id_mater] += pRB_size[cell_id];
                    pRC_mater_size[cell_id_mater] += pRD_size[cell_id] - pRC_size[cell_id];
                    cell_id ++;

                    //intermidiate tiles in the row
                    for (int i = xStartCell + 1; i < xEndCell ; i ++)
                    {
                        pRA_mater_size[cell_id_mater] += pRB_size[cell_id];
                        cell_id ++;
                    }

                    //last tile in the row
                    pRA_mater_size[cell_id_mater] += pRB_size[cell_id];

                    int sum = pRA_mater_size[cell_id_mater] + pRB_mater_size[cell_id_mater] + pRC_mater_size[cell_id_mater] + pRD_mater_size[cell_id_mater];
                    pR_mater[cell_id_mater].resize(sum);

                    pRB_mater_size[cell_id_mater] = pRA_mater_size[cell_id_mater] + pRB_mater_size[cell_id_mater];
                    pRC_mater_size[cell_id_mater] = sum - pRD_mater_size[cell_id_mater];
                    pRD_mater_size[cell_id_mater] = sum;

                    cell_id = 0;

                    //get the appropriate tile
                    cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);
                    
                    //first row
                    //first tile in the row
                    for (int j = 0 ; j < pRB_size[cell_id] ; j ++)
                    {
                        pRA_mater_size[cell_id_mater] = pRA_mater_size[cell_id_mater] - 1;
                        auto pos = pRA_mater_size[cell_id_mater];

                        pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                        pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                        pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                        pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                        pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                    }

                    for (int j = pRB_size[cell_id] ; j < pRC_size[cell_id] ; j ++)
                    {
                        pRB_mater_size[cell_id_mater] = pRB_mater_size[cell_id_mater] - 1;
                        auto pos = pRB_mater_size[cell_id_mater];

                        pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                        pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                        pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                        pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                        pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                    }

                    for (int j = pRC_size[cell_id] ; j < pRD_size[cell_id] ; j ++)
                    {
                        pRC_mater_size[cell_id_mater] = pRC_mater_size[cell_id_mater] - 1;
                        auto pos = pRC_mater_size[cell_id_mater];

                        pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                        pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                        pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                        pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                        pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                    }


                    for (int j = pRD_size[cell_id] ; j < pR[cell_id].size() ; j ++)
                    {
                        pRD_mater_size[cell_id_mater] = pRD_mater_size[cell_id_mater] - 1;
                        auto pos = pRD_mater_size[cell_id_mater];

                        pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                        pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                        pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                        pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                        pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                    }

                    cell_id ++;  

                    //intermidiate tiles in the row
                    for (int i = xStartCell + 1; i < xEndCell ; i ++)
                    {
                        for (int j = 0 ; j < pRB_size[cell_id] ; j ++)
                        {
                            pRA_mater_size[cell_id_mater] = pRA_mater_size[cell_id_mater] - 1;
                            auto pos = pRA_mater_size[cell_id_mater];

                            pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                            pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                            pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                            pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                            pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                        }

                        for (int j = pRB_size[cell_id] ; j < pRC_size[cell_id] ; j ++)
                        {
                            pRB_mater_size[cell_id_mater] = pRB_mater_size[cell_id_mater] - 1;
                            auto pos = pRB_mater_size[cell_id_mater];

                            pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                            pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                            pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                            pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                            pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                        }

                        cell_id ++;   
                    }

                    //end tile in the row
                    for (int j = 0 ; j < pRB_size[cell_id] ; j ++)
                    {
                        pRA_mater_size[cell_id_mater] = pRA_mater_size[cell_id_mater] - 1;
                        auto pos = pRA_mater_size[cell_id_mater];

                        pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                        pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                        pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                        pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                        pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                    }

                    for (int j = pRB_size[cell_id] ; j < pRC_size[cell_id] ; j ++)
                    {
                        pRB_mater_size[cell_id_mater] = pRB_mater_size[cell_id_mater] - 1;
                        auto pos = pRB_mater_size[cell_id_mater];

                        pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                        pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                        pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                        pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                        pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                    }

                    //intermidiate rows
                    for (int y = yStartCell + 1 ; y < yEndCell ; y += 1)
                    {
                        //get the appropriate tile
                        cell_id = utils::getCellId(xStartCell, y, runNumPartitionsPerRelation);

                        //first tile in the row
                        for (int j = 0 ; j < pRB_size[cell_id] ; j ++)
                        {
                            pRA_mater_size[cell_id_mater] = pRA_mater_size[cell_id_mater] - 1;
                            auto pos = pRA_mater_size[cell_id_mater];

                            pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                            pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                            pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                            pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                            pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                        }

                        for (int j = pRC_size[cell_id] ; j < pRD_size[cell_id] ; j ++)
                        {
                            pRC_mater_size[cell_id_mater] = pRC_mater_size[cell_id_mater] - 1;
                            auto pos = pRC_mater_size[cell_id_mater];

                            pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                            pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                            pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                            pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                            pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                        }
                        cell_id ++;  

                        //intermidiate tiles in the row
                        for (int i = xStartCell + 1; i < xEndCell ; i ++)
                        {
                            for ( int j = 0 ; j < pRB_size[cell_id] ; j ++)
                            {
                                pRA_mater_size[cell_id_mater] = pRA_mater_size[cell_id_mater] - 1;
                                auto pos = pRA_mater_size[cell_id_mater];

                                pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                                pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                                pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                                pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                                pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                            }
                            cell_id ++;
                        }

                        //end tile in the row
                        for (int j = 0 ; j < pRB_size[cell_id] ; j ++)
                        {
                            pRA_mater_size[cell_id_mater] = pRA_mater_size[cell_id_mater] - 1;
                            auto pos = pRA_mater_size[cell_id_mater];

                            pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                            pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                            pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                            pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                            pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                        }
                    }

                    //last row 
                    //get the appropriate tile
                    cell_id = utils::getCellId(xStartCell, yEndCell, runNumPartitionsPerRelation);

                    //first tile in the row
                    for (int j = 0 ; j < pRB_size[cell_id] ; j ++)
                    {
                        pRA_mater_size[cell_id_mater] = pRA_mater_size[cell_id_mater] - 1;
                        auto pos = pRA_mater_size[cell_id_mater];

                        pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                        pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                        pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                        pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                        pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                    }

                    for (int j = pRC_size[cell_id] ; j < pRD_size[cell_id] ; j ++)
                    {
                        pRC_mater_size[cell_id_mater] = pRC_mater_size[cell_id_mater] - 1;
                        auto pos = pRC_mater_size[cell_id_mater];

                        pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                        pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                        pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                        pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                        pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                    }
                    cell_id ++;

                    //intermidiate tiles in the row
                    for (int i = xStartCell + 1; i < xEndCell ; i ++)
                    {
                        for (int j = 0 ; j < pRB_size[cell_id] ; j ++)
                        {
                            pRA_mater_size[cell_id_mater] = pRA_mater_size[cell_id_mater] - 1;
                            auto pos = pRA_mater_size[cell_id_mater];

                            pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                            pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                            pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                            pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                            pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                        }
                        cell_id ++;
                    }

                    //last tile in the row
                    for (int j = 0 ; j < pRB_size[cell_id] ; j ++)
                    {
                        pRA_mater_size[cell_id_mater] = pRA_mater_size[cell_id_mater] - 1;
                        auto pos = pRA_mater_size[cell_id_mater];

                        pR_mater[cell_id_mater][pos].id = pR[cell_id][j].id;
                        pR_mater[cell_id_mater][pos].xStart = pR[cell_id][j].xStart;
                        pR_mater[cell_id_mater][pos].yStart = pR[cell_id][j].yStart;
                        pR_mater[cell_id_mater][pos].xEnd = pR[cell_id][j].xEnd;
                        pR_mater[cell_id_mater][pos].yEnd = pR[cell_id][j].yEnd;
                    }
                    cell_id_mater ++;   
                }
            }
            timeIndexingOrPartitioning = tim.stop();

            tim.start();
            utils::spatial_join::redundant_comparisons::SortYStartOneArrayApproach1(pR_mater, pS, pRB_mater_size, pSB_size, pRC_mater_size, pSC_size , pRD_mater_size, pSD_size, runNumSecondPartitions);
            timeSorting = tim.stop();

            tim.start();
            result = twoLayer::spatial_join::unnecessary_redundant_comparisons::ForwardScanBased_PlaneSweep_CNT_Less_Appoach1(pR_mater, pS, pRB_mater_size, pSB_size, pRC_mater_size, pSC_size, pRD_mater_size, pSD_size, /*runPlaneSweepOnX,*/ runNumSecondPartitions);
            timeJoining = tim.stop();

            cout<<"Materialized-strategy2-Spatial-Join-result = "<<result<<"\ttimeIndexingOrPartitioning = "<<timeIndexingOrPartitioning<<"\ttimeSorting = "<< timeSorting<<"\ttimeJoining = " << timeJoining<< "\tfinal = " <<timeIndexingOrPartitioning + timeSorting + timeJoining<<"\tfirst_dataset = " << argv[optind] <<"\tfirst_partition = " << runNumPartitionsPerRelation <<"\tsecond_dataset = " << argv[optind+1] <<"\tsecond_partition = "<< runNumSecondPartitionsPerRelation <<endl;

            delete[] pR;
            delete[] pS;
            delete[] pR_mater;

            delete[] pRA_size;
            delete[] pRB_size;
            delete[] pRC_size;
            delete[] pRD_size;

            delete[] pSA_size;
            delete[] pSB_size;
            delete[] pSC_size;
            delete[] pSD_size;

            delete[] pRA_mater_size;
            delete[] pRB_mater_size;
            delete[] pRC_mater_size;
            delete[] pRD_mater_size;
    
            break; 

        case NON_MATERIALIZED_PARTITIONS :
            result = 0;
            pR = new Relation[runNumPartitions];

            pRA_size = new size_t[runNumPartitions];
            pRB_size = new size_t[runNumPartitions];
            pRC_size = new size_t[runNumPartitions];
            pRD_size = new size_t[runNumPartitions];
            memset(pRA_size, 0, runNumPartitions*sizeof(size_t));
            memset(pRB_size, 0, runNumPartitions*sizeof(size_t));
            memset(pRC_size, 0, runNumPartitions*sizeof(size_t));
            memset(pRD_size, 0, runNumPartitions*sizeof(size_t));   

            pS = new Relation[runNumSecondPartitions];

            pSA_size = new size_t[runNumSecondPartitions];
            pSB_size = new size_t[runNumSecondPartitions];
            pSC_size = new size_t[runNumSecondPartitions];
            pSD_size = new size_t[runNumSecondPartitions];
            memset(pSA_size, 0, runNumSecondPartitions*sizeof(size_t));
            memset(pSB_size, 0, runNumSecondPartitions*sizeof(size_t));
            memset(pSC_size, 0, runNumSecondPartitions*sizeof(size_t));
            memset(pSD_size, 0, runNumSecondPartitions*sizeof(size_t));   

            partition::window_disk::PartitionTwoDimensional(R, pR, pRA_size, pRB_size, pRC_size, pRD_size, runNumPartitionsPerRelation);
            partition::window_disk::PartitionTwoDimensional(S, pS, pSA_size, pSB_size, pSC_size, pSD_size, runNumSecondPartitionsPerRelation);

            tim.start();
            utils::spatial_join::redundant_comparisons::SortYStartOneArrayApproach1(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size , pRD_size, pSD_size, runNumPartitions, runNumSecondPartitions);
            timeSorting = tim.stop();


            tim.start();
            xStartCell = 0;
            yStartCell = 0;
            cell_id_mater = 0;
            for (yStartCell = 0 ; yStartCell <  runNumPartitionsPerRelation ; yStartCell = yStartCell + runNumPartitionPerRelation_mater)
            {
                for (xStartCell = 0 ; xStartCell <  runNumPartitionsPerRelation ; xStartCell = xStartCell + runNumPartitionPerRelation_mater)
                {
                    int yEndCell = 0, xEndCell = 0 , cell_id = 0;

                    yEndCell = yStartCell + runNumPartitionPerRelation_mater - 1;
                    xEndCell = xStartCell + runNumPartitionPerRelation_mater - 1;

                    //get the appropriate tile
                    cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);
                
                    //first row
                    //first tile in the row
                    //A-B'
                    if (pRC_size[cell_id] > pRB_size[cell_id] && pSB_size[cell_id_mater] > 0)
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V2_Y_2( pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pRC_size[cell_id], pSB_size[cell_id_mater]);
                    }

                    //A-C'
                    if (pRD_size[cell_id] > pRC_size[cell_id] && pSB_size[cell_id_mater] > 0)
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Approach1_Less(pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pRC_size[cell_id], pRD_size[cell_id]);
                    }

                    //A-D'
                    if (pR[cell_id].size() > pRD_size[cell_id] && pSB_size[cell_id_mater] > 0)
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V5_Y_2_Approach_Less(pR[cell_id], pS[cell_id_mater], pRD_size[cell_id], pSB_size[cell_id_mater]);
                    }

                    //B-C'
                    if (pRD_size[cell_id] > pRC_size[cell_id] && pSC_size[cell_id_mater] > pSB_size[cell_id_mater])
                    {                                     
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V4_Y_2_Approach_Less( pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pSC_size[cell_id_mater], pRC_size[cell_id], pRD_size[cell_id]);
                    }

                    //C-B'
                    if (pRC_size[cell_id] > pRB_size[cell_id] && pSD_size[cell_id_mater] > pSC_size[cell_id_mater])
                    {                                     
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V4_Y_2_Approach_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pRC_size[cell_id], pSC_size[cell_id_mater], pSD_size[cell_id_mater] );
                    }

                    //D-A'
                    if (pRB_size[cell_id] > 0 && pS[cell_id_mater].size() > pSD_size[cell_id_mater])
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V5_Y_2_Approach_Less(pS[cell_id_mater], pR[cell_id], pSD_size[cell_id_mater] , pRB_size[cell_id]);
                    }

                    //B-A'
                    if (pRB_size[cell_id] > 0 && pSC_size[cell_id_mater] > pSB_size[cell_id_mater])
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V2_Y_2(pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pSC_size[cell_id_mater], pRB_size[cell_id]);
                    }

                    //A-A';
                    if (pRB_size[cell_id] > 0 && pSB_size[cell_id_mater] > 0)
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSB_size[cell_id_mater]);
                    }

                    //C-A'
                    if (pRB_size[cell_id] > 0 && pSD_size[cell_id_mater] > pSC_size[cell_id_mater])
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Approach1_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSC_size[cell_id_mater], pSD_size[cell_id_mater]);
                    }
                    cell_id ++;  

                    //intermidiate tiles in the row
                    for (int i = xStartCell + 1; i < xEndCell ; i ++)
                    {
                        //A-B
                        if (pRC_size[cell_id] > pRB_size[cell_id] && pSB_size[cell_id_mater] > 0)
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V2_Y_2( pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pRC_size[cell_id], pSB_size[cell_id_mater]);
                        }

                        //C-B'
                        if (pRC_size[cell_id] > pRB_size[cell_id] && pSD_size[cell_id_mater] > pSC_size[cell_id_mater])
                        {                                     
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V4_Y_2_Approach_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pRC_size[cell_id], pSC_size[cell_id_mater], pSD_size[cell_id_mater] );
                        }

                        //D-A'
                        if (pRB_size[cell_id] > 0 && pS[cell_id_mater].size() > pSD_size[cell_id_mater])
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V5_Y_2_Approach_Less(pS[cell_id_mater], pR[cell_id], pSD_size[cell_id_mater] , pRB_size[cell_id]);
                        }

                        //B-A'
                        if (pRB_size[cell_id] > 0 && pSC_size[cell_id_mater] > pSB_size[cell_id_mater])
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V2_Y_2(pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pSC_size[cell_id_mater], pRB_size[cell_id]);
                        }

                        //A-A';
                        if (pRB_size[cell_id] > 0 && pSB_size[cell_id_mater] > 0)
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSB_size[cell_id_mater]);
                        }  

                        //C-A'
                        if (pRB_size[cell_id] > 0 && pSD_size[cell_id_mater] > pSC_size[cell_id_mater])
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Approach1_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSC_size[cell_id_mater], pSD_size[cell_id_mater]);
                        }
                        cell_id ++;   
                    }

                    //end tile in the row
                    //A-B
                    if (pRC_size[cell_id] > pRB_size[cell_id] && pSB_size[cell_id_mater] > 0)
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V2_Y_2( pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pRC_size[cell_id], pSB_size[cell_id_mater]);
                    }

                    //C-B'
                    if (pRC_size[cell_id] > pRB_size[cell_id] && pSD_size[cell_id_mater] > pSC_size[cell_id_mater])
                    {                                     
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V4_Y_2_Approach_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pRC_size[cell_id], pSC_size[cell_id_mater], pSD_size[cell_id_mater] );
                    }

                    //D-A'
                    if (pRB_size[cell_id] > 0 && pS[cell_id_mater].size() > pSD_size[cell_id_mater])
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V5_Y_2_Approach_Less(pS[cell_id_mater], pR[cell_id], pSD_size[cell_id_mater] , pRB_size[cell_id]);
                    }

                    //B-A'
                    if (pRB_size[cell_id] > 0 && pSC_size[cell_id_mater] > pSB_size[cell_id_mater])
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V2_Y_2(pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pSC_size[cell_id_mater], pRB_size[cell_id]);
                    }

                    //A-A';
                    if (pRB_size[cell_id] > 0 && pSB_size[cell_id_mater] > 0)
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSB_size[cell_id_mater]);
                    }  

                    //C-A'
                    if (pRB_size[cell_id] > 0 && pSD_size[cell_id_mater] > pSC_size[cell_id_mater])
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Approach1_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSC_size[cell_id_mater], pSD_size[cell_id_mater]);
                    }


                    //intermidiate rows
                    for (int y = yStartCell + 1 ; y < yEndCell ; y += 1)
                    {
                        //get the appropriate tile
                        cell_id = utils::getCellId(xStartCell, y, runNumPartitionsPerRelation);

                        //first tile in the row
                        //A-C' 
                        if (pRD_size[cell_id] > pRC_size[cell_id] && pSB_size[cell_id_mater] > 0)
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Approach1_Less(pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pRC_size[cell_id], pRD_size[cell_id]);
                        }

                        ///B-C'
                        if (pRD_size[cell_id] > pRC_size[cell_id] && pSC_size[cell_id_mater] > pSB_size[cell_id_mater])
                        {                                     
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V4_Y_2_Approach_Less( pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pSC_size[cell_id_mater], pRC_size[cell_id], pRD_size[cell_id]);
                        }

                        //D-A'
                        if (pRB_size[cell_id] > 0 && pS[cell_id_mater].size() > pSD_size[cell_id_mater])
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V5_Y_2_Approach_Less(pS[cell_id_mater], pR[cell_id], pSD_size[cell_id_mater] , pRB_size[cell_id]);
                        }

                        //B-A'
                        if (pRB_size[cell_id] > 0 && pSC_size[cell_id_mater] > pSB_size[cell_id_mater])
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V2_Y_2(pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pSC_size[cell_id_mater], pRB_size[cell_id]);
                        }

                        //A-A';
                        if (pRB_size[cell_id] > 0 && pSB_size[cell_id_mater] > 0)
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSB_size[cell_id_mater]);
                        }

                        //C-A'
                        if (pRB_size[cell_id] > 0 && pSD_size[cell_id_mater] > pSC_size[cell_id_mater])
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Approach1_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSC_size[cell_id_mater], pSD_size[cell_id_mater]);
                        }
                        cell_id ++;  

                        //intermidiate tiles in the row
                        for (int i = xStartCell + 1; i < xEndCell ; i ++)
                        {
                            //D-A'
                            if (pRB_size[cell_id] > 0 && pS[cell_id_mater].size() > pSD_size[cell_id_mater])
                            {
                                result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V5_Y_2_Approach_Less(pS[cell_id_mater], pR[cell_id], pSD_size[cell_id_mater] , pRB_size[cell_id]);
                            }

                            //B-A'
                            if (pRB_size[cell_id] > 0 && pSC_size[cell_id_mater] > pSB_size[cell_id_mater])
                            {
                                result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V2_Y_2(pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pSC_size[cell_id_mater], pRB_size[cell_id]);
                            }

                            //A-A';
                            if (pRB_size[cell_id] > 0 && pSB_size[cell_id_mater] > 0)
                            {
                                result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSB_size[cell_id_mater]);
                            }
                            
                            //C-A'
                            if (pRB_size[cell_id] > 0 && pSD_size[cell_id_mater] > pSC_size[cell_id_mater])
                            {
                                result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Approach1_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSC_size[cell_id_mater], pSD_size[cell_id_mater]);
                            }
                            cell_id ++;
                        }

                        //end tile in the row
                        //D-A'
                        if (pRB_size[cell_id] > 0 && pS[cell_id_mater].size() > pSD_size[cell_id_mater])
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V5_Y_2_Approach_Less(pS[cell_id_mater], pR[cell_id], pSD_size[cell_id_mater] , pRB_size[cell_id]);
                        }

                        //B-A'
                        if (pRB_size[cell_id] > 0 && pSC_size[cell_id_mater] > pSB_size[cell_id_mater])
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V2_Y_2(pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pSC_size[cell_id_mater], pRB_size[cell_id]);
                        }

                        //A-A';
                        if (pRB_size[cell_id] > 0 && pSB_size[cell_id_mater] > 0)
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSB_size[cell_id_mater]);
                        }

                        //C-A'
                        if (pRB_size[cell_id] > 0 && pSD_size[cell_id_mater] > pSC_size[cell_id_mater])
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Approach1_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSC_size[cell_id_mater], pSD_size[cell_id_mater]);
                        }
                    }


                    //last row 
                    //get the appropriate tile
                    cell_id = utils::getCellId(xStartCell, yEndCell, runNumPartitionsPerRelation);

                    //first tile in the row
                    //first tile in the row
                    //A-C' 
                    if (pRD_size[cell_id] > pRC_size[cell_id] && pSB_size[cell_id_mater] > 0)
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Approach1_Less(pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pRC_size[cell_id], pRD_size[cell_id]);
                    }

                    ///B-C'
                    if (pRD_size[cell_id] > pRC_size[cell_id] && pSC_size[cell_id_mater] > pSB_size[cell_id_mater])
                    {                                     
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V4_Y_2_Approach_Less( pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pSC_size[cell_id_mater], pRC_size[cell_id], pRD_size[cell_id]);
                    }

                    //D-A'
                    if (pRB_size[cell_id] > 0 && pS[cell_id_mater].size() > pSD_size[cell_id_mater])
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V5_Y_2_Approach_Less(pS[cell_id_mater], pR[cell_id], pSD_size[cell_id_mater] , pRB_size[cell_id]);
                    }

                    //B-A'
                    if (pRB_size[cell_id] > 0 && pSC_size[cell_id_mater] > pSB_size[cell_id_mater])
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V2_Y_2(pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pSC_size[cell_id_mater], pRB_size[cell_id]);
                    }

                    //A-A';
                    if (pRB_size[cell_id] > 0 && pSB_size[cell_id_mater] > 0)
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSB_size[cell_id_mater]);
                    }

                    //C-A'
                    if (pRB_size[cell_id] > 0 && pSD_size[cell_id_mater] > pSC_size[cell_id_mater])
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Approach1_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSC_size[cell_id_mater], pSD_size[cell_id_mater]);
                    }
                    cell_id ++;  

                    //intermidiate tiles in the row
                    for (int i = xStartCell + 1; i < xEndCell ; i ++)
                    {
                         //D-A'
                        if (pRB_size[cell_id] > 0 && pS[cell_id_mater].size() > pSD_size[cell_id_mater])
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V5_Y_2_Approach_Less(pS[cell_id_mater], pR[cell_id], pSD_size[cell_id_mater] , pRB_size[cell_id]);
                        }

                        //B-A'
                        if (pRB_size[cell_id] > 0 && pSC_size[cell_id_mater] > pSB_size[cell_id_mater])
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V2_Y_2(pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pSC_size[cell_id_mater], pRB_size[cell_id]);
                        }

                        //A-A';
                        if (pRB_size[cell_id] > 0 && pSB_size[cell_id_mater] > 0)
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSB_size[cell_id_mater]);
                        }

                        //C-A'
                        if (pRB_size[cell_id] > 0 && pSD_size[cell_id_mater] > pSC_size[cell_id_mater])
                        {
                            result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Approach1_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSC_size[cell_id_mater], pSD_size[cell_id_mater]);
                        }
                        cell_id ++;
                    }
          
                    //last tile in the row
                    //D-A'
                    if (pRB_size[cell_id] > 0 && pS[cell_id_mater].size() > pSD_size[cell_id_mater])
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V5_Y_2_Approach_Less(pS[cell_id_mater], pR[cell_id], pSD_size[cell_id_mater] , pRB_size[cell_id]);
                    }

                    //B-A'
                    if (pRB_size[cell_id] > 0 && pSC_size[cell_id_mater] > pSB_size[cell_id_mater])
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_V2_Y_2(pS[cell_id_mater], pR[cell_id], pSB_size[cell_id_mater], pSC_size[cell_id_mater], pRB_size[cell_id]);
                    }

                    //A-A';
                    if (pRB_size[cell_id] > 0 && pSB_size[cell_id_mater] > 0)
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSB_size[cell_id_mater]);
                    }
                    
                    //C-A'
                    if (pRB_size[cell_id] > 0 && pSD_size[cell_id_mater] > pSC_size[cell_id_mater])
                    {
                        result += twoLayer::spatial_join::unnecessary_redundant_comparisons::Sweep_Rolled_CNT_Y_Approach1_Less(pR[cell_id], pS[cell_id_mater], pRB_size[cell_id], pSC_size[cell_id_mater], pSD_size[cell_id_mater]);
                    }

                    cell_id_mater ++;
                }
            }
            timeJoining = tim.stop();
        
            cout<<"Non-Materialized-strategy2-Spatial-Join-result = "<<result<<"\ttimeSorting = "<< timeSorting<<"\ttimeJoining = " << timeJoining<< "\tfinal = " << timeSorting + timeJoining<<"\tfirst_dataset = " << argv[optind] <<"\tfirst_partition = " << runNumPartitionsPerRelation <<"\tsecond_dataset = " << argv[optind+1] <<"\tsecond_partition = "<< runNumSecondPartitionsPerRelation <<endl;

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
            
            break ;
    }


}