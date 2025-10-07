#include "def.h"
#include "getopt.h"
#include "./containers/relation.h"
#include "partition.h"
#include "two_layer_plus.h"

void usage()
{
    cerr << "NAME" << endl;
    cerr << "       ./two_layer_plus - range query using the 2-layer plus algorithm" << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./two_layer_plus [OPTION]... [FILE1] [FILE2]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       Mandatory arguments" << endl << endl;
    cerr << "       -p" << endl;
    cerr << "              number of partions per dimension" << endl;
    cerr << "       -w" << endl;
    cerr << "              window query" << endl;
    cerr << "       -d" << endl;
    cerr << "              disk query. Option -e should be used when using -d (Not supported)" << endl;
    cerr << "       -e" << endl;
    cerr << "              radius of the disk query" << endl;
    cerr << "       -s" << endl;
    cerr << "              spatial join (Not supported)" << endl;
    cerr << "       Other arguments" << endl << endl;
    cerr << "       -h" << endl;
    cerr << "              display this help message and exit" << endl << endl;
    cerr << "EXAMPLES" << endl;
    cerr << "       Window range query using the 2-layer-plus algorithm with 3000 partitions per dimension." << endl;
    cerr << "              ./two_layer_plus -p 3000 -w dataset_file.csv query_file.csv" << endl;
    cerr << "\n" << endl;
    exit(1);
}


int main(int argc, char **argv)
{
    char c;
    int runNumPartitionsPerRelation = -1, runProcessingMethod = -1;
    Timer tim;
    double timeIndexingOrPartitioning = 0 , timeQuery = 0;
    Relation R, S, *pR, *pS;
    size_t *pRA_size, *pRB_size, *pRC_size, *pRD_size, *pS_size;
    int runNumPartitions = -1, queryMethod =-1;
    Coord minX, maxX, minY, maxY, diffX, diffY, maxExtend;
    vector<Coord> indexR, indexS;
    vector<Decompose> *pRXStart , *pRXEnd, *pRYStart, *pRYEnd;
    unsigned long long result = 0;
    Coord epsilon = -1.0;

    while ((c = getopt(argc, argv, "p:i:whe:d")) != -1)
    {
        switch (c)
        {
            case 'p':
                runNumPartitionsPerRelation = atoi(optarg);
                break;
            case 'w':
                queryMethod = WINDOW_QUERY;
                break;
            case 'e':
                epsilon = atof(optarg);
                break;
            case 'd':
                queryMethod = DISK_QUERY;
                break;
             case 's':
                queryMethod = MINI_JOIN_UNNECESSARY_REDUNDANT_COMPARISONS_STORE_OPT;
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
        cerr << "Number of partitions is missing" << endl;
        usage();
    }

    if(queryMethod == -1)
    {
        cerr << "Query method is missing" << endl;
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

    
    if (queryMethod == WINDOW_QUERY)
    {
        minX = min(R.minX, S.minX);
        maxX = max(R.maxX, S.maxX);
        minY = min(R.minY, S.minY);
        maxY = max(R.maxY, S.maxY);
        diffX = maxX - minX;
        diffY = maxY - minY;
        maxExtend = (diffX<diffY)?diffY:diffX;

        R.normalize(minX, maxX, minY, maxY, maxExtend);
        S.normalize(minX, maxX, minY, maxY, maxExtend);

        indexR.resize(4*R.size());
        indexS.resize(4*S.size());

        vector<size_t> results;
        results.reserve(S.size());
        for (int i = 0 ; i < S.size() ; i ++)
        {
            results[i] = 0;
        }
        
        int counter = 0;
        for (int i = 0 ; i < R.size(); i ++)
        {
            indexR[counter] = R[i].xStart;
            counter++;
            indexR[counter] = R[i].xEnd;
            counter++;
            indexR[counter] = R[i].yStart;
            counter++;
            indexR[counter] = R[i].yEnd;
            counter++;
        }

        counter = 0;
        for (int i = 0 ; i < S.size() ; i ++)
        {
            indexS[counter] = S[i].xStart;
            counter++;
            indexS[counter] = S[i].xEnd;
            counter++;
            indexS[counter] = S[i].yStart;
            counter++;
            indexS[counter] = S[i].yEnd;
            counter++;
        }

        runNumPartitions = runNumPartitionsPerRelation * runNumPartitionsPerRelation;

        pR = new Relation[runNumPartitions];

        pRXStart = new vector<Decompose>[runNumPartitions];
        pRXEnd = new vector<Decompose>[runNumPartitions];
        pRYStart = new vector<Decompose>[runNumPartitions];
        pRYEnd = new vector<Decompose>[runNumPartitions];

        pRA_size = new size_t[runNumPartitions];
        pRB_size = new size_t[runNumPartitions];
        pRC_size = new size_t[runNumPartitions];
        pRD_size = new size_t[runNumPartitions];
        memset(pRA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRD_size, 0, runNumPartitions*sizeof(size_t));    
        
        tim.start();
        partition::window_disk::PartitionTwoDimensional(R, pR, pRA_size, pRB_size, pRC_size, pRD_size, pRXStart, pRXEnd,  pRYStart, pRYEnd, runNumPartitionsPerRelation);

        utils::window::sort::SortXStart(pRXStart, pRB_size , runNumPartitions);
        utils::window::sort::SortXEnd(pRXEnd, pRB_size , pRC_size, pRD_size, runNumPartitions);
        utils::window::sort::SortYStart(pRYStart, pRB_size , runNumPartitions);
        utils::window::sort::SortYEnd(pRYEnd, pRB_size ,pRC_size, runNumPartitions);
        
        timeIndexingOrPartitioning = tim.stop();

        tim.start();
        for (size_t j = 0; j < S.size(); j++)
        { 
            result = 0;
            auto &s = S[j];
            int counter = 0;
                  
            double xStartCell, yStartCell, xEndCell, yEndCell;

            int runNumPartitions = runNumPartitionsPerRelation*runNumPartitionsPerRelation;
            Coord partitionExtent = 1.0/runNumPartitionsPerRelation;
            xStartCell = utils::myQuotient(s.xStart + EPS, partitionExtent);
            yStartCell = utils::myQuotient(s.yStart + EPS, partitionExtent);
            auto xEnd = utils::myRemainder(s.xEnd, partitionExtent, int(utils::myQuotient(s.xEnd + EPS, partitionExtent)));
            auto yEnd = utils::myRemainder(s.yEnd, partitionExtent, int(utils::myQuotient(s.yEnd + EPS, partitionExtent)));
            

            if (s.xEnd + EPS >= 1) 
            {
                xEndCell = runNumPartitionsPerRelation - 1;
            }
            else 
            {
                xEndCell = utils::myQuotient(s.xEnd + EPS, partitionExtent);
            }

            if (s.yEnd + EPS >= 1) 
            {
                yEndCell = runNumPartitionsPerRelation - 1;
            }
            else 
            {
                yEndCell = utils::myQuotient(s.yEnd + EPS, partitionExtent);
            }

            int cell_id = -1;

            if (xStartCell == xEndCell && yStartCell == yEndCell)
            {
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                //one tile
                result += twoLayerPlus::window::Range_Corners(pR[cell_id], s, 0, pR[cell_id].size() );
            }
            else if (yStartCell == yEndCell)
            {
                //cout<<"edwwww"<<endl;
                
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);
                
                //first tile in the row
                result += twoLayerPlus::window::Range_Corners(pR[cell_id],s, 0, pR[cell_id].size());
                cell_id ++;  

                //intermidiate tiles in the row
                for ( int i = xStartCell + 1; i < xEndCell ; i ++){
                    result += twoLayerPlus::window::Range_B_Class(pRYEnd[cell_id],indexR,s, 0, pRB_size[cell_id]);
                    result += twoLayerPlus::window::RangeWithBinarySearch(pRYEnd[cell_id], pRB_size[cell_id], pRC_size[cell_id], s.yStart);

                    cell_id ++;   
                }

                //end tile in the row
                result += twoLayerPlus::window::Range_Corners(pR[cell_id],s, 0,  pRC_size[cell_id]);
            }
            else if (xStartCell == xEndCell)
            {
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);
                
                //first tile in the column
                result += twoLayerPlus::window::Range_Corners(pR[cell_id],s, 0, pR[cell_id].size());
                cell_id += runNumPartitionsPerRelation;

                //intermidiate tiles in the column
                for ( int i = yStartCell + 1; i < yEndCell ; i += 1){
                    //cout<<"AC inside"<<endl;
                    result += twoLayerPlus::window::Range_C_Class(pRXEnd[cell_id], indexR, s, 0, pRB_size[cell_id]);
                    result += twoLayerPlus::window::RangeWithBinarySearch(pRXEnd[cell_id], pRC_size[cell_id], pRD_size[cell_id], s.xStart);
                    cell_id += runNumPartitionsPerRelation;
                }

                //ent tile in the column
                result += twoLayerPlus::window::Range_Corners(pR[cell_id],s, 0, pRB_size[cell_id]);
                result += twoLayerPlus::window::Range_Corners(pR[cell_id],s, pRC_size[cell_id], pRD_size[cell_id]);
            }
            else
            {
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                
                //first row
                //first tile in the row
                result += twoLayerPlus::window::Range_Corners_ABCD(pR[cell_id],s, 0, pR[cell_id].size());
                cell_id ++;  

                //intermidiate tiles in the row
                for (int i = xStartCell + 1; i < xEndCell ; i ++)
                {
                    //result += twoLayerPlus::window::Range_Borders_AB(pR[cell_id],s, 0, pRC_size[cell_id]);
                    result += twoLayerPlus::window::RangeWithBinarySearch(pRYEnd[cell_id], 0, pRB_size[cell_id],s.yStart);
                    result += twoLayerPlus::window::RangeWithBinarySearch(pRYEnd[cell_id], pRB_size[cell_id], pRC_size[cell_id],s.yStart);

                    cell_id ++;   
                }

                //end tile in the row
                result += twoLayerPlus::window::Range_Corners_AB(pR[cell_id],s, 0,  pRC_size[cell_id]);


                //intermidiate rows
                for (int y = yStartCell + 1 ; y < yEndCell ; y += 1)
                {
                    //get the appropriate tile
                    cell_id = utils::getCellId(xStartCell, y, runNumPartitionsPerRelation);

                    result += twoLayerPlus::window::RangeWithBinarySearch(pRXEnd[cell_id], 0, pRB_size[cell_id],s.xStart);
                    result += twoLayerPlus::window::RangeWithBinarySearch(pRXEnd[cell_id], pRC_size[cell_id], pRD_size[cell_id], s.xStart);

                    
                    cell_id ++;  

                    //intermidiate tiles in the row
                    for (int i = xStartCell + 1; i < xEndCell ; i ++)
                    {
                        for (auto it = 0; it < pRB_size[cell_id]; it++) 
                        {
                            //result += pR[cell_id][it].id ^ s.id;
                            result++;
                        }
                        cell_id ++;
                    }

                    //end tile in the row
                    result += twoLayerPlus::window::RangeWithBinarySearch2(pRXStart[cell_id], 0, pRB_size[cell_id],s.xEnd);
                }

                //last row 
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yEndCell, runNumPartitionsPerRelation);

                //first tile in the row
                result += twoLayerPlus::window::Range_Corners_AC(pR[cell_id],s, 0, pRB_size[cell_id]);
                result += twoLayerPlus::window::Range_Corners_AC(pR[cell_id],s, pRC_size[cell_id], pRD_size[cell_id]);
                cell_id ++;

                //intermidiate tiles in the row
                for (int i = xStartCell + 1; i < xEndCell ; i ++)
                {
                    result += twoLayerPlus::window::RangeWithBinarySearch2(pRYStart[cell_id], 0, pRB_size[cell_id],s.yEnd);
                    cell_id ++;
                }

                //last tile in the row
                result += twoLayerPlus::window::Range_Corners_A(pR[cell_id],s, 0, pRB_size[cell_id]);
            }
            results[j] = result;
        }

        result = 0;
        for ( int  i = 0; i < S.size(); i++)
        {
            result += results[i];
        }

        timeQuery = tim.stop();
    
        cout<<"Range Query Results = " <<result<<"\ttimeIndexingOrPartitioning = " << timeIndexingOrPartitioning<<"\ttime joining = "<< timeQuery<<"\tfirst_dataset = "<<argv[optind]  <<"\tsecond_dataset = "<< argv[optind+1] <<"\trunNumPartitionPerReltion = "<< runNumPartitionsPerRelation<<endl;
    }
    else if (queryMethod = DISK_QUERY){
        cout<<"No implemenations for disk query!!!"<<endl;
    }   
    else if (queryMethod = MINI_JOIN_UNNECESSARY_REDUNDANT_COMPARISONS_STORE_OPT){
        cout<<"No implemenations for spatial join!!!"<<endl;
    }      

    delete[] pRA_size;
    delete[] pRB_size;
    delete[] pRC_size;
    delete[] pRD_size;
    delete[] pR;
} 

