#include "def.h"
#include "getopt.h"
#include "./containers/relation.h"
#include "partition.h"
#include "two_layer.h"
#include "utils.h"


void usage()
{
    cerr << "NAME" << endl;
    cerr << "       ./twoLayer - the 2-layer algorithm" << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./twoLayer [OPTION]... [FILE1] [FILE2]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       Mandatory arguments" << endl << endl;
    cerr << "       -p" << endl;
    cerr << "              number of partions per dimension" << endl;
    cerr << "       -w" << endl;
    cerr << "              window query" << endl;
    cerr << "       -d" << endl;
    cerr << "              disk query. Option -e should be used when using -d " << endl;
    cerr << "       -e" << endl;
    cerr << "              radius of the disk query" << endl;
    cerr << "       -s" << endl;
    cerr << "              spatial join" << endl;
    cerr << "       Other arguments" << endl << endl;
    cerr << "       -h" << endl;
    cerr << "              display this help message and exit" << endl << endl;
    cerr << "EXAMPLES" << endl;
    cerr << "       Window range query using the 2-layer algorithm with 3000 partitions per dimension." << endl;
    cerr << "              ./two_layer -p 3000 -w dataset_file.csv query_file.csv" << endl;
    cerr << "       Disk range query using the 2-layer algorithm with 3000 partitions and a radius of 0.1" << endl;
    cerr << "              ./two_layer -p 3000 -d -e 0.1 dataset_file.csv query_file.csv" << endl;
    cerr << "       Spatial join using the 2-layer algorithm with 3000 partitions per dimension." << endl;
    cerr << "              ./two_layer -p 3000 -s dataset_file.csv dataset_file2.csv" << endl;
    cerr << "\n" << endl;
    exit(1);
}


int main(int argc, char **argv)
{
    char c;
    int runNumPartitionsPerRelation = -1;
    Timer tim;
    double timeIndexingOrPartitioning = 0 , timeQuery = 0, timeSorting = 0;
    Relation R, S, *pR, *pS;
    size_t *pRA_size, *pSA_size, *pRB_size, *pSB_size, *pRC_size, *pSC_size, *pRD_size, *pSD_size;
    vector <ABrec2> *pRABdec2 , *pSABdec2;
    vector<Crec2> *pRCdec2, *pSCdec2;
    vector<Drec> *pRDdec, *pSDdec;
    int runNumPartitions = -1, queryMethod =-1;
    Coord epsilon = -1.0;
    Coord minX, maxX, minY, maxY, diffX, diffY, maxExtend;
    unsigned long long result = 0;


    while ((c = getopt(argc, argv, "p:i:whe:ds")) != -1)
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
        cerr << "Number of partitions is missing." << endl;
        usage();
    }

    if(queryMethod == -1)
    {
        cerr << "Query method is missing." << endl;
        usage();
    }

    if (queryMethod == DISK_QUERY){
        if (epsilon < 0){
            cout<<"Radius value is missing."<<endl;
            usage();
        }
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

    runNumPartitions = runNumPartitionsPerRelation * runNumPartitionsPerRelation;
    
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

        vector<size_t> results;
        results.reserve(S.size());
        for (int i = 0 ; i < S.size() ; i ++)
        {
            results[i] = 0;
        }

        pR = new Relation[runNumPartitions];

        pRA_size = new size_t[runNumPartitions];
        pRB_size = new size_t[runNumPartitions];
        pRC_size = new size_t[runNumPartitions];
        pRD_size = new size_t[runNumPartitions];
        memset(pRA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRD_size, 0, runNumPartitions*sizeof(size_t));    
        
        tim.start();
        partition::window_disk::PartitionTwoDimensional(R, pR, pRA_size, pRB_size, pRC_size, pRD_size, runNumPartitionsPerRelation);
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
                result += twoLayer::window::Range_Corners(pR[cell_id], s, 0, pR[cell_id].size() );
            }
            else if (yStartCell == yEndCell)
            {
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                //first tile in the row
                result += twoLayer::window::Range_Corners(pR[cell_id],s, 0, pR[cell_id].size());
                cell_id ++;  

                //intermidiate tiles in the row
                for ( int i = xStartCell + 1; i < xEndCell ; i ++)
                {
                    result += twoLayer::window::Range_B_Class(pR[cell_id],s, 0, pRC_size[cell_id]);
                    cell_id ++;   
                }

                //end tile in the row
                result += twoLayer::window::Range_Corners(pR[cell_id],s, 0,  pRC_size[cell_id]);
            }
            else if (xStartCell == xEndCell)
            {
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);
                
                //first tile in the column
                result += twoLayer::window::Range_Corners(pR[cell_id],s, 0, pR[cell_id].size());
                cell_id += runNumPartitionsPerRelation;

                //intermidiate tiles in the column
                for (int i = yStartCell + 1; i < yEndCell ; i += 1)
                {
                    result += twoLayer::window::Range_C_Class(pR[cell_id],s, 0, pRB_size[cell_id]);
                    result += twoLayer::window::Range_C_Class(pR[cell_id],s, pRC_size[cell_id], pRD_size[cell_id]);
                    cell_id += runNumPartitionsPerRelation;
                }

                //ent tile in the column
                result += twoLayer::window::Range_Corners(pR[cell_id],s, 0, pRB_size[cell_id]);
                result += twoLayer::window::Range_Corners(pR[cell_id],s, pRC_size[cell_id], pRD_size[cell_id]);
            }
            else{
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                
                //first row
                //first tile in the row
                result += twoLayer::window::Range_Corners_ABCD(pR[cell_id],s, 0, pR[cell_id].size());
                cell_id ++;  

                //intermidiate tiles in the row
                for (int i = xStartCell + 1; i < xEndCell ; i ++)
                {
                    result += twoLayer::window::Range_Borders_AB(pR[cell_id],s, 0, pRC_size[cell_id]);
                    cell_id ++;   
                }

                //end tile in the row
                result += twoLayer::window::Range_Corners_AB(pR[cell_id],s, 0,  pRC_size[cell_id]);


                //intermidiate rows
                for (int y = yStartCell + 1 ; y < yEndCell ; y += 1)
                {
                    //get the appropriate tile
                    cell_id = utils::getCellId(xStartCell, y, runNumPartitionsPerRelation);

                    //first tile in the row
                    result += twoLayer::window::Range_Borders_AC(pR[cell_id],s, 0, pRB_size[cell_id]);
                    result += twoLayer::window::Range_Borders_AC(pR[cell_id],s, pRC_size[cell_id], pRD_size[cell_id]);
                    cell_id ++;  

                    //intermidiate tiles in the row
                    for ( int i = xStartCell + 1; i < xEndCell ; i ++)
                    {
                        for (auto it = 0; it < pRB_size[cell_id]; it++) 
                        {
                            //result += pR[cell_id][it].id ^ s.id;
                            result++;
                        }
                        cell_id ++;
                    }

                    //end tile in the row
                    result += twoLayer::window::Range_Border_A_Vertically(pR[cell_id],s, 0, pRB_size[cell_id]);
                }

                //last row 
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yEndCell, runNumPartitionsPerRelation);

                //first tile in the row
                result += twoLayer::window::Range_Corners_AC(pR[cell_id],s, 0, pRB_size[cell_id]);
                result += twoLayer::window::Range_Corners_AC(pR[cell_id],s, pRC_size[cell_id], pRD_size[cell_id]);
                cell_id ++;

                //intermidiate tiles in the row
                for (int i = xStartCell + 1; i < xEndCell ; i ++)
                {
                    result += twoLayer::window::Range_Border_A_Horizontally(pR[cell_id],s, 0, pRB_size[cell_id]);
                    cell_id ++;
                }

                //last tile in the row
                result += twoLayer::window::Range_Corners_A(pR[cell_id],s, 0, pRB_size[cell_id]);

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
        
        delete[] pRA_size;
        delete[] pRB_size;
        delete[] pRC_size;
        delete[] pRD_size;
        delete[] pR;
    }
    else if (queryMethod == DISK_QUERY)
    {
        vector<Point> points;
        
        minX = R.minX;
        maxX = R.maxX;
        minY = R.minY;
        maxY = R.maxY;
        diffX = maxX - minX;
        diffY = maxY - minY;
        maxExtend = (diffX<diffY)?diffY:diffX;
        
        R.normalize(minX, maxX, minY, maxY, maxExtend);
        S.normalize(minX, maxX, minY, maxY, maxExtend);


        vector<size_t> results;
        results.reserve(S.size());
        for (int i = 0 ; i < S.size() ; i ++)
        {
            results[i] = 0;
        }


        double xMin = -117;
        double yMin = 33;
        double xMax = -78;
        double yMax = 46;
        
        xMin = Coord(xMin - minX) / maxExtend;
        xMax   = Coord(xMax   - minX) / maxExtend;
        yMin = Coord(yMin - minY) / maxExtend;
        yMax   = Coord(yMax   - minY) / maxExtend;

        Coord world = (xMax - xMin) * (yMax - yMin); 
        
        epsilon = sqrt(epsilon*world/3.14);
        points.resize(S.size());
        S.loadDisk(epsilon,points);

        pR = new Relation[runNumPartitions];
        Coord partitionExtent = 1.0/runNumPartitionsPerRelation;

        pRA_size = new size_t[runNumPartitions];
        pRB_size = new size_t[runNumPartitions];
        pRC_size = new size_t[runNumPartitions];
        pRD_size = new size_t[runNumPartitions];
        memset(pRA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRD_size, 0, runNumPartitions*sizeof(size_t));    
        
        tim.start();
        partition::window_disk::PartitionTwoDimensional(R, pR,pRA_size, pRB_size, pRC_size, pRD_size, runNumPartitionsPerRelation);
        timeIndexingOrPartitioning = tim.stop();

        tim.start();
        for (size_t j = 0; j < S.size(); j++)
        { 
            result = 0;
            auto &s = S[j];
            auto &point = points[j];
            int counter = 0;
                        
            double xStartCell, yStartCell, xEndCell, yEndCell;

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
                result += twoLayer::window::Range_Corners(pR[cell_id], s, 0, pR[cell_id].size() );
            }
            else if (yStartCell == yEndCell)
            {
                
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);
                
                //first tile in the row
                result += twoLayer::disk::Range_Corners(pR[cell_id],s, 0, pR[cell_id].size(), epsilon, point);
                cell_id ++;  

                //intermidiate tiles in the row
                for (int i = xStartCell + 1; i < xEndCell ; i ++)
                {
                    result += twoLayer::disk::Range_B_Class(pR[cell_id],s, 0, pRC_size[cell_id], epsilon, point);
                    cell_id ++;   
                }

                //end tile in the row
                result += twoLayer::disk::Range_Corners(pR[cell_id],s, 0,  pRC_size[cell_id], epsilon, point);
            }
            else if (xStartCell == xEndCell)
            {
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);
                
                //first tile in the column
                result += twoLayer::disk::Range_Corners(pR[cell_id],s, 0, pR[cell_id].size(), epsilon, point);
                cell_id += runNumPartitionsPerRelation;

                //intermidiate tiles in the column
                for (int i = yStartCell + 1; i < yEndCell ; i += 1)
                {
                    //cout<<"AC inside"<<endl;
                    result += twoLayer::disk::Range_C_Class(pR[cell_id],s, 0, pRB_size[cell_id], epsilon, point);
                    result += twoLayer::disk::Range_C_Class(pR[cell_id],s, pRC_size[cell_id], pRD_size[cell_id], epsilon, point);
                    cell_id += runNumPartitionsPerRelation;
                }

                //ent tile in the column
                result += twoLayer::disk::Range_Corners(pR[cell_id],s, 0, pRB_size[cell_id], epsilon, point);
                result += twoLayer::disk::Range_Corners(pR[cell_id],s, pRC_size[cell_id], pRD_size[cell_id], epsilon, point);
            }
            else
            {
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                
                //first row
                //first tile in the row
                result += twoLayer::disk::Range_Corners_ABCD(pR[cell_id],s, 0, pR[cell_id].size(), epsilon, point);
                cell_id ++;  

                //intermidiate tiles in the row
                for (int i = xStartCell + 1; i < xEndCell ; i ++)
                {
                    result += twoLayer::disk::Range_Borders_AB(pR[cell_id],s, 0, pRC_size[cell_id], epsilon, point);
                    cell_id ++;   
                }

                //end tile in the row
                result += twoLayer::disk::Range_Corners_AB(pR[cell_id],s, 0,  pRC_size[cell_id], epsilon, point);


                //intermidiate rows
                for (int y = yStartCell + 1 ; y < yEndCell ; y += 1)
                {
                    //get the appropriate tile
                    cell_id = utils::getCellId(xStartCell, y, runNumPartitionsPerRelation);

                    //first tile in the row
                    result += twoLayer::disk::Range_Borders_AC(pR[cell_id],s, 0, pRB_size[cell_id], epsilon, point);
                    result += twoLayer::disk::Range_Borders_AC(pR[cell_id],s, pRC_size[cell_id], pRD_size[cell_id], epsilon, point);
                    cell_id ++;  

                    //intermidiate tiles in the row
                    for (int i = xStartCell + 1; i < xEndCell ; i ++)
                    {            
                        int x,y;
                        utils::disk::findCoordinates(cell_id , x, y, runNumPartitionsPerRelation);

                        Coord xStart = x *partitionExtent;
                        Coord yStart = y *partitionExtent;

                        Record rec (0, xStart , yStart , xStart + partitionExtent, yStart + partitionExtent);
            
                        if (utils::disk::MaxDist(point, rec) <= epsilon*epsilon)
                        {
                            for (auto it = 0; it < pRB_size[cell_id]; it++) 
                            {
                                result++;
                            }
                        }
                        else
                        {
                            for (auto it = 0; it < pRB_size[cell_id]; it++) 
                            {
                                if (utils::disk::MinDist(point,pR[cell_id][it]) > epsilon*epsilon)
                                {
                                    continue;
                                }
                                result++;
                            }
                        }
                        cell_id ++;
                    }

                    //end tile in the row
                    result += twoLayer::disk::Range_Border_A_Vertically(pR[cell_id],s, 0, pRB_size[cell_id], epsilon, point);
                }

                //last row 
                //get the appropriate tile
                cell_id = utils::getCellId(xStartCell, yEndCell, runNumPartitionsPerRelation);

                //first tile in the row
                result += twoLayer::disk::Range_Corners_AC(pR[cell_id],s, 0, pRB_size[cell_id], epsilon, point);
                result += twoLayer::disk::Range_Corners_AC(pR[cell_id],s, pRC_size[cell_id], pRD_size[cell_id], epsilon, point);
                cell_id ++;

                //intermidiate tiles in the row
                for (int i = xStartCell + 1; i < xEndCell ; i ++)
                {
                    result += twoLayer::disk::Range_Border_A_Horizontally(pR[cell_id],s, 0, pRB_size[cell_id], epsilon, point);
                    cell_id ++;
                }

                //last tile in the row
                result += twoLayer::disk::Range_Corners_A(pR[cell_id],s, 0, pRB_size[cell_id], epsilon, point);

            }
            results[j] = result;
        }

        result = 0;
        for (int  i = 0; i < S.size(); i++)
        {
            result += results[i];
        }

        timeQuery = tim.stop();

        cout<<"Disk Query Results = " <<result<<"\ttimeIndexingOrPartitioning = " << timeIndexingOrPartitioning<<"\ttime joining = "<< timeQuery<<"\tfirst_dataset = "<<argv[optind]  <<"\tsecond_dataset = "<< argv[optind+1] <<"\trunNumPartitionPerReltion = "<< runNumPartitionsPerRelation<<endl;
        
        delete[] pRA_size;
        delete[] pRB_size;
        delete[] pRC_size;
        delete[] pRD_size;
        delete[] pR;

    }  
    else if (queryMethod == MINI_JOIN_UNNECESSARY_REDUNDANT_COMPARISONS_STORE_OPT)
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
        timeQuery = tim.stop();
        
        cout<<"Spatial Join-MINI_JOIN_UNNECESSARY_REDUNDANT_COMPARISONS_STORE_OPT result = "<<result<<"\tpartitioning = "<<runNumPartitionsPerRelation<<"\ttimeIndexingOrPartitioning = "<<timeIndexingOrPartitioning<<"\ttimeSorting = "<< timeSorting<<"\ttimeJoining = " << timeQuery<<"\tfirst_dataset = "<<argv[optind]  <<"\tsecond_dataset = "<< argv[optind+1] <<"\trunNumPartitionPerReltion = "<< runNumPartitionsPerRelation<<endl;

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

