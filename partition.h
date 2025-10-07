#include "containers/relation.h"
#include "utils.h"


namespace partition
{
    namespace window_disk
    {
        //two_layer
        void PartitionUniform(const Relation& R, Relation *pR, size_t *pRA_size, size_t *pRB_size, size_t *pRC_size, size_t *pRD_size, int runNumPartitionsPerRelation)
        {
            int runNumPartitions = runNumPartitionsPerRelation*runNumPartitionsPerRelation;
            Coord partitionExtent = 1.0/runNumPartitionsPerRelation;

            double xStartCell, yStartCell, xEndCell, yEndCell;
            int firstCell, lastCell;
     
            for (size_t i = 0; i < R.size(); i++)
            {
                auto &r = R[i];
                
                // Determine cell for (rec.xStart, rec.yStart)
                xStartCell = utils::myQuotient(r.xStart + EPS, partitionExtent);
                yStartCell = utils::myQuotient(r.yStart + EPS, partitionExtent);
                firstCell = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                if (r.xEnd + EPS >= 1) 
                {
                    xEndCell = runNumPartitionsPerRelation - 1;
                }
                else 
                {
                    xEndCell = utils::myQuotient(r.xEnd + EPS, partitionExtent);
                }

                if (r.yEnd + EPS >= 1) 
                {
                    yEndCell = runNumPartitionsPerRelation - 1;
                }
                else 
                {
                    yEndCell = utils::myQuotient(r.yEnd + EPS, partitionExtent);
                }

                lastCell = utils::getCellId(xEndCell, yEndCell, runNumPartitionsPerRelation);

                int x = 0, y = 0;

                // Put record in cells.
                if (firstCell == lastCell) 
                {
                    pRA_size[firstCell]++;
                }
                else 
                {
                    pRA_size[firstCell]++;

                    int cellNumber;
                    for (int i = xStartCell; i <= xEndCell; i++)
                    {
                        if (i != xStartCell)
                        {
                            cellNumber = utils::getCellId(i, yStartCell, runNumPartitionsPerRelation);
                            pRC_size[cellNumber]++;
                        }
                        for (int j = yStartCell + 1; j <= yEndCell; j ++)
                        {
                            cellNumber = utils::getCellId(i, j, runNumPartitionsPerRelation);
                            if(i == xStartCell)
                            {
                                pRB_size[cellNumber]++;
                            }
                            else
                            {
                                pRD_size[cellNumber]++;
                            }
                        }
                    }
                }
            }

            int counter = 0;
            for (int i = 0; i < runNumPartitions; i++)
            {
                counter = pRA_size[i] + pRB_size[i] + pRC_size[i] + pRD_size[i] ;
                pR[i].resize(counter);

                pRB_size[i] = pRA_size[i] + pRB_size[i];
                pRC_size[i] = counter - pRD_size[i];
                pRD_size[i] = counter;
            }      

            for (size_t i = 0; i < R.size(); i++)
            {
                auto &r = R[i];

                xStartCell = utils::myQuotient(r.xStart + EPS, partitionExtent);
                yStartCell = utils::myQuotient(r.yStart + EPS, partitionExtent);
                firstCell = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                if (r.xEnd + EPS >= 1) 
                {
                    xEndCell = runNumPartitionsPerRelation - 1;
                }
                else 
                {
                    xEndCell = utils::myQuotient(r.xEnd + EPS, partitionExtent);
                }

                if (r.yEnd + EPS >= 1) 
                {
                    yEndCell = runNumPartitionsPerRelation - 1;
                }
                else 
                {
                    yEndCell = utils::myQuotient(r.yEnd + EPS, partitionExtent);
                }
                lastCell = utils::getCellId(xEndCell, yEndCell, runNumPartitionsPerRelation);

                int x = 0 , y = 0;

                // Put record in cells.
                if (firstCell == lastCell) 
                {
                    pRA_size[firstCell] = pRA_size[firstCell] - 1;

                    pR[firstCell][pRA_size[firstCell]].id = r.id;
                    pR[firstCell][pRA_size[firstCell]].xStart = r.xStart;
                    pR[firstCell][pRA_size[firstCell]].yStart = r.yStart;
                    pR[firstCell][pRA_size[firstCell]].xEnd = r.xEnd;
                    pR[firstCell][pRA_size[firstCell]].yEnd = r.yEnd;
                    
                }
                else 
                {
                    pRA_size[firstCell] = pRA_size[firstCell] - 1;

                    pR[firstCell][pRA_size[firstCell]].id = r.id;
                    pR[firstCell][pRA_size[firstCell]].xStart = r.xStart;
                    pR[firstCell][pRA_size[firstCell]].yStart = r.yStart;
                    pR[firstCell][pRA_size[firstCell]].xEnd = r.xEnd;
                    pR[firstCell][pRA_size[firstCell]].yEnd = r.yEnd;

                    int cellNumber;
                    for ( int i = xStartCell; i <= xEndCell; i++ )
                    {
                        if (i != xStartCell)
                        {
                            cellNumber = utils::getCellId(i, yStartCell, runNumPartitionsPerRelation);

                            pRC_size[cellNumber] = pRC_size[cellNumber] - 1;

                            pR[cellNumber][pRC_size[cellNumber]].id = r.id;
                            pR[cellNumber][pRC_size[cellNumber]].xStart = r.xStart;
                            pR[cellNumber][pRC_size[cellNumber]].yStart = r.yStart;
                            pR[cellNumber][pRC_size[cellNumber]].xEnd = r.xEnd;
                            pR[cellNumber][pRC_size[cellNumber]].yEnd = r.yEnd;

                        }
                        for (int j = yStartCell + 1; j <= yEndCell; j ++)
                        {
                            cellNumber = utils::getCellId(i, j, runNumPartitionsPerRelation);
                            if(i == xStartCell)
                            {
                                pRB_size[cellNumber] = pRB_size[cellNumber] - 1 ;

                                pR[cellNumber][pRB_size[cellNumber]].id = r.id;
                                pR[cellNumber][pRB_size[cellNumber]].xStart = r.xStart;
                                pR[cellNumber][pRB_size[cellNumber]].yStart = r.yStart;
                                pR[cellNumber][pRB_size[cellNumber]].xEnd = r.xEnd;
                                pR[cellNumber][pRB_size[cellNumber]].yEnd = r.yEnd;

                            }
                            else
                            {
                                pRD_size[cellNumber] = pRD_size[cellNumber] - 1 ;

                                pR[cellNumber][pRD_size[cellNumber]].id = r.id;
                                pR[cellNumber][pRD_size[cellNumber]].xStart = r.xStart;
                                pR[cellNumber][pRD_size[cellNumber]].yStart = r.yStart;
                                pR[cellNumber][pRD_size[cellNumber]].xEnd = r.xEnd;
                                pR[cellNumber][pRD_size[cellNumber]].yEnd = r.yEnd;
                            }
                        }
                    }
                }
            }
        };

        void PartitionTwoDimensional(Relation& R, Relation *pR, size_t *pRA_size, size_t *pRB_size, size_t *pRC_size, size_t *pRD_size, int runNumPartitionsPerRelation)
        {
            PartitionUniform(R, pR, pRA_size, pRB_size, pRC_size, pRD_size, runNumPartitionsPerRelation);            
        };

        void PartitionUniform(const Relation& R, Relation *pR, size_t *pRA_size, size_t *pRB_size, size_t *pRC_size, size_t *pRD_size, vector<Decompose> *pRXStart, vector<Decompose> *pRXEnd, vector<Decompose> *pRYStart, vector<Decompose> *pRYEnd, int runNumPartitionsPerRelation)
        {
            int runNumPartitions = runNumPartitionsPerRelation*runNumPartitionsPerRelation;
            Coord partitionExtent = 1.0/runNumPartitionsPerRelation;
            double xStartCell, yStartCell, xEndCell, yEndCell;
            int firstCell, lastCell;
            double timepR = 0, timeDecomp = 0;

            for (size_t i = 0; i < R.size(); i++)
            {
                auto &r = R[i];

                // Determine cell for (rec.xStart, rec.yStart)
                xStartCell = utils::myQuotient(r.xStart + EPS, partitionExtent);
                yStartCell = utils::myQuotient(r.yStart + EPS, partitionExtent);
                firstCell = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                // Determine cell for (rec.xEnd, rec.yEnd)
                auto xEnd = utils::myRemainder(r.xEnd, partitionExtent, int(utils::myQuotient(r.xEnd + EPS, partitionExtent)));
                auto yEnd = utils::myRemainder(r.yEnd, partitionExtent, int(utils::myQuotient(r.yEnd + EPS, partitionExtent)));

                if (r.xEnd + EPS >= 1) 
                {
                    xEndCell = runNumPartitionsPerRelation - 1;
                }
                else 
                {
                    xEndCell = utils::myQuotient(r.xEnd + EPS, partitionExtent);
                }

                if (r.yEnd + EPS >= 1) 
                {
                    yEndCell = runNumPartitionsPerRelation - 1;
                }
                else 
                {
                    yEndCell = utils::myQuotient(r.yEnd + EPS, partitionExtent);
                }

                lastCell = utils::getCellId(xEndCell, yEndCell, runNumPartitionsPerRelation);

                int x = 0, y = 0;

                // Put record in cells.
                if (firstCell == lastCell) 
                {
                    pRA_size[firstCell]++;
                }
                else 
                {
                    pRA_size[firstCell]++;
                    int cellNumber;
                    for (int i = xStartCell; i <= xEndCell ; i++)
                    {
                        if (i != xStartCell)
                        {
                            cellNumber = utils::getCellId(i, yStartCell, runNumPartitionsPerRelation);
                            pRC_size[cellNumber]++;
                        }
                        for (int j = yStartCell + 1 ; j <= yEndCell ; j ++)
                        {
                            cellNumber = utils::getCellId(i, j, runNumPartitionsPerRelation);
                            if(i == xStartCell)
                            {
                                pRB_size[cellNumber]++;
                            }
                            else
                            {
                                pRD_size[cellNumber]++;
                            }
                        }
                    }
                }
            }

            int counter = 0;
            for (int i = 0; i < runNumPartitions; i++)
            {
                counter = pRA_size[i] + pRB_size[i] + pRC_size[i] + pRD_size[i] ;
                pR[i].resize(counter);
                
                pRXStart[i].resize(counter);
                pRXEnd[i].resize(counter);
                pRYStart[i].resize(counter);
                pRYEnd[i].resize(counter);

                pRB_size[i] = pRA_size[i] + pRB_size[i];
                pRC_size[i] = counter - pRD_size[i];
                pRD_size[i] = counter;
                
            }

            for (size_t i = 0; i < R.size(); i++)
            {
                auto &r = R[i];

                xStartCell = utils::myQuotient(r.xStart + EPS, partitionExtent);
                yStartCell = utils::myQuotient(r.yStart + EPS, partitionExtent);
                firstCell = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                // Determine cell for (rec.xEnd, rec.yEnd)
                auto xEnd = utils::myRemainder(r.xEnd, partitionExtent, int(utils::myQuotient(r.xEnd + EPS, partitionExtent)));
                auto yEnd = utils::myRemainder(r.yEnd, partitionExtent, int(utils::myQuotient(r.yEnd + EPS, partitionExtent)));

                if (r.xEnd + EPS >= 1) 
                {
                    xEndCell = runNumPartitionsPerRelation - 1;
                }
                else 
                {
                    xEndCell = utils::myQuotient(r.xEnd + EPS, partitionExtent);
                }

                if (r.yEnd + EPS >= 1) 
                {
                    yEndCell = runNumPartitionsPerRelation - 1;
                }
                else 
                {
                    yEndCell = utils::myQuotient(r.yEnd + EPS, partitionExtent);
                }
                lastCell = utils::getCellId(xEndCell, yEndCell, runNumPartitionsPerRelation);

                int x = 0 , y = 0;

                // Put record in cells.
                if (firstCell == lastCell) 
                {
                    pRA_size[firstCell] = pRA_size[firstCell] - 1;

                    pR[firstCell][pRA_size[firstCell]] = r;
                    
                    pRXStart[firstCell][pRA_size[firstCell]].id = r.id;
                    pRXStart[firstCell][pRA_size[firstCell]].value = r.xStart;
                    
                    pRXEnd[firstCell][pRA_size[firstCell]].id = r.id;
                    pRXEnd[firstCell][pRA_size[firstCell]].value = r.xEnd;                            
                    
                    pRYStart[firstCell][pRA_size[firstCell]].id = r.id;
                    pRYStart[firstCell][pRA_size[firstCell]].value = r.yStart;                            
                    
                    pRYEnd[firstCell][pRA_size[firstCell]].id = r.id;
                    pRYEnd[firstCell][pRA_size[firstCell]].value = r.yEnd;
                }
                else 
                {
                    pRA_size[firstCell] = pRA_size[firstCell] - 1;

                    pR[firstCell][pRA_size[firstCell]] = r;
                    
                    pRXStart[firstCell][pRA_size[firstCell]].id = r.id;
                    pRXStart[firstCell][pRA_size[firstCell]].value = r.xStart;
                    
                    pRXEnd[firstCell][pRA_size[firstCell]].id = r.id;
                    pRXEnd[firstCell][pRA_size[firstCell]].value = r.xEnd;                            
                    
                    pRYStart[firstCell][pRA_size[firstCell]].id = r.id;
                    pRYStart[firstCell][pRA_size[firstCell]].value = r.yStart;                            
                    
                    pRYEnd[firstCell][pRA_size[firstCell]].id = r.id;
                    pRYEnd[firstCell][pRA_size[firstCell]].value = r.yEnd;
                    
                    int cellNumber;
                    for (int i = xStartCell; i <= xEndCell; i++)
                    {
                        if (i != xStartCell)
                        {
                            cellNumber = utils::getCellId(i, yStartCell, runNumPartitionsPerRelation);

                            pRC_size[cellNumber] = pRC_size[cellNumber] - 1;

                            pR[cellNumber][pRC_size[cellNumber]] = r;
                            
                            pRXStart[cellNumber][pRC_size[cellNumber]].id = r.id;
                            pRXStart[cellNumber][pRC_size[cellNumber]].value = r.xStart;

                            pRXEnd[cellNumber][pRC_size[cellNumber]].id = r.id;
                            pRXEnd[cellNumber][pRC_size[cellNumber]].value = r.xEnd;                            

                            pRYStart[cellNumber][pRC_size[cellNumber]].id = r.id;
                            pRYStart[cellNumber][pRC_size[cellNumber]].value = r.yStart;                            

                            pRYEnd[cellNumber][pRC_size[cellNumber]].id = r.id;
                            pRYEnd[cellNumber][pRC_size[cellNumber]].value = r.yEnd;
                        }
                        for (int j = yStartCell + 1; j <= yEndCell; j ++)
                        {
                            cellNumber = utils::getCellId(i, j, runNumPartitionsPerRelation);
                            if(i == xStartCell)
                            {
                                pRB_size[cellNumber] = pRB_size[cellNumber] - 1 ;

                                pR[cellNumber][pRB_size[cellNumber]] = r;
                                
                                pRXStart[cellNumber][pRB_size[cellNumber]].id = r.id;
                                pRXStart[cellNumber][pRB_size[cellNumber]].value = r.xStart;

                                pRXEnd[cellNumber][pRB_size[cellNumber]].id = r.id;
                                pRXEnd[cellNumber][pRB_size[cellNumber]].value = r.xEnd;                            

                                pRYStart[cellNumber][pRB_size[cellNumber]].id = r.id;
                                pRYStart[cellNumber][pRB_size[cellNumber]].value = r.yStart;                            

                                pRYEnd[cellNumber][pRB_size[cellNumber]].id = r.id;
                                pRYEnd[cellNumber][pRB_size[cellNumber]].value = r.yEnd;
                            }
                            else
                            {
                                pRD_size[cellNumber] = pRD_size[cellNumber] - 1 ;

                                pR[cellNumber][pRD_size[cellNumber]] = r;
                                
                                pRXStart[cellNumber][pRD_size[cellNumber]].id = r.id;
                                pRXStart[cellNumber][pRD_size[cellNumber]].value = r.xStart;

                                pRXEnd[cellNumber][pRD_size[cellNumber]].id = r.id;
                                pRXEnd[cellNumber][pRD_size[cellNumber]].value = r.xEnd;                            

                                pRYStart[cellNumber][pRD_size[cellNumber]].id = r.id;
                                pRYStart[cellNumber][pRD_size[cellNumber]].value = r.yStart;                            

                                pRYEnd[cellNumber][pRD_size[cellNumber]].id = r.id;
                                pRYEnd[cellNumber][pRD_size[cellNumber]].value = r.yEnd;
                            }
                        }
                    }
                }
            }
        };

        //two_layer_plus
        void PartitionTwoDimensional(Relation& R, Relation *pR, size_t *pRA_size, size_t *pRB_size, size_t *pRC_size, size_t *pRD_size, vector<Decompose> *pRXStart, vector<Decompose> *pRXEnd, vector<Decompose> *pRYStart, vector<Decompose> *pRYEnd, int runNumPartitionsPerRelation)
        {
            PartitionUniform(R, pR, pRA_size, pRB_size, pRC_size, pRD_size, pRXStart, pRXEnd, pRYStart, pRYEnd, runNumPartitionsPerRelation);            
        };
    }

    namespace spatial_join
    {
        namespace baseline
        {    
            void Partition_One_Array(const Relation& R, const Relation& S, Relation *pR, Relation *pS, size_t *pRA_size, size_t *pSA_size, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartitionsPerRelation)
            {
                int runNumPartitions = runNumPartitionsPerRelation*runNumPartitionsPerRelation;
                Coord partitionExtent = 1.0/runNumPartitionsPerRelation;

                double xStartCell, yStartCell, xEndCell, yEndCell;
                int firstCell, lastCell;

                for (size_t i = 0; i < R.size(); i++)
                {
                    auto &r = R[i];

                    xStartCell = utils::myQuotient(r.xStart + EPS, partitionExtent);
                    yStartCell = utils::myQuotient(r.yStart + EPS, partitionExtent);
                    firstCell = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                    if (r.xEnd + EPS >= 1) 
                    {
                        xEndCell = runNumPartitionsPerRelation - 1;
                    }
                    else 
                    {
                        xEndCell = utils::myQuotient(r.xEnd + EPS, partitionExtent);
                    }

                    if (r.yEnd + EPS >= 1)
                    {
                        yEndCell = runNumPartitionsPerRelation - 1;
                    }
                    else 
                    {
                        yEndCell = utils::myQuotient(r.yEnd + EPS, partitionExtent);
                    }

                    lastCell = utils::getCellId(xEndCell, yEndCell, runNumPartitionsPerRelation);

                    int x = 0, y = 0;
                    // Put record in cells.
                    if (firstCell == lastCell) 
                    {
                        pRA_size[firstCell]++;
                    }
                    else 
                    {
                        pRA_size[firstCell]++;
                        int cellNumber;
                        for (int i = xStartCell; i <= xEndCell; i++)
                        {
                            if (i != xStartCell)
                            {
                                cellNumber = utils::getCellId(i, yStartCell, runNumPartitionsPerRelation);
                                pRC_size[cellNumber]++;
                            }
                            for (int j = yStartCell + 1; j <= yEndCell; j ++)
                            {
                                cellNumber = utils::getCellId(i, j, runNumPartitionsPerRelation);
                                if(i == xStartCell)
                                {
                                    pRB_size[cellNumber]++;
                                }
                                else
                                {
                                    pRD_size[cellNumber]++;
                                }
                            }
                        }
                    }
                }

                for (size_t i = 0; i < S.size(); i++)
                {
                    auto &s = S[i];

                    xStartCell = utils::myQuotient(s.xStart + EPS, partitionExtent);
                    yStartCell = utils::myQuotient(s.yStart + EPS, partitionExtent);
                    firstCell = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

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

                    lastCell = utils::getCellId(xEndCell, yEndCell, runNumPartitionsPerRelation);

                    int x = 0, y = 0;
                    // Put record in cells.
                    if (firstCell == lastCell) 
                    {
                        pSA_size[firstCell]++;
                    }
                    else 
                    {
                        pSA_size[firstCell]++;
                        int cellNumber;
                        for (int i = xStartCell; i <= xEndCell; i++)
                        {
                            if (i != xStartCell)
                            {
                                cellNumber = utils::getCellId(i, yStartCell, runNumPartitionsPerRelation);
                                pSC_size[cellNumber]++;
                            }
                            for (int j = yStartCell + 1; j <= yEndCell; j ++){
                                cellNumber = utils::getCellId(i, j, runNumPartitionsPerRelation);
                                if(i == xStartCell)
                                {
                                    pSB_size[cellNumber]++;
                                }
                                else
                                {
                                    pSD_size[cellNumber]++;
                                }
                            }
                        }
                    }
                }     

                int counter = 0;
                for (int i = 0; i < runNumPartitions; i++)
                {
                    counter = pRA_size[i] + pRB_size[i] + pRC_size[i] + pRD_size[i] ;
                    pR[i].resize(counter);  

                    pRB_size[i] = pRA_size[i] + pRB_size[i];
                    pRC_size[i] = counter - pRD_size[i];
                    pRD_size[i] = counter;

                    counter = pSA_size[i] + pSB_size[i] + pSC_size[i] + pSD_size[i] ;       
                    pS[i].resize(counter);       

                    pSB_size[i] = pSA_size[i] + pSB_size[i];
                    pSC_size[i] = counter - pSD_size[i];
                    pSD_size[i] = counter;

                }

                for (size_t i = 0; i < R.size(); i++)
                {
                    auto &r = R[i];

                    xStartCell = utils::myQuotient(r.xStart + EPS, partitionExtent);
                    yStartCell = utils::myQuotient(r.yStart + EPS, partitionExtent);
                    firstCell = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                    if (r.xEnd + EPS >= 1) 
                    {
                        xEndCell = runNumPartitionsPerRelation - 1;
                    }
                    else 
                    {
                        xEndCell = utils::myQuotient(r.xEnd + EPS, partitionExtent);
                    }

                    if (r.yEnd + EPS >= 1) 
                    {
                        yEndCell = runNumPartitionsPerRelation - 1;
                    }
                    else 
                    {
                        yEndCell = utils::myQuotient(r.yEnd + EPS, partitionExtent);
                    }
                    lastCell = utils::getCellId(xEndCell, yEndCell, runNumPartitionsPerRelation);

                    int x = 0 , y = 0;

                    // Put record in cells.
                    if (firstCell == lastCell) 
                    {
                        pRA_size[firstCell] = pRA_size[firstCell] - 1;

                        auto pos = pRA_size[firstCell];

                        pR[firstCell][pos].id = r.id;
                        pR[firstCell][pos].xStart = r.xStart;
                        pR[firstCell][pos].yStart = r.yStart;
                        pR[firstCell][pos].xEnd = r.xEnd;
                        pR[firstCell][pos].yEnd = r.yEnd;
                    }
                    else 
                    {
                        pRA_size[firstCell] = pRA_size[firstCell] - 1;

                        auto pos = pRA_size[firstCell];

                        pR[firstCell][pos].id = r.id;
                        pR[firstCell][pos].xStart = r.xStart;
                        pR[firstCell][pos].yStart = r.yStart;
                        pR[firstCell][pos].xEnd = r.xEnd;
                        pR[firstCell][pos].yEnd = r.yEnd;

                        int cellNumber;
                        for (int i = xStartCell; i <= xEndCell; i++)
                        {
                            if (i != xStartCell)
                            {
                                cellNumber = utils::getCellId(i, yStartCell, runNumPartitionsPerRelation);

                                pRC_size[cellNumber] = pRC_size[cellNumber] - 1;

                                auto pos = pRC_size[cellNumber];

                                pR[cellNumber][pos].id = r.id;
                                pR[cellNumber][pos].xStart = r.xStart;
                                pR[cellNumber][pos].yStart = r.yStart;
                                pR[cellNumber][pos].xEnd = r.xEnd;
                                pR[cellNumber][pos].yEnd = r.yEnd;

                            }
                            for (int j = yStartCell + 1; j <= yEndCell; j ++)
                            {
                                cellNumber = utils::getCellId(i, j, runNumPartitionsPerRelation);
                                if(i == xStartCell)
                                {
                                    pRB_size[cellNumber] = pRB_size[cellNumber] - 1 ;

                                    auto pos = pRB_size[cellNumber];

                                    pR[cellNumber][pos].id = r.id;
                                    pR[cellNumber][pos].xStart = r.xStart;
                                    pR[cellNumber][pos].yStart = r.yStart;
                                    pR[cellNumber][pos].xEnd = r.xEnd;
                                    pR[cellNumber][pos].yEnd = r.yEnd;
                                }
                                else
                                {
                                    pRD_size[cellNumber] = pRD_size[cellNumber] - 1 ;

                                    auto pos = pRD_size[cellNumber];

                                    pR[cellNumber][pos].id = r.id;
                                    pR[cellNumber][pos].xStart = r.xStart;
                                    pR[cellNumber][pos].yStart = r.yStart;
                                    pR[cellNumber][pos].xEnd = r.xEnd;
                                    pR[cellNumber][pos].yEnd = r.yEnd;
                                }
                            }
                        }
                    }
                }

                for (size_t i = 0; i < S.size(); i++)
                {
                    auto &s = S[i];

                    xStartCell = utils::myQuotient(s.xStart + EPS, partitionExtent);
                    yStartCell = utils::myQuotient(s.yStart + EPS, partitionExtent);
                    firstCell = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

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
                    lastCell = utils::getCellId(xEndCell, yEndCell, runNumPartitionsPerRelation);

                    int x = 0 , y = 0;

                    // Put record in cells.
                    if (firstCell == lastCell) 
                    {
                        pSA_size[firstCell] = pSA_size[firstCell] - 1;

                        auto pos = pSA_size[firstCell];

                        pS[firstCell][pos].id = s.id;
                        pS[firstCell][pos].xStart = s.xStart;
                        pS[firstCell][pos].yStart = s.yStart;
                        pS[firstCell][pos].xEnd = s.xEnd;
                        pS[firstCell][pos].yEnd = s.yEnd;
                    }
                    else
                    {
                        pSA_size[firstCell] = pSA_size[firstCell] - 1;

                        auto pos = pSA_size[firstCell];

                        pS[firstCell][pos].id = s.id;
                        pS[firstCell][pos].xStart = s.xStart;
                        pS[firstCell][pos].yStart = s.yStart;
                        pS[firstCell][pos].xEnd = s.xEnd;
                        pS[firstCell][pos].yEnd = s.yEnd;

                        int cellNumber;
                        for (int i = xStartCell; i <= xEndCell; i++)
                        {
                            if (i != xStartCell)
                            {
                                cellNumber = utils::getCellId(i, yStartCell, runNumPartitionsPerRelation);

                                pSC_size[cellNumber] = pSC_size[cellNumber] - 1;

                                auto pos = pSC_size[cellNumber];

                                pS[cellNumber][pos].id = s.id;
                                pS[cellNumber][pos].xStart = s.xStart;
                                pS[cellNumber][pos].yStart = s.yStart;
                                pS[cellNumber][pos].xEnd = s.xEnd;
                                pS[cellNumber][pos].yEnd = s.yEnd;
                            }
                            for (int j = yStartCell + 1; j <= yEndCell; j ++)
                            {
                                cellNumber = utils::getCellId(i, j, runNumPartitionsPerRelation);
                                if(i == xStartCell)
                                {
                                    pSB_size[cellNumber] = pSB_size[cellNumber] - 1;

                                    auto pos = pSB_size[cellNumber];

                                    pS[cellNumber][pos].id = s.id;
                                    pS[cellNumber][pos].xStart = s.xStart;
                                    pS[cellNumber][pos].yStart = s.yStart;
                                    pS[cellNumber][pos].xEnd = s.xEnd;
                                    pS[cellNumber][pos].yEnd = s.yEnd;
                                }
                                else
                                {
                                    pSD_size[cellNumber] = pSD_size[cellNumber] - 1;

                                    auto pos = pSD_size[cellNumber];

                                    pS[cellNumber][pos].id = s.id;
                                    pS[cellNumber][pos].xStart = s.xStart;
                                    pS[cellNumber][pos].yStart = s.yStart;
                                    pS[cellNumber][pos].xEnd = s.xEnd;
                                    pS[cellNumber][pos].yEnd = s.yEnd;
                                }
                            }
                        }
                    }          
                }                    
            };
            
            
            void PartitionTwoDimensional(const Relation& R, const Relation& S, Relation *pR, Relation *pS, size_t *pRA_size, size_t *pSA_size, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size,int runNumPartitionsPerRelation)
            {
                Partition_One_Array(R, S, pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitionsPerRelation);
            };
        }

        namespace all_opts
        {
            void Partition_One_Array_Dec2(const Relation& R, const Relation& S, vector<ABrec2>* pRABdec, vector<ABrec2>* pSABdec,  vector<Crec2> *pRCdec, vector<Crec2> *pSCdec, vector<Drec> *pRDdec, vector<Drec> *pSDdec, size_t * pRA_size, size_t * pSA_size, size_t * pRB_size, size_t * pSB_size, size_t * pRC_size, size_t * pSC_size, size_t * pRD_size, size_t * pSD_size, int runNumPartitionsPerRelation)
            {
                int runNumPartitions = runNumPartitionsPerRelation*runNumPartitionsPerRelation;
                Coord partitionExtent = 1.0/runNumPartitionsPerRelation;

                double xStartCell, yStartCell, xEndCell, yEndCell;
                int firstCell, lastCell;

                for (size_t i = 0; i < R.size(); i++)
                {
                    auto &r = R[i];

                    xStartCell = utils::myQuotient(r.xStart + EPS, partitionExtent);
                    yStartCell = utils::myQuotient(r.yStart + EPS, partitionExtent);
                    firstCell = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                    if (r.xEnd + EPS >= 1) 
                    {
                        xEndCell = runNumPartitionsPerRelation - 1;
                    }
                    else 
                    {
                        xEndCell = utils::myQuotient(r.xEnd + EPS, partitionExtent);
                    }

                    if (r.yEnd + EPS >= 1) 
                    {
                        yEndCell = runNumPartitionsPerRelation - 1;
                    }
                    else 
                    {
                        yEndCell = utils::myQuotient(r.yEnd + EPS, partitionExtent);
                    }

                    lastCell = utils::getCellId(xEndCell, yEndCell, runNumPartitionsPerRelation);

                    int x = 0, y = 0;
                    // Put record in cells.
                    if (firstCell == lastCell) 
                    {
                        pRA_size[firstCell]++;
                    }
                    else 
                    {
                        pRA_size[firstCell]++;
                        int cellNumber;
                        for (int i = xStartCell; i <= xEndCell; i++)
                        {
                            if (i != xStartCell)
                            {
                                cellNumber = utils::getCellId(i, yStartCell, runNumPartitionsPerRelation);
                                pRC_size[cellNumber]++;
                            }
                            for (int j = yStartCell + 1; j <= yEndCell; j ++)
                            {
                                cellNumber = utils::getCellId(i, j, runNumPartitionsPerRelation);
                                if (i == xStartCell)
                                {
                                    pRB_size[cellNumber]++;
                                }
                                else
                                {
                                    pRD_size[cellNumber]++;
                                }
                            }
                        }
                    }
                }


                for (size_t i = 0; i < S.size(); i++)
                {
                    auto &s = S[i];

                    xStartCell = utils::myQuotient(s.xStart + EPS, partitionExtent);
                    yStartCell = utils::myQuotient(s.yStart + EPS, partitionExtent);
                    firstCell = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

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

                    lastCell = utils::getCellId(xEndCell, yEndCell, runNumPartitionsPerRelation);

                    int x = 0, y = 0;

                    // Put record in cells.
                    if (firstCell == lastCell) 
                    {
                        pSA_size[firstCell]++;
                    }
                    else 
                    {
                        pSA_size[firstCell]++;
                        int cellNumber;
                        for (int i = xStartCell; i <= xEndCell; i++)
                        {
                            if (i != xStartCell)
                            {
                                cellNumber = utils::getCellId(i, yStartCell, runNumPartitionsPerRelation);
                                pSC_size[cellNumber]++;
                            }
                            for (int j = yStartCell + 1; j <= yEndCell; j ++)
                            {
                                cellNumber = utils::getCellId(i, j, runNumPartitionsPerRelation);
                                if(i == xStartCell)
                                {
                                    pSB_size[cellNumber]++;
                                }
                                else
                                {
                                    pSD_size[cellNumber]++;
                                }
                            }
                        }
                    }
                } 

                int counter = 0;
                for (int i = 0; i < runNumPartitions; i++)
                {
                    counter = pRA_size[i] + pRB_size[i];
                    pRABdec[i].resize(counter);
                    pRCdec[i].resize(pRC_size[i]);                     
                    pRDdec[i].resize(pRD_size[i]);
                    pRB_size[i] = counter;

                    counter = pSA_size[i] + pSB_size[i];
                    pSABdec[i].resize(counter);
                    pSCdec[i].resize(pSC_size[i]);                      
                    pSDdec[i].resize(pSD_size[i]);
                    pSB_size[i] = counter;
                }

                for (size_t i = 0; i < R.size(); i++)
                {
                    auto &r = R[i];

                    xStartCell = utils::myQuotient(r.xStart + EPS, partitionExtent);
                    yStartCell = utils::myQuotient(r.yStart + EPS, partitionExtent);
                    firstCell = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

                    if (r.xEnd + EPS >= 1) 
                    {
                        xEndCell = runNumPartitionsPerRelation - 1;
                    }
                    else 
                    {
                        xEndCell = utils::myQuotient(r.xEnd + EPS, partitionExtent);
                    }

                    if (r.yEnd + EPS >= 1) 
                    {
                        yEndCell = runNumPartitionsPerRelation - 1;
                    }
                    else 
                    {
                        yEndCell = utils::myQuotient(r.yEnd + EPS, partitionExtent);
                    }
                    lastCell = utils::getCellId(xEndCell, yEndCell, runNumPartitionsPerRelation);

                    int x = 0 , y = 0;

                    // Put record in cells.
                    if (firstCell == lastCell) 
                    {
                        pRA_size[firstCell] = pRA_size[firstCell] - 1;

                        auto pos = pRA_size[firstCell];

                        pRABdec[firstCell][pos].id = r.id;
                        pRABdec[firstCell][pos].xStart = r.xStart;
                        pRABdec[firstCell][pos].yStart = r.yStart;
                        pRABdec[firstCell][pos].xEnd = r.xEnd;
                        pRABdec[firstCell][pos].yEnd = r.yEnd;   
                    }
                    else 
                    {
                        pRA_size[firstCell] = pRA_size[firstCell] - 1;

                        auto pos = pRA_size[firstCell];

                        pRABdec[firstCell][pos].id = r.id;
                        pRABdec[firstCell][pos].xStart = r.xStart;
                        pRABdec[firstCell][pos].yStart = r.yStart;
                        pRABdec[firstCell][pos].xEnd = r.xEnd;
                        pRABdec[firstCell][pos].yEnd = r.yEnd;

                        int cellNumber;
                        for (int i = xStartCell; i <= xEndCell; i++)
                        {
                            if (i != xStartCell)
                            {
                                cellNumber = utils::getCellId(i, yStartCell, runNumPartitionsPerRelation);

                                pRC_size[cellNumber] = pRC_size[cellNumber] - 1;

                                auto pos = pRC_size[cellNumber];

                                pRCdec[cellNumber][pos].id = r.id;
                                pRCdec[cellNumber][pos].yStart = r.yStart;
                                pRCdec[cellNumber][pos].xEnd = r.xEnd;
                                pRCdec[cellNumber][pos].yEnd = r.yEnd;
                            }
                            for (int j = yStartCell + 1; j <= yEndCell; j ++)
                            {
                                cellNumber = utils::getCellId(i, j, runNumPartitionsPerRelation);
                                if(i == xStartCell)
                                {
                                    pRB_size[cellNumber] = pRB_size[cellNumber] - 1 ;

                                    auto pos =  pRB_size[cellNumber];

                                    pRABdec[cellNumber][pos].id = r.id;
                                    pRABdec[cellNumber][pos].xStart = r.xStart;
                                    pRABdec[cellNumber][pos].yStart = r.yStart;
                                    pRABdec[cellNumber][pos].xEnd = r.xEnd;
                                    pRABdec[cellNumber][pos].yEnd = r.yEnd;
                                }
                                else
                                {
                                    pRD_size[cellNumber] = pRD_size[cellNumber] - 1;

                                    auto pos = pRD_size[cellNumber];

                                    pRDdec[cellNumber][pos].id = r.id;
                                    pRDdec[cellNumber][pos].xEnd = r.xEnd;
                                    pRDdec[cellNumber][pos].yEnd = r.yEnd;                                    
                                }
                            }
                        }
                    }
                }

                for (size_t i = 0; i < S.size(); i++)
                {
                    auto &s = S[i];

                    xStartCell = utils::myQuotient(s.xStart + EPS, partitionExtent);
                    yStartCell = utils::myQuotient(s.yStart + EPS, partitionExtent);
                    firstCell = utils::getCellId(xStartCell, yStartCell, runNumPartitionsPerRelation);

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
                    lastCell = utils::getCellId(xEndCell, yEndCell, runNumPartitionsPerRelation);

                    int x = 0 , y = 0;

                    // Put record in cells.
                    if (firstCell == lastCell) 
                    {
                        pSA_size[firstCell] = pSA_size[firstCell] - 1;

                        auto pos = pSA_size[firstCell];

                        pSABdec[firstCell][pos].id = s.id;
                        pSABdec[firstCell][pos].xStart = s.xStart;
                        pSABdec[firstCell][pos].yStart = s.yStart;
                        pSABdec[firstCell][pos].xEnd = s.xEnd;
                        pSABdec[firstCell][pos].yEnd = s.yEnd;
                    }
                    else 
                    {
                        pSA_size[firstCell] = pSA_size[firstCell] - 1;

                        auto pos = pSA_size[firstCell];

                        pSABdec[firstCell][pos].id = s.id;
                        pSABdec[firstCell][pos].xStart = s.xStart;
                        pSABdec[firstCell][pos].yStart = s.yStart;
                        pSABdec[firstCell][pos].xEnd = s.xEnd;
                        pSABdec[firstCell][pos].yEnd = s.yEnd;

                        int cellNumber;
                        for (int i = xStartCell; i <= xEndCell; i++)
                        {
                            if (i != xStartCell)
                            {
                                cellNumber = utils::getCellId(i, yStartCell, runNumPartitionsPerRelation);

                                pSC_size[cellNumber] = pSC_size[cellNumber] - 1;

                                auto pos = pSC_size[cellNumber];

                                pSCdec[cellNumber][pos].id = s.id;
                                pSCdec[cellNumber][pos].yStart = s.yStart;
                                pSCdec[cellNumber][pos].xEnd = s.xEnd;
                                pSCdec[cellNumber][pos].yEnd = s.yEnd;
                            }
                            for (int j = yStartCell + 1; j <= yEndCell; j ++)
                            {
                                cellNumber = utils::getCellId(i, j, runNumPartitionsPerRelation);
                                if(i == xStartCell)
                                {
                                    pSB_size[cellNumber] = pSB_size[cellNumber] - 1 ;

                                    auto pos = pSB_size[cellNumber];

                                    pSABdec[cellNumber][pos].id = s.id;
                                    pSABdec[cellNumber][pos].xStart = s.xStart;
                                    pSABdec[cellNumber][pos].yStart = s.yStart;
                                    pSABdec[cellNumber][pos].xEnd = s.xEnd;
                                    pSABdec[cellNumber][pos].yEnd = s.yEnd;
                                }
                                else
                                {
                                    pSD_size[cellNumber] = pSD_size[cellNumber] - 1;

                                    auto pos = pSD_size[cellNumber];

                                    pSDdec[cellNumber][pos].id = s.id;
                                    pSDdec[cellNumber][pos].xEnd = s.xEnd;
                                    pSDdec[cellNumber][pos].yEnd = s.yEnd;                                    
                                }
                            }
                        }
                    }          
                }  
            }
            
            void PartitionTwoDimensionalDec2(const Relation& R, const Relation& S, vector<ABrec2>* pRABdec, vector<ABrec2>* pSABdec,  vector<Crec2> *pRCdec, vector<Crec2> *pSCdec, vector<Drec> *pRDdec, vector<Drec> *pSDdec, size_t * pRA_size, size_t * pSA_size, size_t * pRB_size, size_t * pSB_size, size_t * pRC_size, size_t * pSC_size, size_t * pRD_size, size_t * pSD_size, int runNumPartitionsPerRelation){
                Partition_One_Array_Dec2(R, S, pRABdec, pSABdec, pRCdec, pSCdec, pRDdec, pSDdec, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitionsPerRelation);
            };
        }
    }
}
