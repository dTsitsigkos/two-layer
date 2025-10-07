#ifndef UTILS_H
#define	UTILS_H
#include "./containers/relation.h"

namespace utils
{
    
    int myQuotient(double numer, double denom) 
    {
        return int(numer/denom + EPS);
    };

    double myRemainder(double numer, double denom, int q) 
    {
        double rem = double(numer - q*denom);

        return ((abs(rem) < EPS) ? 0: rem);
    };

    int getCellId(int x, int y, int numCellsPerDimension) 
    {
        return (y * numCellsPerDimension + x);
    };

    int findReferenceCell(double x, double y, double cellExtent, int numCellsPerDimension) 
    {
        int xInt,yInt;

        xInt = (x + EPS)/cellExtent;
        yInt = (y + EPS)/cellExtent;

        return (yInt * numCellsPerDimension + xInt);
    };

    namespace disk
    {
        void findCoordinates(int id , int &xStart, int &yStart, int runNumPartitionsPerRelation)
        {
            xStart = id%runNumPartitionsPerRelation;
            yStart = id/runNumPartitionsPerRelation;
        }

        Coord MinDist(Point &point, RelationIterator &rec)
        {
            Coord sum = 0.0;
            Coord diff;

            diff = 0.0;
            if ( point.x < rec->xStart) {
                diff = rec->xStart - point.x;
            }
            else if ( point.x > rec->xEnd ){
                diff = point.x-rec->xEnd;
            }
            sum = diff*diff;
            diff = 0.0;

            if ( point.y < rec->yStart ) {
                diff = rec->yStart - point.y;
            }
            else if ( point.y > rec->yEnd ){
                diff = point.y - rec->yEnd;
            }
            sum += diff*diff;
                    
            return sum;
        }

        Coord MaxDist(Point &point, Record &rec)
        {
            Coord sum = 0.0;
            Coord diff1, diff2, diff;
            
            diff1 = abs(point.x - rec.xStart);
            diff2 = abs(point.x - rec.xEnd);
            
            diff = diff1 > diff2 ? diff1 : diff2; 
            sum = diff*diff;
                    
            diff1 = 0.0;
            diff2 = 0.0;
            diff = 0.0;    
            
            diff1 = abs(point.y - rec.yStart);
            diff2 = abs(point.y - rec.yEnd);
            
            diff = diff1 > diff2 ? diff1 : diff2; 
            
            sum += diff*diff; 
                    
            return sum;
        }

        Coord MinDist(Point &point, Record &rec)
        {
            Coord sum = 0.0;
            Coord diff;

            diff = 0.0;
            if ( point.x < rec.xStart) {
                diff = rec.xStart - point.x;
            }
            else if ( point.x > rec.xEnd ){
                diff = point.x-rec.xEnd;
            }
            sum = diff*diff;
            diff = 0.0;

            if ( point.y < rec.yStart ) {
                diff = rec.yStart - point.y;
            }
            else if ( point.y > rec.yEnd ){
                diff = point.y - rec.yEnd;
            }
            sum += diff*diff;
                    
            return sum;
        }
    }

    namespace window
    {
        namespace sort
        {
            void SortXStart(vector<Decompose> *pR, size_t *pRA_size,  int runNumPartitions)
            {
                for (int i = 0; i < runNumPartitions; i++)
                {
                    std::sort(pR[i].begin(), pR[i].begin() + pRA_size[i]);
                }
            };

            void SortXEnd(vector<Decompose> *pR, size_t *pRA_size, size_t *pRB_size, size_t *pRC_size,  int runNumPartitions)
            {
                for (int i = 0; i < runNumPartitions; i++)
                {
                    std::sort(pR[i].begin(), pR[i].begin() + pRA_size[i]);
                    std::sort(pR[i].begin() + pRB_size[i], pR[i].begin() + pRC_size[i]);
                }
            };

            void SortYStart(vector<Decompose> *pR, size_t *pRA_size,  int runNumPartitions)
            {
                for (int i = 0; i < runNumPartitions; i++)
                {
                    std::sort(pR[i].begin(), pR[i].begin() + pRA_size[i]);
                }
            };

            void SortYEnd(vector<Decompose> *pR, size_t *pRA_size, size_t *pRB_size,  int runNumPartitions)
            {
                for (int i = 0; i < runNumPartitions; i++)
                {
                    std::sort(pR[i].begin(), pR[i].begin() + pRA_size[i]);
                    std::sort(pR[i].begin() + pRA_size[i], pR[i].begin() + pRB_size[i]);
                }
            };
        }
    }

    namespace spatial_join
    {
         namespace baseline
         {
            struct myclass 
            {
                bool operator() (Record &i, Record &j) { return (i.yStart < j.yStart);}
            } myobject;

            void SortYStartOneArray(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size,  size_t *pRC_size, size_t *pSC_size,size_t *pRD_size, size_t *pSD_size, int runNumPartitions)
            {
                for (int i = 0; i < runNumPartitions; i++)
                {
                    auto pRStart = pR[i].begin();
                    std::sort(pRStart, pRStart + pRB_size[i], myobject);
                    std::sort(pRStart + pRC_size[i], pRStart + pRD_size[i], myobject);
                    
                    auto pSStart = pS[i].begin();

                    std::sort(pSStart, pSStart + pSB_size[i], myobject);
                    std::sort(pSStart + pSC_size[i], pSStart + pSD_size[i], myobject);
                }         
            };
        }

        namespace redundant_comparisons
        {

            struct myclass2 
            {
                bool operator() (Record &i, Record &j) { return (i.yEnd < j.yEnd);}
            } myobject2;

            void SortYStartOneArrayApproach1(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size,  size_t *pRC_size, size_t *pSC_size,size_t *pRD_size, size_t *pSD_size, int runNumPartitions)
            {
                for (int i = 0; i < runNumPartitions; i++)
                {        
                    auto pRStart = pR[i].begin();
                    auto pSStart = pS[i].begin();

                    if (pRB_size[i] > 0)// RA size
                    {
                        std::sort(pRStart, pRStart + pRB_size[i], spatial_join::baseline::myobject); 
                        std::sort(pSStart + pSB_size[i], pSStart + pSC_size[i], myobject2);
                        std::sort(pSStart + pSC_size[i], pSStart + pSD_size[i], spatial_join::baseline::myobject);
                        std::sort(pSStart + pSD_size[i], pSStart + pS[i].size(), myobject2);
                    }
                    else
                    {
                        if (pRC_size[i] > pRB_size[i])//RBsize
                        {
                            std::sort(pSStart + pSC_size[i], pSStart + pSD_size[i], spatial_join::baseline::myobject);
                        }

                        if (pRD_size[i] > pRC_size[i])//RCsize
                        {
                            std::sort(pSStart + pSB_size[i], pSStart + pSC_size[i], myobject2);
                        }
                    }

                    if ( pSB_size[i] > 0)//SA size
                    {
                        std::sort(pSStart, pSStart + pSB_size[i], spatial_join::baseline::myobject);
                        std::sort(pRStart + pRB_size[i], pRStart + pRC_size[i], myobject2);
                        std::sort(pRStart + pRC_size[i], pRStart + pRD_size[i], spatial_join::baseline::myobject);
                        std::sort(pRStart + pRD_size[i], pRStart + pR[i].size(), myobject2);
                    }
                    else
                    {
                        if (pSC_size[i] > pSB_size[i])//SB size
                        {
                            std::sort(pRStart + pRC_size[i], pRStart + pRD_size[i], spatial_join::baseline::myobject);
                        }
                        if (pSD_size[i] > pSC_size[i])//SC size
                        {
                            std::sort(pRStart + pRB_size[i], pRStart + pRC_size[i], myobject2);
                        }
                    }
                }     
            };

            void SortYStartOneArrayApproach1(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size,  size_t *pRC_size, size_t *pSC_size,size_t *pRD_size, size_t *pSD_size, int runNumPartitions, int runNumSecondPartitions)
            {
                int i;

                for (i = 0; i < runNumSecondPartitions; i++)
                {
                    auto pRStart = pR[i].begin();
                    auto pSStart = pS[i].begin();

                    std::sort(pRStart, pRStart + pRB_size[i], spatial_join::baseline::myobject);
                    std::sort(pRStart + pRB_size[i], pRStart + pRC_size[i], myobject2);
                    std::sort(pRStart + pRC_size[i], pRStart + pRD_size[i], spatial_join::baseline::myobject);
                    std::sort(pRStart + pRD_size[i], pRStart + pR[i].size(), myobject2);

                    std::sort(pSStart, pSStart + pSB_size[i], spatial_join::baseline::myobject);
                    std::sort(pSStart + pSB_size[i], pSStart + pSC_size[i], myobject2);
                    std::sort(pSStart + pSC_size[i], pSStart + pSD_size[i], spatial_join::baseline::myobject);
                    std::sort(pSStart + pSD_size[i], pSStart + pS[i].size(), myobject2);
                }

                for (int j = i; j < runNumPartitions; j++)
                {
                    auto pRStart = pR[j].begin();
                    
                    std::sort(pRStart, pRStart + pRB_size[j], spatial_join::baseline::myobject);
                    std::sort(pRStart + pRB_size[j], pRStart + pRC_size[j], myobject2);
                    std::sort(pRStart + pRC_size[j], pRStart + pRD_size[j], spatial_join::baseline::myobject);
                    std::sort(pRStart + pRD_size[j], pRStart + pR[j].size(), myobject2);
                }   
            };
        }

        namespace all_opts
        {
            struct myobjectAB 
            {
                bool operator() (ABrec2 &i, ABrec2 &j) { return (i.yStart < j.yStart);}
            } myobjectAB;

            struct myobjectAB2 
            {
                bool operator() (ABrec2 &i, ABrec2 &j) {  return (i.yEnd < j.yEnd);}
            } myobjectAB2;

            struct myobjectC 
            {
                bool operator() (Crec2 &i, Crec2 &j) { return (i.yStart < j.yStart);}
            } myobjectC;

            struct myobjectD 
            {
                bool operator() (Drec &i, Drec &j) { return (i.yEnd < j.yEnd);}
            } myobjectD;

            void SortYStartOneArray2(vector<ABrec2> *pRABdec, vector <ABrec2> *pSABdec, vector<Crec2> *pRCdec , vector<Crec2>  *pSCdec , vector<Drec> *pRDdec, vector<Drec> *pSDdec, size_t *pRB_size, size_t *pSB_size, int runNumPartitions)
            {
                for (int i = 0; i < runNumPartitions; i++)
                {
                    auto pRStart = pRABdec[i].begin();
                    auto pSStart = pSABdec[i].begin();

                    if (pRB_size[i] > 0)// RA size
                    {
                        std::sort(pRStart, pRStart + pRB_size[i], myobjectAB); 
                        std::sort(pSStart + pSB_size[i], pSABdec[i].end(), myobjectAB2);
                        std::sort(pSCdec[i].begin() , pSCdec[i].end(), myobjectC);
                        std::sort(pSDdec[i].begin() , pSDdec[i].end(),  myobjectD);
                    }
                    else
                    {
                        if (pRABdec[i].size() > pRB_size[i])//RBsize
                        {
                            std::sort(pSCdec[i].begin() , pSCdec[i].end(), myobjectC);
                        }

                        if (pRCdec[i].size() > 0)//RCsize
                        {
                            std::sort(pSStart + pSB_size[i], pSABdec[i].end(),  myobjectAB2);
                        }
                    }
                    
                    if (pSB_size[i] > 0)//SA size
                    {    
                        std::sort(pSStart, pSStart + pSB_size[i], myobjectAB);
                        std::sort(pRStart + pRB_size[i], pRABdec[i].end(), myobjectAB2);
                        std::sort(pRCdec[i].begin() , pRCdec[i].end(), myobjectC);
                        std::sort(pRDdec[i].begin() , pRDdec[i].end(),  myobjectD);
                    }
                    else
                    {
                        if (pSABdec[i].size() > pSB_size[i])//SB size
                        {
                            std::sort(pRCdec[i].begin() , pRCdec[i].end(), myobjectC);
                        }
                        if (pSCdec[i].size() > 0)//SC size
                        {
                            std::sort(pRStart + pRB_size[i], pRABdec[i].end(),  myobjectAB2);
                        }
                    }
                }
            }
        };   
    }
}
#endif