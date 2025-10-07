#ifndef TWO_LAYER_H
#define	TWO_LAYER_H
#include "utils.h"

namespace twoLayer
{ 
    namespace window
    {
        inline unsigned long long InternalLoop_Range_Corners(RelationIterator firstFS, Record &S, RelationIterator lastFS)
        {             
            unsigned long long result = 0;    
            auto pivot = firstFS;
            while (pivot < lastFS)
            {
                if (S.yStart > pivot->yEnd || S.yEnd < pivot->yStart || S.xStart > pivot->xEnd || S.xEnd < pivot->xStart)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot ++;
            }

            return result;
        };

        inline unsigned long long Range_Corners(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners(r, S, lastR);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Corners_A(RelationIterator firstFS,Record &S, RelationIterator lastFS)
        {           
            unsigned long long result = 0;      
            auto pivot = firstFS;
            while (pivot < lastFS)
            {
                if (S.yEnd < pivot->yStart || S.xEnd < pivot->xStart)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot ++;
            }

            return result;
        };

        inline unsigned long long Range_Corners_A(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners_A(r, S, lastR);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Corners_AB(RelationIterator firstFS, Record &S, RelationIterator lastFS)
        {      
            unsigned long long result = 0;           
            auto pivot = firstFS;
            while (pivot < lastFS)
            {
                if (S.yStart > pivot->yEnd || S.xEnd < pivot->xStart)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot ++;
            }

            return result;
        };

        inline unsigned long long Range_Corners_AB(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners_AB(r, S, lastR);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Corners_AC(RelationIterator firstFS,Record &S, RelationIterator lastFS)
        {         
            unsigned long long result = 0;        
            auto pivot = firstFS;
            while (pivot < lastFS)
            {
                if (S.yEnd < pivot->yStart || S.xStart > pivot->xEnd)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot ++;
            }

            return result;
        };

        inline unsigned long long Range_Corners_AC(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners_AC(r, S, lastR);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Corners_ABCD(RelationIterator firstFS, Record &S, RelationIterator lastFS)
        {      
            unsigned long long result = 0;           
            auto pivot = firstFS;
            while (pivot < lastFS)
            {
                if (S.xStart > pivot->xEnd || S.yStart > pivot->yEnd)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot ++;
            }

            return result;
        };

        inline unsigned long long Range_Corners_ABCD(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners_ABCD(r, S, lastR);

            return result;
        };

        inline unsigned long long InternalLoop_Range_B_Class(RelationIterator firstFS, Record &S, RelationIterator lastFS)
        {     
            unsigned long long result = 0;            
            auto pivot = firstFS;
            while (pivot < lastFS)
            {    
                if (S.yEnd < pivot->yStart || S.yStart > pivot->yEnd)
                {
                    pivot++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot++;
            }

            return result;
        };

        inline unsigned long long Range_B_Class(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;

            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_B_Class(r, S, lastR);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Border_A_Horizontally(RelationIterator firstFS, Record &S, RelationIterator lastFS)
        {   
            unsigned long long result = 0;              
            auto pivot = firstFS;

            while (pivot < lastFS)
            {    
                if (S.yEnd < pivot->yStart)
                {
                    pivot++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot++;
            }

            return result;
        };

        inline unsigned long long Range_Border_A_Horizontally(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;

            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Border_A_Horizontally(r, S, lastR);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Borders_AB(RelationIterator firstFS, Record &S, RelationIterator lastFS)
        {    
            unsigned long long result = 0;             
            auto pivot = firstFS;

            while (pivot < lastFS)
            {    
                if (S.yStart > pivot->yEnd)
                {
                    pivot++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot++;
            }

            return result;
        };

        inline unsigned long long Range_Borders_AB(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Borders_AB(r, S, lastR);

            return result;
        };

        inline unsigned long long InternalLoop_Range_C_Class(RelationIterator firstFS, Record &S, RelationIterator lastFS)
        { 
            unsigned long long result = 0;
            auto pivot = firstFS;

            while (pivot < lastFS)
            {
                if (S.xStart > pivot->xEnd || S.xEnd < pivot->xStart)
                {
                    pivot++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot++;
            }

            return result;
        };

        inline unsigned long long Range_C_Class(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_C_Class(r, S, lastR);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Border_A_Vertically(RelationIterator firstFS, Record &S, RelationIterator lastFS)
        { 
            unsigned long long result = 0;
            auto pivot = firstFS;

            while (pivot < lastFS)
            {
                if (S.xEnd < pivot->xStart)
                {
                    pivot++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;  
                pivot++;
            }

            return result;
        };

        inline unsigned long long Range_Border_A_Vertically(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Border_A_Vertically(r, S, lastR);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Borders_AC(RelationIterator firstFS,Record &S, RelationIterator lastFS)
        { 
            unsigned long long result = 0;
            auto pivot = firstFS;

            while (pivot < lastFS)
            {
                if (S.xStart > pivot->xEnd)
                {
                    pivot++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot++;
            }

            return result;
        };

        inline unsigned long long Range_Borders_AC(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Borders_AC(r, S, lastR);

            return result;
        };

    }
    
    namespace disk{

        inline unsigned long long InternalLoop_Range_Corners(RelationIterator firstFS,Record &S, RelationIterator lastFS, Coord epsilon, Point point)
        {             
            unsigned long long result = 0;    
            auto pivot = firstFS;
            
            while (pivot < lastFS)
            {
                if (S.yStart > pivot->yEnd || S.yEnd < pivot->yStart || S.xStart > pivot->xEnd || S.xEnd < pivot->xStart)
                {
                    pivot ++;
                    continue;
                }

                if (utils::disk::MinDist(point,pivot) > epsilon*epsilon)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot ++;
            }

            return result;
        };

        inline unsigned long long Range_Corners(Relation &pR, Record &S, size_t startR, size_t endR, Coord epsilon, Point point)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners(r, S, lastR, epsilon, point);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Corners_A(RelationIterator firstFS, Record &S, RelationIterator lastFS, Coord epsilon, Point point)
        {           
            unsigned long long result = 0;      
            auto pivot = firstFS;
            
            while (pivot < lastFS)
            {
                if (S.yEnd < pivot->yStart || S.xEnd < pivot->xStart)
                {
                    pivot ++;
                    continue;
                }

                if (utils::disk::MinDist(point,pivot) > epsilon*epsilon)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot ++;
            }

            return result;
        };

        inline unsigned long long Range_Corners_A(Relation &pR, Record &S, size_t startR, size_t endR, Coord epsilon, Point point)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners_A(r, S, lastR, epsilon, point);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Corners_AB(RelationIterator firstFS, Record &S, RelationIterator lastFS, Coord epsilon, Point point)
        {      
            unsigned long long result = 0;           
            auto pivot = firstFS;
            
            while (pivot < lastFS)
            {
                if (S.yStart > pivot->yEnd || S.xEnd < pivot->xStart)
                {
                    pivot ++;
                    continue;
                }

                if (utils::disk::MinDist(point,pivot) > epsilon*epsilon)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot ++;
            }

            return result;
        };

        inline unsigned long long Range_Corners_AB(Relation &pR, Record &S, size_t startR, size_t endR, Coord epsilon, Point point)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners_AB(r, S, lastR, epsilon, point);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Corners_AC(RelationIterator firstFS, Record &S, RelationIterator lastFS, Coord epsilon, Point point)
        {         
            unsigned long long result = 0;        
            auto pivot = firstFS;
           
            while (pivot < lastFS)
            {
                if(S.yEnd < pivot->yStart || S.xStart > pivot->xEnd)
                {
                    pivot ++;
                    continue;
                }

                if (utils::disk::MinDist(point,pivot) > epsilon*epsilon)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot ++;
            }

            return result;
        };

        inline unsigned long long Range_Corners_AC(Relation &pR, Record &S, size_t startR, size_t endR, Coord epsilon, Point point)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners_AC(r, S, lastR, epsilon, point);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Corners_ABCD(RelationIterator firstFS, Record &S, RelationIterator lastFS, Coord epsilon, Point point)
        {      
            unsigned long long result = 0;           
            auto pivot = firstFS;
            
            while (pivot < lastFS)
            {
                if (S.xStart > pivot->xEnd || S.yStart > pivot->yEnd)
                {
                    pivot ++;
                    continue;
                }
                
                if (utils::disk::MinDist(point,pivot) > epsilon*epsilon)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot ++;
            }

            return result;
        };

        inline unsigned long long Range_Corners_ABCD(Relation &pR, Record &S, size_t startR, size_t endR, Coord epsilon, Point point)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners_ABCD(r, S, lastR, epsilon, point);

            return result;
        };

        inline unsigned long long InternalLoop_Range_B_Class(RelationIterator firstFS, Record &S, RelationIterator lastFS, Coord epsilon, Point point)
        {     
            unsigned long long result = 0;            
            auto pivot = firstFS;
            
            while (pivot < lastFS)
            {    
                if (S.yEnd < pivot->yStart || S.yStart > pivot->yEnd)
                {
                    pivot++;
                    continue;
                }

                if (utils::disk::MinDist(point,pivot) > epsilon*epsilon)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot++;
            }

            return result;
        };

        inline unsigned long long Range_B_Class(Relation &pR, Record &S, size_t startR, size_t endR, Coord epsilon, Point point)
        {
            unsigned long long result = 0;

            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_B_Class(r, S, lastR, epsilon, point);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Border_A_Horizontally(RelationIterator firstFS, Record &S, RelationIterator lastFS, Coord epsilon, Point point)
        {   
            unsigned long long result = 0;              
            auto pivot = firstFS;

            while (pivot < lastFS)
            {    
                if (S.yEnd < pivot->yStart)
                {
                    pivot++;
                    continue;
                }

                if (utils::disk::MinDist(point,pivot) > epsilon*epsilon)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot++;
            }

            return result;
        };

        inline unsigned long long Range_Border_A_Horizontally(Relation &pR, Record &S, size_t startR, size_t endR, Coord epsilon, Point point)
        {
            unsigned long long result = 0;

            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Border_A_Horizontally(r, S, lastR, epsilon, point);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Borders_AB(RelationIterator firstFS, Record &S, RelationIterator lastFS, Coord epsilon, Point point)
        {    
            unsigned long long result = 0;             
            auto pivot = firstFS;

            while (pivot < lastFS)
            {    
                if (S.yStart > pivot->yEnd)
                {
                    pivot++;
                    continue;
                }
                
                if (utils::disk::MinDist(point,pivot) > epsilon*epsilon)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot++;
            }

            return result;
        };

        inline unsigned long long Range_Borders_AB(Relation &pR, Record &S, size_t startR, size_t endR, Coord epsilon, Point point)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Borders_AB(r, S, lastR, epsilon, point);

            return result;
        };

        inline unsigned long long InternalLoop_Range_C_Class(RelationIterator firstFS, Record &S, RelationIterator lastFS, Coord epsilon, Point point)
        { 
            unsigned long long result = 0;
            auto pivot = firstFS;

            while (pivot < lastFS)
            {
                if (S.xStart > pivot->xEnd || S.xEnd < pivot->xStart)
                {
                    pivot++;
                    continue;
                }
                
                if (utils::disk::MinDist(point,pivot) > epsilon*epsilon)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot++;
            }

            return result;
        }

        inline unsigned long long Range_C_Class(Relation &pR, Record &S, size_t startR, size_t endR, Coord epsilon, Point point)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_C_Class(r, S, lastR, epsilon, point);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Border_A_Vertically(RelationIterator firstFS, Record &S, RelationIterator lastFS, Coord epsilon, Point point)
        { 
            unsigned long long result = 0;
            auto pivot = firstFS;

            while (pivot < lastFS)
            {
                if (S.xEnd < pivot->xStart)
                {
                    pivot++;
                    continue;
                }

                if (utils::disk::MinDist(point,pivot) > epsilon*epsilon)
                {
                    pivot ++;
                    continue;
                }
                //result += pivot->id ^ S.id;
                result ++;
                pivot++;
            }

            return result;
        }

        inline unsigned long long Range_Border_A_Vertically(Relation &pR, Record &S, size_t startR, size_t endR, Coord epsilon, Point point)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Border_A_Vertically(r, S, lastR, epsilon, point);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Borders_AC(RelationIterator firstFS, Record &S, RelationIterator lastFS, Coord epsilon, Point point)
        { 
            unsigned long long result = 0;
            auto pivot = firstFS;

            while (pivot < lastFS)
            {
                if (S.xStart > pivot->xEnd)
                {
                    pivot++;
                    continue;
                }

                if (utils::disk::MinDist(point,pivot) > epsilon*epsilon)
                {
                    pivot ++;
                    continue;
                }           
                //result += pivot->id ^ S.id;
                result ++;
                pivot++;
            }

            return result;
        }

        inline unsigned long long Range_Borders_AC(Relation &pR, Record &S, size_t startR, size_t endR, Coord epsilon, Point point)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Borders_AC(r, S, lastR, epsilon, point);

            return result;
        };
    }

    namespace spatial_join
    {
        namespace baseline
        {
            //Internal Loops
            inline unsigned long long InternalLoop_Rolled_CNT_Y_(RelationIterator rec, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && rec->yEnd >= pivot->yStart)
                {        
                    if (rec->xStart > pivot->xEnd || rec->xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }
                    //result += rec->id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_Y_(RecordId id, Coord xStart, Coord xEnd, Coord yEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {
                    
                    if (xStart > pivot->xEnd || xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }
                    result++;
                    //result += id ^ pivot->id;
                    pivot++;
                }

                return result;
            };

            //Sweep Rolled
            //A-A
            inline unsigned long long Sweep_Rolled_CNT_Y_base1(Relation &R, Relation &S, size_t endR, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin() ;
                auto s = S.begin() ;
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while (r < lastR && s < lastS)
                {
                    if (r->yStart < s->yStart)
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(r, s, lastS);
                        r++;
                    }
                    else
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(s, r, lastR);
                        s++;
                    }
                }
                return result;
            };

            //A-B //A-C //B-A //C-A
            inline unsigned long long Sweep_Rolled_CNT_Y_base3(Relation &R, Relation &S, size_t endR, size_t startS, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin();
                auto s = S.begin() + startS;
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while (r < lastR && s < lastS)
                {
                    if (r->yStart < s->yStart)
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(r->id, r->xStart, r->xEnd, r->yEnd, s, lastS);
                        r++;
                    }
                    else
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(s->id, s->xStart, s->xEnd, s->yEnd, r, lastR);
                        s++;
                    }
                }
                return result;
            };

            //A-D //D-A
            inline unsigned long long Sweep_Rolled_CNT_Y_base4(Relation &R, Relation &S, size_t endR, size_t startS)
            {
                unsigned long long result = 0;
                auto r = R.begin();
                auto s = S.begin() + startS;
                auto lastR = R.begin() + endR;
                auto lastS = S.end();

                while (r < lastR && s < lastS)
                {
                    if (r->yStart < s->yStart)
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(r->id, r->xStart, r->xEnd, r->yEnd, s, lastS);
                        r++;
                    }
                    else
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(s->id, s->xStart, s->xEnd, s->yEnd, r, lastR);
                        s++;
                    }
                }

                return result;
            };

            //B-C //C-B
            inline unsigned long long Sweep_Rolled_CNT_Y_base(Relation &R, Relation &S, size_t startR, size_t endR, size_t startS, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin() + startR;
                auto s = S.begin() + startS;
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while (r < lastR && s < lastS)
                {
                    if (r->yStart < s->yStart)
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(r->id, r->xStart, r->xEnd, r->yEnd, s, lastS);
                        r++;
                    }
                    else
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(s->id, s->xStart, s->xEnd, s->yEnd, r, lastR);
                        s++;
                    }
                }

                return result;
            };

            inline unsigned long long ForwardScanBased_PlaneSweep_CNT_Y(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartitions)
            {
                unsigned long long result = 0;

                for (int pid = 0; pid < runNumPartitions; pid++)
                {
                    auto pRB = pRB_size[pid];
                    auto pRC = pRC_size[pid];
                    auto pRD = pRD_size[pid];

                    auto pSB = pSB_size[pid];
                    auto pSC = pSC_size[pid];
                    auto pSD = pSD_size[pid];

                    //A-A
                    if (pRB > 0 && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_Y_base1(pR[pid], pS[pid], pRB, pSB);
                    }

                    //A-B
                    if (pRB > 0 && pSC > pSB)
                    {
                        result += Sweep_Rolled_CNT_Y_base3(pR[pid], pS[pid], pRB, pSB, pSC);
                    }

                    //A-C
                    if (pRB > 0 && pSD > pSC)
                    {
                        result += Sweep_Rolled_CNT_Y_base3(pR[pid], pS[pid], pRB, pSC, pSD);
                    }

                    //A-D
                    if (pRB > 0 && pS[pid].size() > pSD)
                    {
                        result += Sweep_Rolled_CNT_Y_base4(pR[pid], pS[pid], pRB, pSD);
                    }

                    //B-A
                    if (pRC > pRB && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_Y_base3(pS[pid], pR[pid], pSB , pRB, pRC);
                    }

                    //B-C
                    if (pRC > pRB && pSD > pSC)
                    {
                        result += Sweep_Rolled_CNT_Y_base(pR[pid],pS[pid], pRB, pRC, pSC,pSD);  
                    }

                    //C-A
                    if (pRD > pRC && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_Y_base3(pS[pid], pR[pid], pSB , pRC, pRD);  
                    }

                    //C-B
                    if (pRD > pRC && pSC > pSB)
                    {
                        result += Sweep_Rolled_CNT_Y_base(pR[pid], pS[pid], pRC, pRD, pSB, pSC);  
                    }

                    //D-A
                    if (pR[pid].size() > pRD && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_Y_base4(pS[pid], pR[pid], pSB, pRD); 
                    }
                }
                return result;
            };

            unsigned long long ForwardScanBased_PlaneSweep_CNT(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartitions)
            {
                return ForwardScanBased_PlaneSweep_CNT_Y(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitions);
            };
        }

        namespace unnecessary_comparisons
        {
            //Internal Loops
            inline unsigned long long InternalLoop_Rolled_CNT_Y_(RecordId id, Coord xStart, Coord xEnd, Coord yEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {          
                    if (xStart > pivot->xEnd || xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V2_Y_(RecordId id, Coord xStart, Coord xEnd, Coord yEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {
                    if (xStart > pivot->xEnd || xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }   
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V3_1_Y_(RecordId id, Coord xStart, Coord yEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {        
                    if (xStart > pivot->xEnd)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V3_2_Y_(RecordId id, Coord xEnd, Coord yEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {       
                    if (pivot->xStart > xEnd)
                    {
                        pivot++;
                        continue;
                    } 
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V5_Y_(RecordId id, Coord xEnd, Coord yEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {
                    if (xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            }; 

            inline unsigned long long InternalLoop_Rolled_CNT_V4_Y_(RecordId id, Coord xStart, Coord yEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {
                    if (xStart > pivot->xEnd)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            //Sweep Rolled
            //A-A
            inline unsigned long long Sweep_Rolled_CNT_Y_Less(Relation &R, Relation &S,  size_t endR, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin();
                auto s = S.begin();
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while ((r < lastR) && (s < lastS))
                {
                    if (r->yStart < s->yStart)
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(r->id, r->xStart, r->xEnd, r->yEnd, s, lastS);
                        r++;
                    }
                    else
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(s->id, s->xStart, s->xEnd, s->yEnd, r, lastR);
                        s++;
                    }
                }

                return result;
            };

            //A-B //B-A
            inline unsigned long long Sweep_Rolled_CNT_V2_Y_(Relation &R, Relation &S, size_t startR, size_t endR, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin() + startR;
                auto s = S.begin();
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while (r < lastR)
                {
                    result += InternalLoop_Rolled_CNT_V2_Y_(r->id, r->xStart, r->xEnd, r->yEnd, s, lastS );
                    r++;
                }

                return result;
            };

            //A-C //C-A
            inline unsigned long long Sweep_Rolled_CNT_V3_Y_(Relation &R, Relation &S, size_t endR, size_t startS, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin();
                auto s = S.begin() + startS;
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while ((r < lastR) && (s < lastS))
                {
                    if (r->yStart < s->yStart)
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_V3_1_Y_(r->id, r->xStart, r->yEnd, s, lastS );
                        r++;
                    }
                    else
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_V3_2_Y_(s->id, s->xEnd, s->yEnd, r, lastR );
                        s++;
                    }
                }

                return result;
            };  

            //A-D //D-A
            inline unsigned long long Sweep_Rolled_CNT_V5_Y_(Relation &R, Relation &S, size_t startR, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin() + startR;
                auto s = S.begin();
                auto lastR = R.end();
                auto lastS = S.begin() + endS;

                while (r < lastR)
                {
                    // Run internal loop.
                    result += InternalLoop_Rolled_CNT_V5_Y_(r->id, r->xEnd, r->yEnd, s, lastS);
                    r++;
                }

                return result;
            };

            //B-C //C-B
            inline unsigned long long Sweep_Rolled_CNT_V4_Y_(Relation &R, Relation &S,size_t startR, size_t endR, size_t startS, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin() + startR;
                auto s = S.begin() + startS;
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while (r < lastR)
                { 
                    // Run internal loop.
                    result += InternalLoop_Rolled_CNT_V4_Y_(r->id, r->xStart, r->yEnd, s, lastS);
                    r++;
                }

                return result;
            };

            inline unsigned long long ForwardScanBased_PlaneSweep_CNT_Y_Less(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartitions)
            {
                unsigned long long result = 0;

                for (int pid = 0; pid < runNumPartitions; pid++)
                {
                    auto pRB = pRB_size[pid];
                    auto pRC = pRC_size[pid];
                    auto pRD = pRD_size[pid];

                    auto pSB = pSB_size[pid];
                    auto pSC = pSC_size[pid];
                    auto pSD = pSD_size[pid];

                    //A-A
                    if (pRB > 0 && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_Y_Less(pR[pid], pS[pid], pRB, pSB);
                    }

                    //A-B
                    if (pRB > 0 && pSC > pSB)
                    {
                        result += Sweep_Rolled_CNT_V2_Y_(pS[pid], pR[pid], pSB , pSC, pRB);
                    }

                    //A-C
                    if (pRB> 0 && pSD > pSC)
                    {
                        result += Sweep_Rolled_CNT_V3_Y_(pR[pid], pS[pid], pRB, pSC, pSD);
                    }

                    //A-D
                    if (pRB> 0 && pS[pid].size() > pSD)
                    {
                        result += Sweep_Rolled_CNT_V5_Y_(pS[pid], pR[pid], pSD, pRB);
                    }

                    //B-A
                    if (pRC > pRB && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_V2_Y_(pR[pid], pS[pid], pRB, pRC, pSB);
                    }
                
                    //B-C
                    if (pRC > pRB && pSD > pSC)
                    {
                        result += Sweep_Rolled_CNT_V4_Y_(pR[pid], pS[pid], pRB, pRC, pSC, pSD);
                    }
                
                    //C-A
                    if (pRD > pRC && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_V3_Y_(pS[pid], pR[pid], pSB, pRC, pRD);
                    }
                
                    //C-B
                    if (pRD > pRC && pSC > pSB)
                    {
                        result += Sweep_Rolled_CNT_V4_Y_(pS[pid], pR[pid], pSB, pSC, pRC, pRD);
                    }

                    //D-A
                    if (pR[pid].size() > pRD && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_V5_Y_(pR[pid], pS[pid], pRD, pSB);
                    }     
                }

                return result;
            };

            unsigned long long ForwardScanBased_PlaneSweep_CNT_Less(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartitions)
            {
                return ForwardScanBased_PlaneSweep_CNT_Y_Less(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitions);
            };
        }

        namespace redundant_comparisons
        {
            inline unsigned long long InternalLoop_Rolled_CNT_Y_(RecordId id, Coord xStart, Coord xEnd, Coord yEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {         
                    if (xStart > pivot->xEnd || xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V2_Y_2(RecordId id, Coord xStart, Coord xEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS)
                {
                    if (xStart > pivot->xEnd || xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V5_Y_2(RecordId id, Coord xStart, Coord xEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS)
                {
                    if (xStart > pivot->xEnd || xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            }; 

            inline unsigned long long InternalLoop_Rolled_CNT_V4_Y_2(RecordId id, Coord xStart, Coord xEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS)
                {
                    if (xStart > pivot->xEnd || xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }    
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            //A-C //C-A
            inline unsigned long long Sweep_Rolled_CNT_Y_Approach1(Relation &R, Relation &S,size_t endR, size_t startS, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin();
                auto s = S.begin() + startS;
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while (r < lastR && s < lastS)
                {
                    if (r->yStart < s->yStart)
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(r->id, r->xStart, r->xEnd, r->yEnd, s, lastS);
                        r++;
                    }
                    else
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(s->id, s->xStart, s->xEnd, s->yEnd, r, lastR);
                        s++;
                    }
                }

                return result;
            };
            
            //A-A
            inline unsigned long long Sweep_Rolled_CNT_Y_Less(Relation &R, Relation &S,  size_t endR, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin();
                auto s = S.begin();
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while (r < lastR && s < lastS)
                {
                    if (r->yStart < s->yStart)
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(r->id, r->xStart, r->xEnd, r->yEnd, s, lastS);
                        r++;
                    }
                    else
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(s->id, s->xStart, s->xEnd, s->yEnd, r, lastR);
                        s++;
                    }
                }

                return result;
            };

            //A-B //B-A
            inline unsigned long long Sweep_Rolled_CNT_V2_Y_2(Relation &R, Relation &S, size_t startR, size_t endR, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin() + startR;
                auto s = S.begin();
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while (r < lastR)
                {
                    while(s < lastS && s->yStart <= r->yEnd)
                    {
                        result += InternalLoop_Rolled_CNT_V2_Y_2(s->id, s->xStart,s->xEnd, r, lastR );
                        s++;
                    }

                    if (!(s < lastS))
                    {
                        return result;
                    }

                    r++;
                }

                return result;
            };

            //A-D //D-A
            inline unsigned long long Sweep_Rolled_CNT_V5_Y_2(Relation &R, Relation &S, size_t startR, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin() + startR;
                auto s = S.begin();
                auto lastR = R.end();
                auto lastS = S.begin() + endS;

                while (r < lastR)
                {
                    while( s < lastS && s->yStart <= r->yEnd)
                    {
                        result += InternalLoop_Rolled_CNT_V5_Y_2(s->id, s->xStart, s->xEnd, r, lastR);
                        s++;
                    }

                    if (!(s < lastS))
                    {
                        return result;
                    }
                    r++;
                }

                return result;
            };

            //B-C //C-B
            inline unsigned long long Sweep_Rolled_CNT_V4_Y_2(Relation &R, Relation &S,size_t startR, size_t endR, size_t startS, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin() + startR;
                auto s = S.begin() + startS;
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;
                
                while (r < lastR)
                {
                    while( s < lastS && s->yStart <= r->yEnd)
                    {
                        result += InternalLoop_Rolled_CNT_V4_Y_2(s->id, s->xStart, s->xEnd, r, lastR );
                        s++;
                    }

                    if (!(s < lastS))
                    {
                        return result;
                    }
                    r++;
                }
                return result;
            };

            inline unsigned long long ForwardScanBased_PlaneSweep_CNT_Y_Approach1(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartitions)
            {
                unsigned long long result = 0;

                for (int pid = 0; pid < runNumPartitions; pid++)
                {
                    auto pRB = pRB_size[pid];
                    auto pRC = pRC_size[pid];
                    auto pRD = pRD_size[pid];

                    auto pSB = pSB_size[pid];
                    auto pSC = pSC_size[pid];
                    auto pSD = pSD_size[pid];

                    //A-A
                    if (pRB > 0 && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_Y_Less(pR[pid], pS[pid], pRB, pSB);
                    }

                    //A-B
                    if (pRB > 0 && pSC > pSB)
                    {
                        result +=Sweep_Rolled_CNT_V2_Y_2(pS[pid], pR[pid], pSB , pSC, pRB);
                    }

                    //A-C
                    if (pRB > 0 && pSD > pSC)
                    {
                        result += Sweep_Rolled_CNT_Y_Approach1(pR[pid], pS[pid], pRB, pSC, pSD);
                    }

                    //A-D
                    if (pRB > 0 && pS[pid].size() > pSD)
                    {
                        result += Sweep_Rolled_CNT_V5_Y_2(pS[pid], pR[pid], pSD, pRB);
                    }

                    //B-A
                    if (pRC > pRB && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_V2_Y_2(pR[pid], pS[pid], pRB, pRC, pSB);
                    }
                
                    //B-C
                    if (pRC > pRB && pSD > pSC)
                    {
                        result += Sweep_Rolled_CNT_V4_Y_2(pR[pid], pS[pid], pRB, pRC, pSC, pSD);
                    }
                
                    //C-A
                    if (pRD > pRC && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_Y_Approach1(pS[pid], pR[pid], pSB, pRC, pRD);
                    }
                
                    //C-B
                    if (pRD > pRC && pSC > pSB)
                    {
                        result += Sweep_Rolled_CNT_V4_Y_2(pS[pid], pR[pid], pSB, pSC, pRC, pRD);
                    }
                    
                    //D-A
                    if (pR[pid].size() > pRD && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_V5_Y_2(pR[pid], pS[pid], pRD, pSB);
                    }
                }
                return result;
            };

            unsigned long long ForwardScanBased_PlaneSweep_CNT_Appoach1(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartitions)
            {
                return ForwardScanBased_PlaneSweep_CNT_Y_Approach1(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitions);
            };
        }

        namespace unnecessary_redundant_comparisons
        {
            inline unsigned long long InternalLoop_Rolled_CNT_Y_(RecordId id, Coord xStart, Coord xEnd, Coord yEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {              
                    if (xStart > pivot->xEnd || xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V2_Y_2(RecordId id, Coord xStart, Coord xEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS)
                {
                    if (xStart > pivot->xEnd || xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V3_1_Y_(RecordId id, Coord xStart, Coord yEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {        
                    if (xStart > pivot->xEnd)
                    {
                        pivot++;
                        continue;
                    }   
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V3_2_Y_(RecordId id, Coord xEnd, Coord yEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {       
                    if (pivot->xStart > xEnd)
                    {
                        pivot++;
                        continue;
                    }  
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V5_Y_2_Approach_LESS(RecordId id, Coord xStart, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS)
                {
                    if (pivot->xEnd < xStart)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };  

             inline unsigned long long InternalLoop_Rolled_CNT_V4_Y_2_Approach_Less(RecordId id, Coord xEnd, RelationIterator firstFS, RelationIterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS)
                {
                    if (pivot->xStart > xEnd)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long Sweep_Rolled_CNT_Y_Less(Relation &R, Relation &S,  size_t endR, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin();
                auto s = S.begin();
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while (r < lastR && s < lastS)
                {
                    if (r->yStart < s->yStart)
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(r->id, r->xStart, r->xEnd, r->yEnd, s, lastS);
                        r++;
                    }
                    else
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_Y_(s->id, s->xStart, s->xEnd, s->yEnd, r, lastR);
                        s++;
                    }
                }

                return result;
            };

            inline unsigned long long Sweep_Rolled_CNT_V2_Y_2(Relation &R, Relation &S, size_t startR, size_t endR, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin() + startR;
                auto s = S.begin();
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while (r < lastR)
                {
                    while(s < lastS && s->yStart <= r->yEnd)
                    {
                        result += InternalLoop_Rolled_CNT_V2_Y_2(s->id, s->xStart,s->xEnd, r, lastR );
                        s++;
                    }

                    if (!(s < lastS)){
                        return result;
                    }

                    r++;
                }

                return result;
            };

            inline unsigned long long Sweep_Rolled_CNT_Y_Approach1_Less(Relation &R, Relation &S, size_t endR, size_t startS, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin();
                auto s = S.begin() + startS;
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;

                while (r < lastR && s < lastS)
                {
                    if (r->yStart < s->yStart)
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_V3_1_Y_(r->id, r->xStart, r->yEnd, s, lastS );
                        r++;
                    }
                    else
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_V3_2_Y_(s->id, s->xEnd, s->yEnd, r, lastR );
                        s++;
                    }
                }

                return result;
            };

            inline unsigned long long Sweep_Rolled_CNT_V5_Y_2_Approach_Less(Relation &R, Relation &S, size_t startR, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin() + startR;
                auto s = S.begin();
                auto lastR = R.end();
                auto lastS = S.begin() + endS;

                while (r < lastR)
                {
                    while(s < lastS && s->yStart <= r->yEnd)
                    {
                        result += InternalLoop_Rolled_CNT_V5_Y_2_Approach_LESS(s->id, s->xStart, r, lastR);
                        s++;
                    }

                    if (!(s < lastS)){
                        return result;
                    }
                    r++;
                }

                return result;
            };

            inline unsigned long long Sweep_Rolled_CNT_V4_Y_2_Approach_Less(Relation &R, Relation &S,size_t startR, size_t endR, size_t startS, size_t endS)
            {
                unsigned long long result = 0;
                auto r = R.begin() + startR;
                auto s = S.begin() + startS;
                auto lastR = R.begin() + endR;
                auto lastS = S.begin() + endS;
                
                while (r < lastR)
                {
                    while(s < lastS && s->yStart <= r->yEnd)
                    {
                        result += InternalLoop_Rolled_CNT_V4_Y_2_Approach_Less(s->id, s->xEnd, r, lastR);
                        s++;
                    }

                    if (!(s < lastS))
                    {
                        return result;
                    }
                    r++;
                }

                return result;
            };


            inline unsigned long long ForwardScanBased_PlaneSweep_CNT_Y_Less_Approach1(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartitions)
            {
                unsigned long long result = 0;

                for (int pid = 0; pid < runNumPartitions; pid++)
                {
                    auto pRB = pRB_size[pid];
                    auto pRC = pRC_size[pid];
                    auto pRD = pRD_size[pid];

                    auto pSB = pSB_size[pid];
                    auto pSC = pSC_size[pid];
                    auto pSD = pSD_size[pid];

                    //A-A
                    if (pRB > 0 && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_Y_Less(pR[pid], pS[pid], pRB, pSB);
                    }

                    //A-B
                    if (pRB > 0 && pSC > pSB)
                    {
                        result +=Sweep_Rolled_CNT_V2_Y_2(pS[pid], pR[pid], pSB , pSC, pRB);
                    }

                    //A-C
                    if (pRB > 0 && pSD > pSC)
                    {
                        result += Sweep_Rolled_CNT_Y_Approach1_Less(pR[pid], pS[pid], pRB, pSC, pSD);
                    }

                    //A-D
                    if (pRB > 0 && pS[pid].size() > pSD)
                    {
                        result += Sweep_Rolled_CNT_V5_Y_2_Approach_Less(pS[pid], pR[pid], pSD, pRB);
                    }

                    //B-A
                    if (pRC > pRB && pSB > 0){

                        result += Sweep_Rolled_CNT_V2_Y_2(pR[pid], pS[pid], pRB, pRC, pSB);
                    }
                
                    //B-C
                    if (pRC > pRB && pSD > pSC)
                    {
                        result += Sweep_Rolled_CNT_V4_Y_2_Approach_Less(pR[pid], pS[pid], pRB, pRC, pSC, pSD);
                    }
                
                    //C-A
                    if (pRD > pRC && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_Y_Approach1_Less(pS[pid], pR[pid], pSB, pRC, pRD);
                    }
                
                    //C-B
                    if (pRD > pRC && pSC > pSB)
                    {
                        result += Sweep_Rolled_CNT_V4_Y_2_Approach_Less(pS[pid], pR[pid], pSB, pSC, pRC, pRD);
                    }
                    
                    //D-A
                    if (pR[pid].size() > pRD && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_V5_Y_2_Approach_Less(pR[pid], pS[pid], pRD, pSB);
                    } 
                }

                return result;
            };

            unsigned long long ForwardScanBased_PlaneSweep_CNT_Less_Appoach1(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartitions)
            {
                return ForwardScanBased_PlaneSweep_CNT_Y_Less_Approach1(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitions);
            };

        }

        namespace all_opts
        {
            // Internal Loops
            inline unsigned long long InternalLoop_Rolled_CNT_Y_Dec2(RecordId id, Coord xStart, Coord xEnd, Coord yEnd, vector<ABrec2>::const_iterator firstFS, vector<ABrec2>::const_iterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {
                    if (xStart > pivot->xEnd || xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }      
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V2_Y_Dec2_Approach1(RecordId id, Coord xStart, Coord xEnd, Coord yEnd, vector<ABrec2>::const_iterator firstFS, vector<ABrec2>::const_iterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;
                
                while (pivot < lastFS)
                {
                    if (xStart > pivot->xEnd || xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V3_2_Y_Dec2(RecordId id, Coord xEnd, Coord yEnd, vector<ABrec2>::const_iterator firstFS, vector<ABrec2>::const_iterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {       
                    if (pivot->xStart > xEnd)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V3_1_Y_Dec2(RecordId id, Coord xStart, Coord  yEnd, vector<Crec2>::const_iterator firstFS, vector<Crec2>::const_iterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS && yEnd >= pivot->yStart)
                {        
                    if (xStart > pivot->xEnd)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result ++;
                    pivot++;
                }

                return result;
            };

            inline unsigned long long InternalLoop_Rolled_CNT_V5_Y_Dec2_Approach1(RecordId id, Coord xStart, vector<Drec>::const_iterator firstFS, vector<Drec>::const_iterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS)
                {
                    if (xStart > pivot->xEnd)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result ++;
                    pivot++;
                }

                return result;
            };   
            
            inline unsigned long long InternalLoop_Rolled_CNT_V4_Y_Dec2_Approach1(RecordId id, Coord xEnd, vector<ABrec2>::const_iterator firstFS, vector<ABrec2>::const_iterator lastFS)
            {
                unsigned long long result = 0;
                auto pivot = firstFS;

                while (pivot < lastFS)
                {
                    if (xEnd < pivot->xStart)
                    {
                        pivot++;
                        continue;
                    }
                    //result += id ^ pivot->id;
                    result++;
                    pivot++;
                }

                return result;
            };

            //Sweep Rolled
            //B-C //C-B
            inline unsigned long long Sweep_Rolled_CNT_V4_Y_Dec2_Approach1(vector<ABrec2> &pRABdec, vector<Crec2> &pSCdec,size_t startR)
            {
                unsigned long long result = 0;
                auto rAB = pRABdec.begin() + startR;
                auto sC = pSCdec.begin();
                auto lastRAB = pRABdec.end();
                auto lastSC = pSCdec.end();

                while (rAB < lastRAB)
                { 
                    while (sC < lastSC && sC->yStart <= rAB->yEnd)
                    {
                        // Run internal loop.                                            //rAB->id, rYEND->yEnd, rAB->xStart, sC, lastSC
                        result += InternalLoop_Rolled_CNT_V4_Y_Dec2_Approach1(sC->id, sC->xEnd, rAB, lastRAB);
                        sC++;
                    }

                    if (!(sC < lastSC))
                    {
                        return result;
                    }
                    
                    rAB++;
                }

                return result;
            };

            //A-B //A-D //D-A
            inline unsigned long long Sweep_Rolled_CNT_V5_Y_Dec2_Approach1(vector<Drec> &pSDdec, vector<ABrec2> &pRABdec, size_t endS)
            {
                unsigned long long result = 0;
                auto rD = pSDdec.begin();
                auto sAB = pRABdec.begin();
                auto lastRD = pSDdec.end();
                auto lastSAB = pRABdec.begin() + endS;

                while (rD < lastRD)
                {
                    while (sAB < lastSAB && sAB->yStart <= rD->yEnd)
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_V5_Y_Dec2_Approach1(sAB->id, sAB->xStart, rD, lastRD);
                        sAB ++;
                    }

                    if (!(sAB < lastSAB))
                    {
                        return result;
                    }
                    rD++;
                }

                return result;
            };

            //A-C //C-A
            inline unsigned long long Sweep_Rolled_CNT_V3_Y_Dec2(vector<ABrec2>& pRABdec, vector<Crec2>& pSCdec, size_t endR)
            {
                unsigned long long result = 0;
                auto rAB = pRABdec.begin();
                auto sC = pSCdec.begin();
                auto lastRAB = pRABdec.begin() + endR;
                auto lastSC = pSCdec.end();

                while (rAB < lastRAB && sC < lastSC)
                {
                    if (rAB->yStart < sC->yStart)
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_V3_1_Y_Dec2(rAB->id, rAB->xStart, rAB->yEnd, sC, lastSC);
                        rAB++;
                    }
                    else
                    {
                        // Run internal loop.
                        result += InternalLoop_Rolled_CNT_V3_2_Y_Dec2(sC->id, sC->xEnd, sC->yEnd, rAB, lastRAB);
                        sC++;
                    }
                }

                return result;
            };

            //B-A
            inline unsigned long long Sweep_Rolled_CNT_V2_Y_Dec2_Approach1(vector<ABrec2>& pRABdec, vector<ABrec2>& pSABdec, size_t startR,  size_t endS)
            {
                unsigned long long result = 0;
                auto rAB = pRABdec.begin() + startR;
                auto sAB = pSABdec.begin();
                auto lastRAB = pRABdec.end();
                auto lastSAB = pSABdec.begin() + endS;

                while (rAB < lastRAB)
                {                                                                  
                    while (sAB < lastSAB && sAB->yStart <= rAB->yEnd)
                    {
                        result += InternalLoop_Rolled_CNT_V2_Y_Dec2_Approach1(sAB->id, sAB->xStart, sAB->xEnd, sAB->yEnd, rAB, lastRAB);
                        sAB++;
                    }

                    if (!(sAB < lastSAB)){
                        return result;
                    }
                    rAB++;
                }

                return result;

            };

            //A-A
            inline unsigned long long Sweep_Rolled_CNT_Y_Dec2(vector<ABrec2>& pRABdec, vector<ABrec2>& pSABdec, size_t endR, size_t endS)
            {
                unsigned long long result = 0;
                auto rAB = pRABdec.begin();
                auto sAB = pSABdec.begin();
                auto lastRAB = pRABdec.begin() + endR;
                auto lastSAB = pSABdec.begin() + endS;

                while (rAB < lastRAB && sAB < lastSAB)
                {
                    if (rAB->yStart < sAB->yStart)
                    {
                        // Run internal loop.                                          //rYEND->id,rYEND->yEnd, rAB->xStart,rAB->xEnd, sAB, lastSAB 
                        result += InternalLoop_Rolled_CNT_Y_Dec2(rAB->id, rAB->xStart, rAB->xEnd, rAB->yEnd, sAB, lastSAB);
                        rAB++;
                    }
                    else
                    {
                        // Run internal loop.                                         //rYEND->id,rYEND->yEnd, rAB->xStart,rAB->xEnd, sAB, lastSAB 
                        result += InternalLoop_Rolled_CNT_Y_Dec2(sAB->id, sAB->xStart, sAB->xEnd, sAB->yEnd, rAB, lastRAB);
                        sAB++;
                    }
                }

                return result;
            };

            inline unsigned long long  ForwardScanBased_PlaneSweep_CNT_Y_Less_Dec_Approach1(vector<ABrec2>* pRABdec, vector<ABrec2>* pSABdec,  vector<Crec2> *pRCdec, vector<Crec2> *pSCdec, vector<Drec> *pRDdec, vector<Drec> *pSDdec, size_t *pRB_size, size_t *pSB_size, int runNumPartitions){
                unsigned long long result = 0; 

                for (int pid = 0; pid < runNumPartitions; pid++)
                {
                    auto pRB = pRB_size[pid];
                    auto pRAB = pRABdec[pid].size();

                    auto pSB = pSB_size[pid];
                    auto pSAB = pSABdec[pid].size();

                    //A-A
                    if (pRB > 0 && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_Y_Dec2(pRABdec[pid], pSABdec[pid], pRB, pSB);
                    }

                    
                    //A-B
                    if (pRB > 0 && pSAB > pSB)
                    {
                        result +=Sweep_Rolled_CNT_V2_Y_Dec2_Approach1(pSABdec[pid], pRABdec[pid], pSB, pRB);
                    }

                    //A-C
                    if (pRB> 0 && pSCdec[pid].size() > 0){
                        result += Sweep_Rolled_CNT_V3_Y_Dec2(pRABdec[pid], pSCdec[pid], pRB);
                    }

                    //A-D
                    if (pRB > 0 && pSDdec[pid].size() > 0)
                    {
                        result += Sweep_Rolled_CNT_V5_Y_Dec2_Approach1(pSDdec[pid], pRABdec[pid], pRB);
                    }

                    //B-A
                    if (pRAB > pRB && pSB > 0)
                    {
                        result +=Sweep_Rolled_CNT_V2_Y_Dec2_Approach1(pRABdec[pid], pSABdec[pid], pRB, pSB);
                    }
                
                    //B-C
                    if (pRAB > pRB && pSCdec[pid].size() > 0)
                    {
                        result += Sweep_Rolled_CNT_V4_Y_Dec2_Approach1(pRABdec[pid], pSCdec[pid], pRB);
                    }

                    //C-A
                    if (pRCdec[pid].size() > 0 && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_V3_Y_Dec2(pSABdec[pid], pRCdec[pid], pSB);
                    }

                    //C-B
                    if (pRCdec[pid].size() > 0 && pSAB > pSB)
                    {
                        result += Sweep_Rolled_CNT_V4_Y_Dec2_Approach1(pSABdec[pid], pRCdec[pid], pSB);
                    }
                
                    //D-A
                    if (pRDdec[pid].size() > 0 && pSB > 0)
                    {
                        result += Sweep_Rolled_CNT_V5_Y_Dec2_Approach1(pRDdec[pid], pSABdec[pid], pSB);
                    }  
                }
                return result;
            };

            unsigned long long ForwardScanBased_PlaneSweep_CNT_Less_Dec_Approach1(vector<ABrec2>* pRABdec, vector<ABrec2>* pSABdec,  vector<Crec2> *pRCdec, vector<Crec2> *pSCdec, vector<Drec> *pRDdec, vector<Drec> *pSDdec, size_t *pRB_size, size_t *pSB_size, int runNumPartitions)
            {
                return ForwardScanBased_PlaneSweep_CNT_Y_Less_Dec_Approach1(pRABdec, pSABdec, pRCdec, pSCdec, pRDdec, pSDdec, pRB_size, pSB_size, runNumPartitions);
            };
        }
    }

}

#endif

