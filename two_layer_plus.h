#ifndef TWO_LAYER_PLUS_H
#define	TWO_LAYER_PLUS_H

namespace twoLayerPlus{ 
    namespace window{
        inline unsigned long long InternalLoop_Range_Corners(RelationIterator firstFS,Record &S, RelationIterator lastFS)
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
        }

        inline unsigned long long Range_Corners(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners(r,S,lastR);

            return result;
        };


        inline unsigned long long InternalLoop_Range_Corners_A(RelationIterator firstFS,Record &S, RelationIterator lastFS)
        {           
            unsigned long long result = 0;      
            auto pivot = firstFS;
            
            while ((pivot < lastFS))
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
        }

        inline unsigned long long Range_Corners_A(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners_A(r,S,lastR);

            return result;
        };

        inline unsigned long long InternalLoop_Range_Corners_AB(RelationIterator firstFS,Record &S, RelationIterator lastFS)
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
        }

        inline unsigned long long Range_Corners_AB(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners_AB(r,S,lastR);

            return result;
        };


        inline unsigned long long InternalLoop_Range_Corners_AC(RelationIterator firstFS,Record &S, RelationIterator lastFS)
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
                //result += pivot->id ^ S.id;
                result ++;
                pivot ++;
            }

            return result;
        }

        inline unsigned long long Range_Corners_AC(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners_AC(r,S,lastR);

            return result;
        };


        inline unsigned long long InternalLoop_Range_Corners_ABCD(RelationIterator firstFS,Record &S, RelationIterator lastFS)
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
        }

        inline unsigned long long Range_Corners_ABCD(Relation &pR, Record &S, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_Corners_ABCD(r,S,lastR);

            return result;
        };

        inline unsigned long long InternalLoop_Range_B_Class(vector<Decompose>::const_iterator firstFS, vector<Decompose>::const_iterator lastFS, Record &s, vector<double> &indexFS)
        { 
            unsigned long long result = 0;
            auto pivot = firstFS;
            
            while (pivot < lastFS)
            {
                if (s.yStart > indexFS[(*pivot).id*4+3] || s.yEnd < indexFS[(*pivot).id*4+2])
                {
                    pivot++;
                    continue;
                }
                result++;
                pivot++;
            }

            return result;
        }

        inline unsigned long long Range_B_Class(vector<Decompose> &pR,vector<Coord> &indexR, Record &s, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR ;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_B_Class(r, lastR/*, count*/,s, indexR);
            
            return result;
        };

        inline unsigned long long InternalLoop_Range_C_Class(vector<Decompose>::const_iterator firstFS, vector<Decompose>::const_iterator lastFS, Record &s, vector<double> &indexFS)
        {   
            unsigned long long result = 0;
            auto pivot = firstFS;
            while (pivot < lastFS)
            {
                if (s.xStart > indexFS[(*pivot).id*4+1] || s.xEnd < indexFS[(*pivot).id*4])
                {
                    pivot++;
                    continue;
                }
                result ++;
                pivot++;
            }

            return result;
        }

        inline unsigned long long Range_C_Class(vector<Decompose> &pR,vector<Coord> &indexR, Record &s, size_t startR, size_t endR)
        {
            unsigned long long result = 0;
            auto r = pR.begin() + startR;
            auto lastR = pR.begin() + endR;

            result = InternalLoop_Range_C_Class(r, lastR/*, count*/,s, indexR);
        
            return result;
        };


        int binarySearch(vector<Decompose> &R, size_t startR, size_t endR,double key)
        {
            if(startR >= endR){
                return 0;
            }
            
            int low = startR;
            int high = endR;
            int ans = endR;

            while (low <= high) 
            {    
                int mid = (high+low)/2;
                double midVal = R[mid].value;
                
                if (midVal < key) 
                {
                    low = mid + 1;
                }
                else if (midVal >= key) 
                {
                    ans = mid;
                    high = mid - 1;
                }
            }
            
            if(low > endR){
                return 0;
            }
            return endR-ans;
        }

        inline unsigned long long RangeWithBinarySearch(vector<Decompose> &pR, size_t startR, size_t endR,double key)
        {
            unsigned long long result = 0;
            int index = binarySearch(pR, startR, endR,key);
            
            if (index > 0)
            {
                for(int i = endR-index; i < endR; i++)
                {
                    result++;
                }
            }

            return result;
        };  

        int binarySearch2(vector<Decompose> &R, size_t startR, size_t endR,double key)
        {
            if(startR >= endR)
            {
                return 0;
            }
            
            int low = startR;
            int high = endR;
            int ans = endR;

            while (low <= high) 
            {    
                int mid = (high+low)/2;
                double midVal = R[mid].value;
                
                if (midVal < key) 
                {
                    low = mid + 1;
                }
                else if (midVal > key) 
                {
                    ans = mid;
                    high = mid - 1;
                }
                else if (midVal == key) 
                {
                    low = mid+1;
                }
            }

            if(low > endR){
                return endR;
            }
            
            return ans;
        }

        inline unsigned long long RangeWithBinarySearch2(vector<Decompose> &pR, size_t startR, size_t endR,double key)
        {
            unsigned long long result = 0;
            int index = binarySearch2(pR, startR, endR,key);
            if (index >= 0)
            {
                for(int i = 0; i < index;i++)
                {
                    result++;
                }
            }

            return result;
        }; 

    }
}
#endif