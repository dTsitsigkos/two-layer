/******************************************************************************
 * Project:  ijoin
 * Purpose:  Compute interval overlap joins
 * Author:   Panagiotis Bouros, pbour@github.io
 ******************************************************************************
 * Copyright (c) 2017, Panagiotis Bouros
 *
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ******************************************************************************/


#include "relation.h"


/*inline bool CompareByYStart2(const OneDStorage& ld, const OneDStorage& rd)
{
    return (ld.yStart < rd.yStart);
}*/

Point::Point( double x, double y)
{
    this->x = x;
    this->y = y;
}


Point::Point()
{

}


void Point::print () {
    cout << this->x << " " << this->y << " \n";
}



inline bool CompareByYStart(const Record& lhs, const Record& rhs)
{
    return (lhs.yStart < rhs.yStart);
}


inline bool CompareByCellId(const Record& lhs, const Record& rhs)
{
    return (lhs.id < rhs.id);
}


inline bool CompareByCellId_Y(const Record& lhs, const Record& rhs)
{
    if (lhs.id == rhs.id){
        return (lhs.yStart < rhs.yStart);
    }
    return (lhs.id < rhs.id);
}



Record::Record()
{
}


Record::Record(RecordId id, Coord xStart, Coord yStart, Coord xEnd, Coord yEnd)
{
    this->id = id;

    // MBR
    this->xStart = xStart;
    this->xEnd   = xEnd;
    this->yStart = yStart;
    this->yEnd   = yEnd;
}


bool Record::operator < (const Record& rhs) const
{
    return this->xStart < rhs.xStart;
}

bool Record::operator >= (const Record& rhs) const
{
    return !((*this) < rhs);
}


void Record::print(char c) const
{
    cout <<"[" << c << this->id << ": ("  << this->xStart << "," << this->yStart << ") -> ("  << this->xEnd << "," << this->yEnd << ")]" << endl;
}

   
Record::~Record()
{
}



Relation::Relation()
{
    this->minX = std::numeric_limits<Coord>::max();
    this->maxX = std::numeric_limits<Coord>::min() -500;
    this->minY = std::numeric_limits<Coord>::max();
    this->maxY = std::numeric_limits<Coord>::min();
    // this->avgXExtent = 0;
    // this->avgYExtent = 0;
}


void Relation::load(const char *filename)
{
    RecordId id = 0;
    
    ifstream file( filename );
    if(!file)
    {
        cerr << "Cannot open the File : " << filename << endl;
        exit(1);
    }
    
    string line;
    while (getline( file, line ) )
    {
        
        istringstream is( line );
        string s;

        bool first = true;
        Coord minXmbr, minYmbr, maxXmbr, maxYmbr;
        minXmbr = std::numeric_limits<Coord>::max();
        maxXmbr = -std::numeric_limits<Coord>::max();
        minYmbr = std::numeric_limits<Coord>::max();
        maxYmbr = -std::numeric_limits<Coord>::max();
      
        //count vextor size
        int counter = 0 ;
        for ( int i = 0 ; i < line.size()  ; i ++){
        
            if ( line[i] == ','){
                counter++;
            }
        }
        counter++;
        
        //vector<Coordinates> vec ; 
       
        //vec.reserve(counter);

        //cout<<"iiiiiiiiiiiiii = "<<endl;
        
        while(getline( is, s, ',' ))
        {
            
            Coord x,y;
            istringstream ss(s);
               
            ss >> x >> y;

            //Coordinates c = Coordinates(x,y);
            //vec.push_back(c);
            //cout<<"x = " << x <<"\ty = " << y<<endl;
            
            this->minX = std::min(this->minX, x);
            this->maxX = std::max(this->maxX, x);
            this->minY = std::min(this->minY, y);
            this->maxY = std::max(this->maxY, y);

            minXmbr = std::min(minXmbr, x);
            maxXmbr = std::max(maxXmbr, x);
            minYmbr = std::min(minYmbr, y);
            maxYmbr = std::max(maxYmbr, y);
            
        }
        
        
        //cout<<minXmbr <<" " <<minYmbr <<"," <<maxXmbr <<" " <<maxYmbr<< endl;
        
        //this->emplace_back(id, minXmbr, minYmbr, maxXmbr, maxYmbr,vec);
        
        this->emplace_back(id, minXmbr, minYmbr, maxXmbr, maxYmbr);
        
        id++;

    }
    file.close();

    //this->numRecords = this->size();
}

void Relation::loadDisk(Coord epsilon, vector<Point> &points)
{
    Coord x,y;
    
    for ( int i = 0 ; i < this->size() ; i ++){
        x = (this->at(i).xStart +this->at(i).xEnd)/2;
        y = (this->at(i).yStart + this->at(i).yEnd)/2;

        points[i].x = x;
        points[i].y = y;
        
        if ( x - epsilon < 0){
            this->at(i).xStart = 0;
        } 
        else{
            this->at(i).xStart = x - epsilon;
        }

        if (x + epsilon > 1.0){
            this->at(i).xEnd = 1.0;
        }
        else{
            this->at(i).xEnd = x + epsilon;
        }

        if (y - epsilon < 0 ){ 
            this->at(i).yStart = 0;
        }
        else{
            this->at(i).yStart = y - epsilon;
        }

        if (y + epsilon > 1.0){
            this->at(i).yEnd = 1.0;
        }
        else{
            this->at(i).yEnd = y + epsilon;
        }
    }
}


void Relation::sortByXStart()
{
    sort(this->begin(), this->end());
}


void Relation::sortByCellId(){

    sort(this->begin(), this->end(), CompareByCellId);
}

void Relation::sortByCellId_Y(){

    sort(this->begin(), this->end(), CompareByCellId_Y);
}


void Relation::sortByYStart()
{
    sort(this->begin(), this->end(), CompareByYStart);
}


void Relation::print(char c)
{
    for (const Record& rec : (*this)){
        rec.print(c);
    }
}

void Relation::normalize(Coord minX, Coord maxX, Coord minY, Coord maxY, Coord maxExtent) {
    for (Record& rec : (*this))
    {
        rec.xStart = Coord(rec.xStart - minX) / maxExtent;
        rec.xEnd   = Coord(rec.xEnd   - minX) / maxExtent;
        rec.yStart = Coord(rec.yStart - minY) / maxExtent;
        rec.yEnd   = Coord(rec.yEnd   - minY) / maxExtent;
        //cout<<"rec.xStart = " << rec.xStart << ", rec.xEnd = " << rec.xEnd << ", rec.yStart = " << rec.yStart << ", rec.yEnd = " << rec.yEnd<<endl;
    }
    
    
}


void Relation::computeAvgExtents1d() {
    double sumX = 0, sumY = 0;

    for (Record& rec : (*this)) {
        sumX += rec.xEnd-rec.xStart;
        sumY += rec.yEnd-rec.yStart;
    }
    // this->avgXExtent = (double)sumX/this->numRecords;
    // this->avgYExtent = (double)sumY/this->numRecords;
}

Relation::~Relation()
{
}

Decompose::Decompose (RecordId id, Coord value){
    this->id = id;
    this->value = value;
}


Decompose::Decompose (){
}


bool Decompose::operator < (const Decompose& rhs) const
{
    return this->value < rhs.value;
}


bool Decompose::operator < (const double rh) const
{
    return this->value > rh;
}


bool Decompose::operator >= (const Decompose& rhs) const
{
    return !((*this) < rhs);
}

ABrec2::ABrec2(){

}

ABrec2::ABrec2(RecordId id, Coord xStart, Coord yStart, Coord xEnd, Coord yEnd){
    this->id = id;
    this->xStart = xStart;
    this->yStart = yStart;
    this->xEnd = xEnd;
    this->yEnd = yEnd;
}

ABrec2::~ABrec2(){

}


Crec2::Crec2(){

}

Crec2::Crec2(RecordId id, Coord yStart, Coord xEnd, Coord yEnd){
    this->id = id;
    this->yStart = yStart;
    this->xEnd = xEnd;
    this->yEnd = yEnd;
}

Crec2::~Crec2(){

}

Drec::Drec(){

}

Drec::Drec(RecordId id, Coord xEnd, Coord yEnd){
    this->id = id;
    this->xEnd = xEnd;
    this->yEnd = yEnd;
}

Drec::~Drec(){

}