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


#ifndef _RELATION_H_
#define _RELATION_H_

#include "../def.h"

class Point {
    public:
	Coord x,y;

	Point(double x, double y);
	Point();
	void print();
};


class Record
{
public:
    RecordId id;
    
    // MBR
	Coord xStart, yStart; // bottom-left corner
    Coord xEnd, yEnd;     // top-right corner

	Record();
	Record(RecordId id, Coord xStart, Coord yStart, Coord xEnd, Coord yEnd);
	bool operator < (const Record& rhs) const;
	bool operator >= (const Record& rhs) const;
	void print() const;
	void print(char c) const;
	~Record();	

};


class Relation : public vector<Record>{
	public:
		Coord minX, maxX, minY, maxY;

		Relation();
		void load(const char *filename);
		void loadDisk(Coord epsilon,vector<Point> &points);
		void sortByXStart();
		void sortByYStart();
		void sortByCellId();
		void sortByCellId_Y();
		void print(char c);
		void normalize(Coord minX, Coord maxX, Coord minY, Coord maxY, Coord maxExtent);
		void computeAvgExtents1d();
		~Relation();
};

//two_layer+
class Decompose {
    public: 
        RecordId id;
        Coord value;
        
        Decompose (RecordId id, Coord value);
        Decompose ();
        bool operator < (const Decompose& rhs) const;
        bool operator >= (const Decompose& rhs) const;
        bool operator < (const double rh) const;
}; 

//two_layer spatial join, s-opt
class ABrec2{
	public:
		RecordId id;
		Coord yStart, xStart, xEnd, yEnd;   

		ABrec2();
		ABrec2(RecordId id, Coord xStart, Coord yStart, Coord xEnd, Coord yEnd);
		~ABrec2();
};

class Crec2{
	public:
		RecordId id;
		Coord yStart, xEnd, yEnd;

		Crec2();
		Crec2(RecordId id, Coord yStart, Coord xEnd, Coord yEnd);
		~Crec2();

};


class Drec{
	public:
		RecordId id;
		Coord xEnd, yEnd;

		Drec();
		Drec(RecordId id, Coord xEnd, Coord yEnd);
		~Drec();
};





typedef Relation::const_iterator RelationIterator;
typedef Relation Group;
typedef Group::const_iterator GroupIterator;
#endif //_RELATION_H_
