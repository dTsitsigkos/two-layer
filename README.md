# Two-layer Space-oriented Partitioning for Non-point Data

Non-point spatial objects (e.g., polygons, linestrings, etc.) are ubiquitous. We study the problem of indexing non-point
objects in memory for range queries and spatial intersection joins. We propose a secondary partitioning technique for space-oriented
partitioning indices (e.g., grids), which improves their performance significantly, by avoiding the generation and elimination of duplicate
results. Our approach is easy to implement and can be used by any space-partitioning index to significantly reduce the cost of range
queries and intersection joins. In addition, the secondary partitions can be processed independently, which makes our method
appropriate for distributed and parallel indexing. Experiments on real datasets confirm the advantage of our approach against
alternative duplicate elimination techniques and data-oriented state-of-the-art spatial indices. We also show that our partitioning
technique, paired with optimized partition-to-partition join algorithms, typically reduces the cost of spatial joins by around 50%.

Source code from the following publications:

- Dimitrios Tsitsigkos, Panagiotis Bouros, Konstantinos Lampropoulos, Nikos Mamoulis and Manolis Terrovitis, <i>Two-layer Space-oriented Partitioning for Non-point Data</i>, https://doi.org/10.1109/TKDE.2023.3297975, IEEE Transactions on Knowledge and Data Engineering (IEEE TKDE), Vol 36, No 3, March 2024
- <p align="justify"> Dimitrios Tsitsigkos, Konstantinos Lampropoulos, Panagiotis Bouros, Nikos Mamoulis and Manolis Terrovitis, <i>A Two-layer Partitioning for Non-point Spatial Data</i>, https://doi.org/10.1109/ICDE51399.2021.00157, Proceedings of the 37th IEEE International Conference on Data Engineering (IEEE ICDE'21), Chania, Greece, April 19-22, 2021</p>

### Dependencies
- g++/gcc 
- OpenMP

### Datasets
Our implementation uses two types of datasets, depending on the query.
    - For range and disk queries, we use one input dataset file to build the 2-layer index and a query file.
    - For spatial joins, we use two input dataset files.
Both types use MBRs, and the file format is:

     ```
    x1Start y1Start, x1End y1End
    x2Start y2Start, x2End y2End
    ```
    
For disk queries, we calculate the center of each MBR to use as the query point.

### Compile
Compile using ```make all``` or ```make <option>``` where <option> can be one of the following:
   - main_two_layer
   - main_two_layer_plus
   - main_spatial_join_optimizations
   - main_transformation_spatial_join

### Parameters
Parameters of main_two_layer and main_two_layer_plus. **Keep in mind** that main_two_layer_plus does not support Disk query and Spatial join:
| Parameters | README |
| ------ | ------ |
| -p | The number of partitions to be used |
| -w | Window query |
| -d | Disk Query. **Keep in mind** that when using -d the use of -e is mandatory. (see examples) |
| -e | Radius of the disk query |
| -s | Spatial join |
| -h | Print the manual |


Parameters of main_spatial_join_optimizations:
| Parameters | README |
| ------ | ------ |
| -p | The number of partitions to be used |
| -b | Mini joins baseline |
| -u | Mini joins and unnecessary comparisons |
| -r | Mini joins and redundant comparisons |
| -c | Mini joins, unnecessary and redundant comparisons |
| -a | Mini joins, unnecessary and redundant comparisons and storage optimizations |
| -h | Print the manual |

Parameters of main_transformation_spatial_join:
| Parameters | README |
| ------ | ------ |
| -p | The number of partions for the first dataset per dimension |
| -s | The number of partions for the second dataset per dimension |
| -m | Materialized spatial join algorithm |
| -n | Non-materialized spatial join algorithm |
| -h | Print the manual |


### Files
- #####  utils.h
    
    Contains key functions for the grid, distance calculations for disk queries, and sorting for all queries. 

- #####  partition.h
    
    Contains all the necessary partitioning methods for the different queries.  

- #####  two_layer.h
    
    Contains all querying methods for two-layer, including range queries, disk queries, and spatial joins.  

- #####  two_layer_plus.h
    
    Contains range query methods for two-layer-plus.

- #####  main_two_layer.cpp

    Contains the 2-layer code, which supports window queries, disk queries, and spatial joins.

    ##### Example 
    - ###### window Query
    
        ```sh
        $ ./two_layer -p 3000 -w dataset_file.csv query_file.csv
        ```
    - ###### disk Query
    
        ```sh
        $ ./two_layer -p 3000 -d -e 0.1 dataset_file.csv query_file.csv
        ```

     - ###### spatial join

     ```sh
        $ ./two_layer -p 3000 -s dataset_file.csv dataset_file2.csv
        ```

- #####  main_two_layer_plus.cpp

    Contains the 2-layer-plus code, which supports window queries.

    - ###### window Query
    
        ```sh
        $ ./two_layer_plus -p 3000 -w dataset_file.csv query_file.csv
        ```

- #####  main_spatial_join_optimizations.cpp
    
    Contains the 2-layer code for spatial join optimizations when no partitions exist for the two datasets.

    - ###### Spatial join query mini joins baseline optimization

    ```sh
    $ ./sj_opt -p 2000 -b dataset_file1.csv dataset_file2.csv
    ```


- #####  main_transformation_spatial_join.cpp

    Contains the 2-layer code for spatial joins when the two datasets already have partitions with different powers of 2 (transformation grid: materialized and non-materialized).

    - ###### Spatial join query materialized
    
        ```sh
        $ ./sj_transf -p 2048 -s 256 -m dataset_file1.csv dataset_file2.csv
        ```

     - ###### Spatial join query non-materialized
    
        ```sh
        $ ./sj_transf -p 2048 -s 256 -n dataset_file1.csv dataset_file2.csv
        ```

# Cite
```
@article{DBLP:journals/tkde/TsitsigkosBLMT24,
  author       = {Dimitrios Tsitsigkos and
                  Panagiotis Bouros and
                  Konstantinos Lampropoulos and
                  Nikos Mamoulis and
                  Manolis Terrovitis},
  title        = {Two-Layer Space-Oriented Partitioning for Non-Point Data},
  journal      = {{IEEE} Trans. Knowl. Data Eng.},
  volume       = {36},
  number       = {3},
  pages        = {1341--1355},
  year         = {2024},
  url          = {https://doi.org/10.1109/TKDE.2023.3297975},
  doi          = {10.1109/TKDE.2023.3297975}
}

@inproceedings{DBLP:conf/icde/TsitsigkosLBMT21,
  author       = {Dimitrios Tsitsigkos and
                  Konstantinos Lampropoulos and
                  Panagiotis Bouros and
                  Nikos Mamoulis and
                  Manolis Terrovitis},
  title        = {A Two-layer Partitioning for Non-point Spatial Data},
  booktitle    = {37th {IEEE} International Conference on Data Engineering, {ICDE} 2021,
                  Chania, Greece, April 19-22, 2021},
  pages        = {1787--1798},
  publisher    = {{IEEE}},
  year         = {2021},
  url          = {https://doi.org/10.1109/ICDE51399.2021.00157},
  doi          = {10.1109/ICDE51399.2021.00157}
}
```

# Acknowledgments
This work has been supported by the Hellenic Foundation for Research and Innovation (H.F.R.I.) under the "2nd Call for H.F.R.I. Research Projects to support Faculty Members & Researchers" (Project Number: 2757).
![alt text](https://github.com/dTsitsigkos/two-layer/blob/main/ELIDEK.jpeg)
