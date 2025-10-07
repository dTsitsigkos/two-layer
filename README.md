# Two-layer Space-oriented Partitioning for Non-point Data

Source code for the paper "**A Two-Layer Partitioning for Non-Point Spatial Data**" from the 37th International Conference on Data Engineering (ICDE) 2021, and the paper "**Two-Layer Space-Oriented Partitioning for Non-Point Data**" from IEEE Transactions on Knowledge and Data Engineering (TKDE) 2024.

### Dependencies
- g++/gcc 
- OpenMP

### Datasets


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
| -h | Print the manual|


Parameters of main_spatial_join_optimizations:
| Parameters | README |
| ------ | ------ |
| -p | The number of partitions to be used |
| -b | Mini joins baseline |
| -u | Mini joins and unnecessary comparisons |
| -r | Mini joins and redundant comparisons |
| -c | Mini joins, unnecessary and redundant comparisons |
| -a | Mini joins, unnecessary and redundant comparisons and storage optimizations |
| -h | Print the manual|

Parameters of main_transformation_spatial_join:
| Parameters | README |
| ------ | ------ |
| -p | The number of partions for the first dataset per dimension |
| -s | The number of partions for the second dataset per dimension |
| -m | Materialized spatial join algorithm |
| -n | Non-materialized spatial join algorithm |
| -h | Print the manual|


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

     - ###### Spatial join

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
Dimitrios Tsitsigkos, Panagiotis Bouros, Konstantinos Lampropoulos, Nikos Mamoulis and Manolis Terrovitis, "Two-layer Space-oriented Partitioning for Non-point Data", IEEE Transactions on Knowledge and Data Engineering (IEEE TKDE), Vol 36, No 3, March 2024.
Dimitrios Tsitsigkos, Konstantinos Lampropoulos, Panagiotis Bouros, Nikos Mamoulis and Manolis Terrovitis, "A Two-layer Partitioning for Non-point Spatial Data", Proceedings of the 37th IEEE International Conference on Data Engineering (IEEE ICDE'21), Chania, Greece, April 19-22, 2021.
```

# Acknowledgments
This work has been supported by the Hellenic Foundation for Research and Innovation (H.F.R.I.) under the "2nd Call for H.F.R.I. Research Projects to support Faculty Members & Researchers" (Project Number: 2757).
![alt text](https://github.com/dTsitsigkos/two-layer/blob/main/ELIDEK.jpeg)
