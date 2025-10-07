# two-layer
Two-layer Space-oriented Partitioning for Non-point Data

Source code for the paper 'A Two-Layer Partitioning for Non-Point Spatial Data' from the 37th International Conference on Data Engineering (ICDE) 2021, and the paper 'Two-Layer Space-Oriented Partitioning for Non-Point Data' from IEEE Transactions on Knowledge and Data Engineering (TKDE) 2024.

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
Parameters of main_two_layer and main_two_layer_plus. *Keep in mind** that main_two_layer_plus does not support Disk query and Spatial join:
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

- #####  partition.h

- #####  two_layer.h

- #####  two_layer_plus.h

- #####  main_two_layer.cpp

- #####  main_two_layer_plus.cpp

- #####  main_spatial_join_optimizations.cpp

- #####  main_transformation_spatial_join.cpp

# Cite
```
Dimitrios Tsitsigkos, Panagiotis Bouros, Konstantinos Lampropoulos, Nikos Mamoulis and Manolis Terrovitis, Two-layer Space-oriented Partitioning for Non-point Data, IEEE Transactions on Knowledge and Data Engineering (IEEE TKDE), Vol 36, No 3, March 2024.

Dimitrios Tsitsigkos, Konstantinos Lampropoulos, Panagiotis Bouros, Nikos Mamoulis and Manolis Terrovitis, A Two-layer Partitioning for Non-point Spatial Data, Proceedings of the 37th IEEE International Conference on Data Engineering (IEEE ICDE'21), Chania, Greece, April 19-22, 2021.
```

# Acknowledgments
This work has been supported by the Hellenic Foundation for Research and Innovation (H.F.R.I.) under the "2nd Call for H.F.R.I. Research Projects to support Faculty Members & Researchers" (Project Number: 2757).
![alt text](https://github.com/dTsitsigkos/two-layer/blob/main/ELIDEK.jpeg)
