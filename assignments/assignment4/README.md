# SET SIMILARITY JOIN

**SET SIMILARITY JOIN** is an algorithm to compute all pairs of similar sets from a collection of sets.

### NOTABLE FILES
_Screenshots/_ : Screenshots of the program running on AWS
_Optimization.pdf_ : Brief description of optimization techniques used.
_Runtime.jpg_ : Graph of runtime with different number of clusters.
_SetSimJoin.scala_ : The file used to generate the .jar file

### AMAZON S3 SETUP
1. Open the Amazon S3 console
2. Click "Create Bucket"
3. Enter a unique bucket name (i.e. _comp9313.z5136414_)
4. Select region (i.e. _Asia Pacific (Sydney)_)
5. Click "Create"
6. Create a folder (i.e. _project4_) in this bucket for holding the input files
7. Extract _sample/flickr_large.zip_ (containing flickr_london.txt)
8. Upload _flickr_london.txt_ and _setsimjoin_2.11-1.0.jar_ into the folder

### AMAZON EMR SETUP
1. Open the Amazon EMR console
2. Choose "Create cluster" and fill in the cluster information
3. Under **General Configuration**
Cluster name: _any_
S3 folder: _use default_, the folder is used to store the logs
Launch mode: _Step execution_, so the instances will be terminated once the task is completed
4. Under **Add steps**
Step type: _Custom JAR_, then click "Configure"
Name: _any_
JAR location: _locate the setsimjoin_2.11-1.0.jar in your S3 folder_
Arguments: _path_to_flickr_london path_to_output_folder similarity_threshold_ (i.e _s3://comp9313.z5136414/project4/flickr_london.txt s3://comp9313.z5136414/project4/output 0.85_)
Action on failure: _Terminate cluster_
5. Under **Hardware configuration**
Instance type: _m3.xlarge_
Number of instances: _2, 3, or 4_
6. Click "Create cluster"