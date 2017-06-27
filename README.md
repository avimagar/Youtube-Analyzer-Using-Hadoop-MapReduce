# Youtube-Analyzer-Using-Hadoop-MapReduce
Project on analyzing youtube dataset with the help of hadoop mapreduce framework.

----------------------------------------------------------Dataset Description -----------------------------------------------------------------

| Video ID | Uploader | Age (In days) | Category | length | Views | Rate | Ratings | Comments | Related ID's |
| -------- | -------------------- | ------------- | -------- | ------ | ----- | ---- | ------- | -------- | ------------ |
| 11 character unique string | User Name |  Days between the date when the video was uploaded and YouTube's establishment |  Video category | Video length |  Number of the views | Video rate |  Number of the ratings | Number of the comments | 20 related video IDs |



After running a job on dataset use following commands to filter out the results to find top 5 categories or videos:---

hadoop fs -cat /Output_Directory_Name/part-00000 | sort –n –k2 –r | head –n5

