# PerfDebug
PerfDebug: Performance Debugging of Computation Skew in Dataflow Systems (SoCC 2019)

## Summary of PerfDebug
Performance is a key factor for big data applications, and much research has been devoted to optimizing these applications. While prior work can diagnose and correct data skew, the problem of computation skew—abnormally high computation costs for a small subset of input data—has been largely overlooked. Computation skew commonly occurs in real-world applications and yet no tool is available for developers to pinpoint underlying causes. 

To enable a user to debug applications that exhibit computation skew, we develop a post-mortem performance debugging tool. PerfDebug automatically finds input records responsible for such abnormalities in a big data application by reasoning about deviations in performance metrics such as job execution time, garbage collection time, and serialization time. The key to PerfDebug’s success is a data provenance-based technique that computes and propagates record-level computation latency to keep track of abnormally expensive records throughout the pipeline. Finally, the input records that have the largest latency contributions are presented to the user for bug fixing. We evaluate PerfDebug via in-depth case studies and observe that remediation such as removing the single most expensive record or simple code rewrite can achieve up to 16X performance improvement.

## Team
This project is developed by Professor [Miryung Kim](http://web.cs.ucla.edu/~miryung/)'s Software Engineering and Analysis Laboratory at UCLA. 
If you encounter any problems, please open an issue or feel free to contact us:

[Jason Teoh](http://https://jiateoh.github.io/): PhD student, jteoh@cs.ucla.edu;

[Muhammad Ali Gulzar](https://people.cs.vt.edu/~gulzar/): Professor at Virginia Tech, gulzar@cs.vt.edu;

[Guoqing Harry Xu](http://web.cs.ucla.edu/~harryxu/): Professor at UCLA, harryxu@cs.ucla.edu;

[Miryung Kim](http://web.cs.ucla.edu/~miryung/): Professor at UCLA, miryung@cs.ucla.edu;

## How to cite 
Please refer to our SoCC 2019 paper, [PerfDebug: Performance Debugging of Computation Skew in Dataflow Systems](http://web.cs.ucla.edu/~miryung/Publications/socc2019-perfdebug-teoh.pdf) for more details. 

### Bibtex  
@inproceedings{10.1145/3357223.3362727,
author = {Teoh, Jason and Gulzar, Muhammad Ali and Xu, Guoqing Harry and Kim, Miryung},
title = {PerfDebug: Performance Debugging of Computation Skew in Dataflow Systems},
year = {2019},
isbn = {9781450369732},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3357223.3362727},
doi = {10.1145/3357223.3362727},
abstract = {Performance is a key factor for big data applications, and much research has been
devoted to optimizing these applications. While prior work can diagnose and correct
data skew, the problem of computation skew---abnormally high computation costs for
a small subset of input data---has been largely overlooked. Computation skew commonly
occurs in real-world applications and yet no tool is available for developers to pinpoint
underlying causes.To enable a user to debug applications that exhibit computation
skew, we develop a post-mortem performance debugging tool. PerfDebug automatically
finds input records responsible for such abnormalities in a big data application by
reasoning about deviations in performance metrics such as job execution time, garbage
collection time, and serialization time. The key to PerfDebug's success is a data
provenance-based technique that computes and propagates record-level computation latency
to keep track of abnormally expensive records throughout the pipeline. Finally, the
input records that have the largest latency contributions are presented to the user
for bug fixing. We evaluate PerfDebug via in-depth case studies and observe that remediation
such as removing the single most expensive record or simple code rewrite can achieve
up to 16X performance improvement.},
booktitle = {Proceedings of the ACM Symposium on Cloud Computing},
pages = {465–476},
numpages = {12},
keywords = {fault localization, data intensive scalable computing, data provenance, Performance debugging, big data systems},
location = {Santa Cruz, CA, USA},
series = {SoCC '19}
}

[DOI Link](https://doi.org/10.1145/3357223.3362727)

## Other Notes:
* This project originated as a fork of [BigDebug](https://github.com/UCLA-SEAL/BigDebug), which itself is inherently based on Apache Spark. In particular, the original project code can be found in BigDebug's [`perf-ignite` branch](https://github.com/UCLA-SEAL/BigDebug/tree/perfdebug-ignite).
* Because the evaluation benchmarks depend directly on different variants of Spark (Standard Spark vs Titian vs PerfDebug), these are stored in other repositories.
  * Links pending.
