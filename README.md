# Overview:
Pipeline for EDGAR dashboard:
This repository contains a pipeline to view how users are accessing EDGAR, including how long they stay and the number of documents they access during the visit.

## Description:

> Programing language: Python
> Main: sessionization.py
> Inputs: inactivity_period.txt log.csv
> Output: sessionization.txt

### Prerequisites

```
python2.7
```

## Usage 

run on either Unix or Linux
```
./run.sh
```

### Approach

Since the size of 'log.scv' can be quite large (up to 1TB), line by line processing approach is utilized to make sure that the method is scalable.
In order to keep track of active sessions, Queue data structure is used.
The use of Queue enables dynamic memory allocation in order to avoid large arrays.
The use of Queue also enables to prioritizes the requests as they show up in 'log.csv'.
   


## Author

* **Abtin Ansari** [author](https://www.linkedin.com/in/abtin-ansari-55b0953a/)

