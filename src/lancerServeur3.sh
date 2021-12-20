#!/bin/bash
ssh $1@hippogriffe.enseeiht.fr 'cd ./hidoop/src ; java hdfs/HdfsServer ./config/server3.xml'
