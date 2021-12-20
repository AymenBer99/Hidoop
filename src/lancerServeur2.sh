#!/bin/bash
ssh $1@succube.enseeiht.fr 'cd ./hidoop/src ; java hdfs/HdfsServer ./config/server2.xml'
