#!/bin/bash
ssh $1@minotaure.enseeiht.fr 'cd ./hidoop/src ; java hdfs/HdfsServer ./config/server1.xml'
