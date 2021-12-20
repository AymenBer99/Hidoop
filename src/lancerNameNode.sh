#!/bin/bash
ssh $1@dragon.enseeiht.fr 'cd ./hidoop/src ; java hdfs/NameNodeImpl ./config/nameNode.xml'
