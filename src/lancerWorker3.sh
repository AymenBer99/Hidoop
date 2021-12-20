#!/bin/bash
ssh $1@hippogriffe.enseeiht.fr 'cd ./hidoop/src ; java ordo/WorkerImpl ./config/worker3.xml'
