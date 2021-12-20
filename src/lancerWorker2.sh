#!/bin/bash
ssh $1@succube.enseeiht.fr 'cd ./hidoop/src ; java ordo/WorkerImpl ./config/worker2.xml'
