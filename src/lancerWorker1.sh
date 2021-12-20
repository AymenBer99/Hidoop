#!/bin/bash
ssh $1@minotaure.enseeiht.fr 'cd ./hidoop/src ; java ordo/WorkerImpl ./config/worker1.xml'
