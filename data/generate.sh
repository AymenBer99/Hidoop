#!/bin/bash
cp filesample.txt /tmp/data.txt

# generate 2^$1 times file filesample.txt
for ((n=0;n<$1;n++))
do
cat /tmp/data.txt /tmp/data.txt > /tmp/temp
mv /tmp/temp /tmp/data.txt
done
