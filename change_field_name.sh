#!/bin/bash
for filename in *.csv; do
        [ -e "$filename" ] || continue
        awk 'NR==1{$0=toupper($0)} 1'  $filename > temp_file && mv temp_file $filename
done
