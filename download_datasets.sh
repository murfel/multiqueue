#!/usr/bin/env bash

wget -O rome99.gr http://users.diag.uniroma1.it/challenge9/data/rome/rome99.gr
cat rome99.gr | sed -e '/^c/d' | cut -c 3- | tail -c +4 > rome.in
rm rome99.gr

wget -O USA-road-d.NY.gr.gz http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.NY.gr.gz
gunzip USA-road-d.NY.gr.gz
cat USA-road-d.NY.gr | sed -e '/^c/d' | cut -c 3- | tail -c +4 > ny.in
rm USA-road-d.NY.gr