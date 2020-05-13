#!/usr/bin/env bash

wget -O rome99.gr http://users.diag.uniroma1.it/challenge9/data/rome/rome99.gr
cat rome99.gr | tail -n +15 | cut -c 3- | tail -c +4 > rome.in
rm rome99.gr