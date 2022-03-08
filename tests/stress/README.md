# Stress Testing for Diskquota

This directory contains scripts for testing the behavior of Diskquota under heavy workloads. Specifically, we are interested in

- The performance of Diskquota itself for monitoring active tables and updating quota usage, and
- The performance of reading and writing data with Diskquota enabled.

Each script in this directory identifies one potential performance bottleneck and contains code to evaluate its impact on performance.