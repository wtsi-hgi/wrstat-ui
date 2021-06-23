# wrstat
Efficiently walk a tree then get stats of all files in parallel across many
nodes using wr.

This is a replacement for https://github.com/wtsi-hgi/mpistat that doesn't use
any run-time inter-process communication, for greater reliability and speed.

It relies on https://github.com/VertebrateResequencing/wr to do the scheduling
of its work.
## Build
```
git clone https://github.com/wtsi-ssg/wrstat/
cd wrstat
make
```
## Useage
As a user that has permission to see all files on the disks of interest:

```
wr manager start -s lsf
wrstat multi -w /working/dir -f /final/dir /disk1 /disk2
wr status -i wrstat -z -o s
```