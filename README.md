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

To do certain chmod and chown operations on desired paths to bring them in to
line with desired unix groups, create a YAML file like the example ch.yml in the
git repository, and supply it as the --ch option to `wrstat multi`.
For more details on ch, see `wrstat stat -h`.

Generally, use the `-h` option on wrstat and its sub commands for detailed
help.
