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

Or if your user account has the ability to sudo without a password when
executing the wrstat executable, add the --sudo option to `wrstat multi`.
If you're running `wr` and `wrstat` without custom configurations, `wr` must
be run as the root user, because `wrstat` will generate `sudo` commands that
attempt to connect to a port number based on the user, which will be the root
user, not the running user.

To do certain chmod and chown operations on desired paths to bring them in to
line with desired unix groups, create a YAML file like the example ch.yml in the
git repository, and supply it as the --ch option to `wrstat multi`.
For more details on ch, see `wrstat stat -h`.

Generally, use the `-h` option on wrstat and its sub commands for detailed
help.
