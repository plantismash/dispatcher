antiSMASH compute job dispatcher
================================

This is the job dispatcher powering http://antismash.secondarymetabolites.org/

Installation
------------

```
pip install -r requirements.txt
```

The dispatcher also requires a Redis database to work, it looks on
Redis's port on localhost per default.

Additionally, make sure run_antismash.py is in your path with all its dependencies installed.

Running the dispatcher
----------------------

The whole dispatcher consists of a number of scripts, listed here in the order of importance.

**runSMASH**

The main dispatcher. You _need_ this in order to start antiSMASH jobs.
Use `--help` to see all available options. The two important options that need to match
the corresponding settings in the websmash web UI configuration are `--queue` and `--workdir`.
If you want to run multiple dispatchers, you will want to give all of them a unique name using
`--name`. You also might want to limit the numbers of CPUs that can be used with `--cpus`.

**watchStatus**

The status updater. Use this to get a detailed job status display in the database and web UI.
Make sure `--queue` and `--statusdir` are set up correctly. This script uses the Linux inotify API,
so it might not work on network filesystems that are modified remotely.

**smashctl**

A tool to help manage running dispatchers and the web UI. Comes with the subcommands `job` to manage jobs,
`control` to manage dispatchers and `notice` to manage notices displayed on the web UI.

**cleanup_jobs**

Remove data for timed-out jobs. Again, `--queue` and `--workdir` are the important parameters to sync up
with the rest of the install.

License
-------

The dispatcher code is licensed under the GNU GPL version 3.
See the `LICENSE.txt` file for details.
