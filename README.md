# SQLVer
SQL passive version control, debug logging, and many utilities

SQLVer automatically tracks and logs all DDL changes to a SQL database, and provides tools for reviewing historical changes and reverting back to older versions.

It also provides a system for run-time logging (think debug and performance tuning logging), a way to search for a string in all the source code in a database, a way to identify slow queries, a way to identify SQL connections that are hogging resources and blocking access to objects, and more.

SQLVer is written entirely in T-SQL with no external dependencies. It installs via execution of a single .SQL script, creates all of it's objects neatly within a sqlver schema in the current database, and can be uninstalled with a single command.

Original article about SQLVer published on SQL Sever Central on 1/22/2015:  http://www.sqlservercentral.com/articles/Version+Control+Systems+(VCS)/119029/
Originally published on Sourceforge 1/25/2015, this GitHub respository is now the official home of SQLVer.
