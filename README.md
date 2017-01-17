# SQLVer
SQL passive version tracking, debug logging, and many utilities

SQLVer uses a database trigger to automatically track and log all DDL changes to a SQL database, and provides tools for reviewing historical changes and reverting back to older versions.

It also provides a system for run-time logging (think debug and performance tuning logging), a way to search for a string in all the source code in a database, a way to identify slow queries, a way to identify SQL connections that are hogging resources and blocking access to objects, and more.

SQLVer is written entirely in T-SQL with no external dependencies. It installs via execution of a single .SQL script, creates all of it's objects neatly within a sqlver schema in the current database, and can be uninstalled with a single command.

Since the original release, SQLVer numerous other utility procedures and functions have been added to SQLVer, including:  string parsing, CLR assembly building and deploying, HTTP, FTP, and email utilities, geolocation distance calculations, and more.  While the primary purpose of SQLVer is passive version tracking, SQLVer is a convenient place to add useful utility procedures and functions that would be useful for many databases.

Original article about SQLVer published on SQL Sever Central on 1/22/2015.  Originally published on Sourceforge 1/25/2015, this GitHub respository is now the official home of SQLVer.

See:  http://www.sqlservercentral.com/articles/Version+Control+Systems+(VCS)/119029/
