# SQLVer
SQL passive version tracking, debug logging, and many utilities

SQLVer uses a database trigger to automatically track and log all DDL changes to a SQL database, and provides tools for reviewing historical changes and reverting back to older versions.

It also provides a system for run-time logging (think debug and performance tuning logging), a way to search for a string in all the source code in a database, a way to identify slow queries, a way to identify SQL connections that are hogging resources and blocking access to objects, and more.

SQLVer is written entirely in T-SQL with no external dependencies. It installs via execution of a single .SQL script, creates all of it's objects neatly within a sqlver schema in the current database, and can be uninstalled with a single command.

Since the original release, SQLVer numerous other utility procedures and functions have been added to SQLVer, including:  string parsing, CLR assembly building and deploying, HTTP, FTP, and email utilities, geolocation distance calculations, and more.  While the primary purpose of SQLVer is passive version tracking, SQLVer is a convenient place to add useful utility procedures and functions that would be useful for many databases.

Original article about SQLVer published on SQL Sever Central on 1/22/2015.  Originally published on Sourceforge 1/25/2015, this GitHub respository is now the official home of SQLVer.

See:  http://www.sqlservercentral.com/articles/Version+Control+Systems+(VCS)/119029/


#Installation Notes

SQLVer uses two Microsoft tools:  the command-line  compiler csc.exe and the strong-name tool sn.exe

Though Microsoft does make these available, Microsoft's approach to installation is not as simple as it should be.

csc.exe (C# command-line compiler)

csc.exe is shipped the following ways:

Part of the .NET framework installed in Windows (i.e. in C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe for example)

HOWEVER, this is not a current version of the compiler, which means it has limitations

This compiler is provided as part of the Microsoft (R) .NET Framework, but only supports language versions up to C# 5, which is no longer the latest version. For compilers that support newer versions of the C# programming language, see http://go.microsoft.com/fwlink/?LinkID=533240

Part of Visual Studio

HOWEVER we really don't want to install VisualStudio on our SQL server machine

Part of the Windows Software Development Kit (SDK)

HOWEVER we really don't want to install the SDK on our SQL Server machine

Install stand-alone, utilizing the nuget package manager

Our preferred option

i.e. in a command prompt, running nuget install Microsoft.Net.Compilerswill create a folder under the current folder, and will download and the compiler (including csc.exe) to that folder

You will need to download nuget.exe from NuGet Gallery | Downloads and move that file to the folder C:\SQLVer\Tools before running it.

You can also execute this Powershell script to download nuget.exe

(new-object System.Net.WebClient).DownloadFile(
  'https://dist.nuget.org/win-x86-commandline/latest/nuget.exe',
  'C:\SQLVer\Tools\nuget.exe'
)

Regardless of how or where you install csc.exe, you will need a symbolic directory link from C:\SQLVer\tools\csc to the folder in which the csc.exe file is actually located.

mklink /D C:\SQLVer\Tools\csc "C:\SQLVer\Tools\Microsoft.Net.Compilers.4.2.0\tools"

You can find more information about how to use csc.exe here:  Command-line build with csc.exe and more information about symbolic links here:  The Complete Guide to Creating Symbolic Links (aka Symlinks) on Windows

sn.exe (Strong Name tool)

sn.exe is used to sign a file with a cryptographically strong name.  This is needed to properly install the CLR DLL assemblies that we compile into SQL CLR assemblies.

sn.exe is part of the Microsoft netfx tools ( .NET Framework Tools)

You might think that sn.exe would be shipped with Windows.  (Or, at least with the csc.exe compiler, or MSBuild.  But it is not.  It seems it is only in the .NET Framework Dev Pack.

You can check for an existing sn.exe on your server from a command prompt:

cd \
dir sn.exe /s

If there is an sn.exe on your machine, GREAT!  Simply create a symbolic directory link to that folder:

mklink /D C:\SQLVer\Tools\netfx "C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"

If there is not an sn.exe on your machine, you will need to get a copy.  You may be able to copy this file from a different machine.  (If you find a copy of sn.exe somewhere, and there is a corresponding sn.exe.config file present, copy the sn.exe.config file as well.)

If you can find a copy of sn.exe, create a folder  C:\SQLVer\Tools\netfx  and copy sn.exe (and sn.exe.config if present) to that folder.

The reason this is all so crazy: sn.exe is tiny:  just 339 K ... but the compressed installer for the Developer Pack (aka SDK) is more than 140 MB, and when installed, is even larger still!!!  We just want the tiny sn.exe file.

This is a long-standing limitation / problem with .NET Framework Tools and deploying to a production server.  See .NET 4 gacutil on production server and especially the answer from MarkdeCates. 

 If you cannot find a copy of sn.exe, you may need to install the .NET Framework--either on the server, or on a different machine.

Visit https://dotnet.microsoft.com/download



Click on the "All .NET Framework downloads.  Or, directly visit Download .NET Framework 4.8 | Free official downloads and then click on Offline Installer Developer Pack  ( or go directly to https://dotnet.microsoft.com/download/dotnet-framework/thank-you/net48-developer-pack-offline-installer)

You can perform the install on a different machine if you want, and just copy the sn.exe file to the server.  Or, you can do a full install on the server itself.  (Unfortunately, there does not seem to be a way to unpack the installer's files without installing, and there does not seem to be an .iso download option available.)

Once you have the sn.exe you can create a folder C:\SQLVer\Tools\netfx and copy sc.exe there.  Then you can uninstall the developer pack.  (Copy sn.exe.config also, if it is present.)

Note:  after installing, this "Developer Pack" shows up in the list of installed applications as "Microsoft.NET Framework 4.8 SDK".  And there may be other related applications installed by the installer.  You can uninstall all of these.  (You can sort by date to help you see what you can uninstall.)

