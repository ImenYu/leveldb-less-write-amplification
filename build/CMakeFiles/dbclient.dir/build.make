# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.10

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/markus/Desktop/projects/leveldb-less-write-amplification

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/markus/Desktop/projects/leveldb-less-write-amplification/build

# Include any dependencies generated for this target.
include CMakeFiles/dbclient.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/dbclient.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/dbclient.dir/flags.make

CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o: CMakeFiles/dbclient.dir/flags.make
CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o: ../dbtests/dbclient.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/markus/Desktop/projects/leveldb-less-write-amplification/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o"
	/usr/bin/g++-7  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o -c /home/markus/Desktop/projects/leveldb-less-write-amplification/dbtests/dbclient.cc

CMakeFiles/dbclient.dir/dbtests/dbclient.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/dbclient.dir/dbtests/dbclient.cc.i"
	/usr/bin/g++-7 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/markus/Desktop/projects/leveldb-less-write-amplification/dbtests/dbclient.cc > CMakeFiles/dbclient.dir/dbtests/dbclient.cc.i

CMakeFiles/dbclient.dir/dbtests/dbclient.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/dbclient.dir/dbtests/dbclient.cc.s"
	/usr/bin/g++-7 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/markus/Desktop/projects/leveldb-less-write-amplification/dbtests/dbclient.cc -o CMakeFiles/dbclient.dir/dbtests/dbclient.cc.s

CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o.requires:

.PHONY : CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o.requires

CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o.provides: CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o.requires
	$(MAKE) -f CMakeFiles/dbclient.dir/build.make CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o.provides.build
.PHONY : CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o.provides

CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o.provides.build: CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o


# Object files for target dbclient
dbclient_OBJECTS = \
"CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o"

# External object files for target dbclient
dbclient_EXTERNAL_OBJECTS =

dbclient: CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o
dbclient: CMakeFiles/dbclient.dir/build.make
dbclient: libleveldb.a
dbclient: CMakeFiles/dbclient.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/markus/Desktop/projects/leveldb-less-write-amplification/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable dbclient"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/dbclient.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/dbclient.dir/build: dbclient

.PHONY : CMakeFiles/dbclient.dir/build

CMakeFiles/dbclient.dir/requires: CMakeFiles/dbclient.dir/dbtests/dbclient.cc.o.requires

.PHONY : CMakeFiles/dbclient.dir/requires

CMakeFiles/dbclient.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/dbclient.dir/cmake_clean.cmake
.PHONY : CMakeFiles/dbclient.dir/clean

CMakeFiles/dbclient.dir/depend:
	cd /home/markus/Desktop/projects/leveldb-less-write-amplification/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/markus/Desktop/projects/leveldb-less-write-amplification /home/markus/Desktop/projects/leveldb-less-write-amplification /home/markus/Desktop/projects/leveldb-less-write-amplification/build /home/markus/Desktop/projects/leveldb-less-write-amplification/build /home/markus/Desktop/projects/leveldb-less-write-amplification/build/CMakeFiles/dbclient.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/dbclient.dir/depend

