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
CMAKE_SOURCE_DIR = /home/markus/Desktop/projects/leveldb-improved

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/markus/Desktop/projects/leveldb-improved/build

# Include any dependencies generated for this target.
include CMakeFiles/tbltest.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/tbltest.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/tbltest.dir/flags.make

CMakeFiles/tbltest.dir/dbtests/table_test.cc.o: CMakeFiles/tbltest.dir/flags.make
CMakeFiles/tbltest.dir/dbtests/table_test.cc.o: ../dbtests/table_test.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/markus/Desktop/projects/leveldb-improved/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/tbltest.dir/dbtests/table_test.cc.o"
	/usr/bin/g++-7  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/tbltest.dir/dbtests/table_test.cc.o -c /home/markus/Desktop/projects/leveldb-improved/dbtests/table_test.cc

CMakeFiles/tbltest.dir/dbtests/table_test.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/tbltest.dir/dbtests/table_test.cc.i"
	/usr/bin/g++-7 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/markus/Desktop/projects/leveldb-improved/dbtests/table_test.cc > CMakeFiles/tbltest.dir/dbtests/table_test.cc.i

CMakeFiles/tbltest.dir/dbtests/table_test.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/tbltest.dir/dbtests/table_test.cc.s"
	/usr/bin/g++-7 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/markus/Desktop/projects/leveldb-improved/dbtests/table_test.cc -o CMakeFiles/tbltest.dir/dbtests/table_test.cc.s

CMakeFiles/tbltest.dir/dbtests/table_test.cc.o.requires:

.PHONY : CMakeFiles/tbltest.dir/dbtests/table_test.cc.o.requires

CMakeFiles/tbltest.dir/dbtests/table_test.cc.o.provides: CMakeFiles/tbltest.dir/dbtests/table_test.cc.o.requires
	$(MAKE) -f CMakeFiles/tbltest.dir/build.make CMakeFiles/tbltest.dir/dbtests/table_test.cc.o.provides.build
.PHONY : CMakeFiles/tbltest.dir/dbtests/table_test.cc.o.provides

CMakeFiles/tbltest.dir/dbtests/table_test.cc.o.provides.build: CMakeFiles/tbltest.dir/dbtests/table_test.cc.o


# Object files for target tbltest
tbltest_OBJECTS = \
"CMakeFiles/tbltest.dir/dbtests/table_test.cc.o"

# External object files for target tbltest
tbltest_EXTERNAL_OBJECTS =

tbltest: CMakeFiles/tbltest.dir/dbtests/table_test.cc.o
tbltest: CMakeFiles/tbltest.dir/build.make
tbltest: libleveldb.a
tbltest: CMakeFiles/tbltest.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/markus/Desktop/projects/leveldb-improved/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable tbltest"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/tbltest.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/tbltest.dir/build: tbltest

.PHONY : CMakeFiles/tbltest.dir/build

CMakeFiles/tbltest.dir/requires: CMakeFiles/tbltest.dir/dbtests/table_test.cc.o.requires

.PHONY : CMakeFiles/tbltest.dir/requires

CMakeFiles/tbltest.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/tbltest.dir/cmake_clean.cmake
.PHONY : CMakeFiles/tbltest.dir/clean

CMakeFiles/tbltest.dir/depend:
	cd /home/markus/Desktop/projects/leveldb-improved/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/markus/Desktop/projects/leveldb-improved /home/markus/Desktop/projects/leveldb-improved /home/markus/Desktop/projects/leveldb-improved/build /home/markus/Desktop/projects/leveldb-improved/build /home/markus/Desktop/projects/leveldb-improved/build/CMakeFiles/tbltest.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/tbltest.dir/depend

