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
include CMakeFiles/filereader.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/filereader.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/filereader.dir/flags.make

CMakeFiles/filereader.dir/dbtests/filereader.cc.o: CMakeFiles/filereader.dir/flags.make
CMakeFiles/filereader.dir/dbtests/filereader.cc.o: ../dbtests/filereader.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/markus/Desktop/projects/leveldb-improved/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/filereader.dir/dbtests/filereader.cc.o"
	/usr/bin/g++-7  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/filereader.dir/dbtests/filereader.cc.o -c /home/markus/Desktop/projects/leveldb-improved/dbtests/filereader.cc

CMakeFiles/filereader.dir/dbtests/filereader.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/filereader.dir/dbtests/filereader.cc.i"
	/usr/bin/g++-7 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/markus/Desktop/projects/leveldb-improved/dbtests/filereader.cc > CMakeFiles/filereader.dir/dbtests/filereader.cc.i

CMakeFiles/filereader.dir/dbtests/filereader.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/filereader.dir/dbtests/filereader.cc.s"
	/usr/bin/g++-7 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/markus/Desktop/projects/leveldb-improved/dbtests/filereader.cc -o CMakeFiles/filereader.dir/dbtests/filereader.cc.s

CMakeFiles/filereader.dir/dbtests/filereader.cc.o.requires:

.PHONY : CMakeFiles/filereader.dir/dbtests/filereader.cc.o.requires

CMakeFiles/filereader.dir/dbtests/filereader.cc.o.provides: CMakeFiles/filereader.dir/dbtests/filereader.cc.o.requires
	$(MAKE) -f CMakeFiles/filereader.dir/build.make CMakeFiles/filereader.dir/dbtests/filereader.cc.o.provides.build
.PHONY : CMakeFiles/filereader.dir/dbtests/filereader.cc.o.provides

CMakeFiles/filereader.dir/dbtests/filereader.cc.o.provides.build: CMakeFiles/filereader.dir/dbtests/filereader.cc.o


# Object files for target filereader
filereader_OBJECTS = \
"CMakeFiles/filereader.dir/dbtests/filereader.cc.o"

# External object files for target filereader
filereader_EXTERNAL_OBJECTS =

filereader: CMakeFiles/filereader.dir/dbtests/filereader.cc.o
filereader: CMakeFiles/filereader.dir/build.make
filereader: libleveldb.a
filereader: CMakeFiles/filereader.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/markus/Desktop/projects/leveldb-improved/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable filereader"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/filereader.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/filereader.dir/build: filereader

.PHONY : CMakeFiles/filereader.dir/build

CMakeFiles/filereader.dir/requires: CMakeFiles/filereader.dir/dbtests/filereader.cc.o.requires

.PHONY : CMakeFiles/filereader.dir/requires

CMakeFiles/filereader.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/filereader.dir/cmake_clean.cmake
.PHONY : CMakeFiles/filereader.dir/clean

CMakeFiles/filereader.dir/depend:
	cd /home/markus/Desktop/projects/leveldb-improved/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/markus/Desktop/projects/leveldb-improved /home/markus/Desktop/projects/leveldb-improved /home/markus/Desktop/projects/leveldb-improved/build /home/markus/Desktop/projects/leveldb-improved/build /home/markus/Desktop/projects/leveldb-improved/build/CMakeFiles/filereader.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/filereader.dir/depend

