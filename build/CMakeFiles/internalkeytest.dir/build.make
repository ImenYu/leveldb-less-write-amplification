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
include CMakeFiles/internalkeytest.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/internalkeytest.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/internalkeytest.dir/flags.make

CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o: CMakeFiles/internalkeytest.dir/flags.make
CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o: ../dbtests/internal_key_test.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/markus/Desktop/projects/leveldb-improved/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o"
	/usr/bin/g++-7  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o -c /home/markus/Desktop/projects/leveldb-improved/dbtests/internal_key_test.cc

CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.i"
	/usr/bin/g++-7 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/markus/Desktop/projects/leveldb-improved/dbtests/internal_key_test.cc > CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.i

CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.s"
	/usr/bin/g++-7 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/markus/Desktop/projects/leveldb-improved/dbtests/internal_key_test.cc -o CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.s

CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o.requires:

.PHONY : CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o.requires

CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o.provides: CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o.requires
	$(MAKE) -f CMakeFiles/internalkeytest.dir/build.make CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o.provides.build
.PHONY : CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o.provides

CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o.provides.build: CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o


# Object files for target internalkeytest
internalkeytest_OBJECTS = \
"CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o"

# External object files for target internalkeytest
internalkeytest_EXTERNAL_OBJECTS =

internalkeytest: CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o
internalkeytest: CMakeFiles/internalkeytest.dir/build.make
internalkeytest: libleveldb.a
internalkeytest: CMakeFiles/internalkeytest.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/markus/Desktop/projects/leveldb-improved/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable internalkeytest"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/internalkeytest.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/internalkeytest.dir/build: internalkeytest

.PHONY : CMakeFiles/internalkeytest.dir/build

CMakeFiles/internalkeytest.dir/requires: CMakeFiles/internalkeytest.dir/dbtests/internal_key_test.cc.o.requires

.PHONY : CMakeFiles/internalkeytest.dir/requires

CMakeFiles/internalkeytest.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/internalkeytest.dir/cmake_clean.cmake
.PHONY : CMakeFiles/internalkeytest.dir/clean

CMakeFiles/internalkeytest.dir/depend:
	cd /home/markus/Desktop/projects/leveldb-improved/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/markus/Desktop/projects/leveldb-improved /home/markus/Desktop/projects/leveldb-improved /home/markus/Desktop/projects/leveldb-improved/build /home/markus/Desktop/projects/leveldb-improved/build /home/markus/Desktop/projects/leveldb-improved/build/CMakeFiles/internalkeytest.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/internalkeytest.dir/depend

