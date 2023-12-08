# TESTING YOUR LOGGING FUNCTIONALITY

For convenience, we have included some files to help as you work on your project.

The test C++ source file is named `test_logging_restore.cpp`. It is a stripped down version of `test.cpp` that we expect you to modify to include your logging functionality (with proper initialization).

There are two parameters that your logging class will need to accept (depending on your logging implementation as discussed in your design document), `persistence_granularity` and `checkpoint_granularity`. `test_logging_restore.cpp` adds both of those as required parameters when running the binary as `-p` and `-c`. Please be sure they exist when running the program.

## BUILDING THE TEST PROGRAM


You can compile the tests using the provided Makefile: `make clean && make test_logging_restore`.

Your Makefile may differ slightly as you will need to ensure your logging is compiled. Please be sure to modify the Makefile properly so that the logging functionality you create is also compiled and found.

## RUNNING THE TEST PROGRAM

Included with the project is a text file that will run 10,000 inserts and queries followed by 400 queries (and their correct results) for the final tree. The file is named `test_inputs.txt`. To use this file to test your tree, use a command like so:

```bash
./test_logging_restore -m test -d your_temp_dir -i test_inputs.txt -t 10400 -c 50 -p 200
```

NOTE: `-c 50` will set the checkpoint granularity to 50, and `-p 200` will set the persistence granularity to 200.

For the research of adpative Be-tree, we also add some new options in the command line instruction. You can find the detailed information and examples of how to set these paramaters via command line instruction in the file designed_test_result.md.

You can also add an optional -o flag to make sure the program has gone through all 10,400 operations and see the results of the various queries (e.g. `-o test_outputs.txt`).

## RUNNING THE TEST SCRIPT

We have also included a test script that will test crashing the program and resuming operation.

You can run the script by calling `bash ./TestScript.sh`. This will run through the test of running a crash and recovery. It will end the first test by making 400 queries at the end of the program based on the correct final state of the program.

When this test finishes, it will give you some results regarding the percentage of incorrect queries. Due to the potential of a crash losing a portion of your checkpoint data (as part of the checkpoint granularity), the final percentages may not be zero. We are looking for a value as close to zero as possible.
