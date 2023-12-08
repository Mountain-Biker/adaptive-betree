// This test program performs a series of inserts, deletes, updates,
// and queries to a betree.  It performs the same sequence of
// operatons on a std::map.  It checks that it always gets the same
// result from both data structures.

// The program takes 1 command-line parameter -- the number of
// distinct keys it can use in the test.

// The values in this test are strings.  Since updates use operator+
// on the values, this test performs concatenation on the strings.

#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <deque>
#include <cstdlib> 
#include <unistd.h> 
#include <sys/stat.h>
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <dirent.h>

// INCLUDE YOUR LOGGING FILE HERE
#include "betree.hpp"

void timer_start(uint64_t &timer) {
    struct timeval t;
    assert(!gettimeofday(&t, NULL));
    timer -= 1000000 * t.tv_sec + t.tv_usec;
}

void timer_stop(uint64_t &timer) {
    struct timeval t;
    assert(!gettimeofday(&t, NULL));
    timer += 1000000 * t.tv_sec + t.tv_usec;
}

int next_command(FILE *input, int *op, uint64_t *arg) {
    int ret;
    char command[64];

    ret = fscanf(input, "%s %ld", command, arg);
    if (ret == EOF)
        return EOF;
    else if (ret != 2) {
        fprintf(stderr, "Parse error\n");
        exit(3);
    }

    if (strcmp(command, "Inserting") == 0) {
        *op = 0;
    } else if (strcmp(command, "Updating") == 0) {
        *op = 1;
    } else if (strcmp(command, "Deleting") == 0) {
        *op = 2;
    } else if (strcmp(command, "Query") == 0) {
        *op = 3;
        if (1 != fscanf(input, " -> %s", command)) {
            fprintf(stderr, "Parse error\n");
            exit(3);
        }
    } else {
        fprintf(stderr, "Unknown command: %s\n", command);
        exit(1);
    }

    return 0;
}

#define DEFAULT_TEST_MAX_NODE_SIZE (1ULL << 6)
#define DEFAULT_TEST_MIN_FLUSH_SIZE (DEFAULT_TEST_MAX_NODE_SIZE / 4)
#define DEFAULT_TEST_CACHE_SIZE (4)
#define DEFAULT_TEST_NDISTINCT_KEYS (1ULL << 10)
#define DEFAULT_TEST_NOPS (1ULL << 12)

void usage(char *name) {
    std::cout
        << "Usage: " << name << " [OPTIONS]" << std::endl
        << "Tests the betree implementation" << std::endl
        << std::endl
        << "Options are" << std::endl
        << "  Required:" << std::endl
        << "    -d <backing_store_directory>                    [ default: "
           "none, parameter is required ]"
        << std::endl
        << "    -m  <mode>  (test or benchmark-<mode>)          [ default: "
           "none, parameter required ]"
        << std::endl
        << "        benchmark modes:" << std::endl
        << "          upserts    " << std::endl
        << "          queries    " << std::endl
        << "  Betree tuning parameters:" << std::endl
        << "    -N <max_node_size>            (in elements)     [ default: "
        << DEFAULT_TEST_MAX_NODE_SIZE << " ]" << std::endl
        << "    -f <min_flush_size>           (in elements)     [ default: "
        << DEFAULT_TEST_MIN_FLUSH_SIZE << " ]" << std::endl
        << "    -C <max_cache_size>           (in betree nodes) [ default: "
        << DEFAULT_TEST_CACHE_SIZE << " ]" << std::endl
        << "  Options for both tests and benchmarks" << std::endl
        << "    -k <number_of_distinct_keys>                    [ default: "
        << DEFAULT_TEST_NDISTINCT_KEYS << " ]" << std::endl
        << "    -t <number_of_operations>                       [ default: "
        << DEFAULT_TEST_NOPS << " ]" << std::endl
        << "    -s <random_seed>                                [ default: "
           "random ]"
        << std::endl
        << "  Test scripting options" << std::endl
        << "    -o <output_script>                              [ default: no "
           "output ]"
        << std::endl
        << "    -i <script_file>                                [ default: "
           "none ]"
        << std::endl
        << "  ====REQUIRED PARAMETERS FOR PROJECT 2====" << std::endl
        << "    -p <persistence_granularity>  (an integer)" << std::endl
        << "    -c <checkpoint_granularity>   (an integer)" << std::endl;
}

int test(betree<uint64_t, std::string> &b, 
         double write_heavy_epsilon, 
         double read_heavy_epsilon, 
         bool shorten_betree, 
         double& shorten_betree_time, 
         uint64_t nops,
         uint64_t number_of_distinct_keys, FILE *script_input,
         FILE *script_output) {

    int write_counter = 0;
    int read_counter = 0;
    int granularity = 500;
    int workload_state = 0;
    int state = b.get_state();
    
    for (unsigned int i = 0; i < nops; i++) {
        // if state = 7 it means betree is in fixed mode, 
        // the value of epsilon do not adjust to the change of workload pattern
        if (state != 7 && i != 0 && i % granularity == 0) {
            
            double write_ratio = write_counter * 1.0 / (write_counter + read_counter);
            // workload is write heavy
            if (write_ratio > 0.7 && state > 0) {
                state--;
            }

            // workload is read heavy
            if (write_ratio < 0.3 && state < 3) {
                state++;
            }

            // if current workload is write heavy and 
            // previous betree state is not write heavy
            if (state == 0 && b.get_state() == 3 ) {
                std::cout << "betree state (before change state): " << b.get_state() << std::endl;
                std::cout << "betree epsilon (before change state): " << b.get_epsilon() << std::endl;
                std::cout << "betree pivot upper bound (before change state): " << b.get_pivot_upper_bound() << std::endl;

                b.set_state(state);
                b.set_epsilon(write_heavy_epsilon);

                std::cout << "operation number : " << i << ", write_ratio: " << write_ratio << std::endl;
                std::cout << "betree state: " << b.get_state() << std::endl;
                std::cout << "betree epsilon: " << b.get_epsilon() << std::endl;
                std::cout << "betree pivot upper bound: " << b.get_pivot_upper_bound() << std::endl;
                std::cout << "betree message upper bound: " << b.get_message_upper_bound() << std::endl;
            
            }

            // if current workload is read heavy and 
            // previous betree state is not read heavy
            if (state == 3 && b.get_state() == 0 ) {
                std::cout << "betree state (before change state): " << b.get_state() << std::endl;
                std::cout << "betree epsilon (before change state): " << b.get_epsilon() << std::endl;
                std::cout << "betree pivot upper bound (before change state): " << b.get_pivot_upper_bound() << std::endl;
                double average_nodes_height = b.calculateAverageHeight();
                std::cout << "average betree nodes height(before shortening betree): " << average_nodes_height << std::endl;

                b.set_state(state);
                b.set_epsilon(read_heavy_epsilon);
                if (shorten_betree) {
                    uint64_t timer = 0;
                    timer_start(timer);
                    b.shorten_betree();
                    timer_stop(timer);
                    shorten_betree_time = timer * 1.0 / 1000000;
                }
                
                // b.shorten_root_node();
                average_nodes_height = b.calculateAverageHeight();
                std::cout << "average betree nodes height(before shortening betree): " << average_nodes_height << std::endl;

                std::cout << "operation number : " << i << ", write_ratio: " << write_ratio << std::endl;
                std::cout << "betree state :" << b.get_state() << std::endl;
                std::cout << "betree epsilon: " << b.get_epsilon() << std::endl;
                std::cout << "betree pivot upper bound: " << b.get_pivot_upper_bound() << std::endl;
                std::cout << "betree message upper bound: " << b.get_message_upper_bound() << std::endl;
            }
            
            write_counter = 0;
            read_counter = 0;
        }

        printf("%u/%lu\n", i, nops);
        int op;
        uint64_t t;
        if (script_input) {
            int r = next_command(script_input, &op, &t);
            if (r == EOF)
                exit(0);
            else if (r < 0)
                exit(4);
        } else {
            op = rand() % 4;
            t = rand() % number_of_distinct_keys;
        }

        // std::cout << "op: " << op << ", key: " << t << std::endl;

        switch (op) {
            case 0:  // insert
                if (script_output){
                    //printf("Printing insert op!\n");
                    fprintf(script_output, "Inserting %lu\n", t);
                } 
                b.insert(t, std::to_string(t) + ":");
                write_counter++;
                break;
            case 1:  // update
                if (script_output) fprintf(script_output, "Updating %lu\n", t);
                b.update(t, std::to_string(t) + ":");
                write_counter++;
                break;
            case 2:  // delete
                if (script_output) fprintf(script_output, "Deleting %lu\n", t);
                b.erase(t);
                write_counter++;
                break;
            case 3:  // query
                try {
                    std::string bval = b.query(t);
                    if (script_output)
                        fprintf(script_output, "Query %lu -> %s\n", t,
                                bval.c_str());
                } catch (std::out_of_range & e) {
                    if (script_output)
                        fprintf(script_output, "Query %lu -> DNE\n", t);
                }
                read_counter++;
                break;
            default:
                abort();
        }
    }



    std::cout << "Test PASSED" << std::endl;
    
    

    return 0;
}

void benchmark_upserts(betree<uint64_t, std::string> &b, uint64_t nops,
                       uint64_t number_of_distinct_keys, uint64_t random_seed) {
    uint64_t overall_timer = 0;
    for (uint64_t j = 0; j < 100; j++) {
        uint64_t timer = 0;
        timer_start(timer);
        for (uint64_t i = 0; i < nops / 100; i++) {
            uint64_t t = rand() % number_of_distinct_keys;
            b.update(t, std::to_string(t) + ":");
        }
        timer_stop(timer);
        printf("%ld %ld %ld\n", j, nops / 100, timer);
        overall_timer += timer;
    }
    printf("# overall: %ld %ld\n", 100 * (nops / 100), overall_timer);
}

void benchmark_queries(betree<uint64_t, std::string> &b, uint64_t nops,
                       uint64_t number_of_distinct_keys, uint64_t random_seed) {
    // Pre-load the tree with data
    srand(random_seed);
    for (uint64_t i = 0; i < nops; i++) {
        uint64_t t = rand() % number_of_distinct_keys;
        b.update(t, std::to_string(t) + ":");
    }

    // Now go back and query it
    srand(random_seed);
    uint64_t overall_timer = 0;
    timer_start(overall_timer);
    for (uint64_t i = 0; i < nops; i++) {
        uint64_t t = rand() % number_of_distinct_keys;
        b.query(t);
    }
    timer_stop(overall_timer);
    printf("# overall: %ld %ld\n", nops, overall_timer);
}


// Check if a file exists
bool fileExists(const std::string& filePath) {
    struct stat buffer;
    return (stat(filePath.c_str(), &buffer) == 0);
}

// Function to copy a file
bool copyFile(const std::string& sourcePath, const std::string& destinationPath) {
    std::ifstream sourceFile(sourcePath, std::ios::binary);
    std::ofstream destinationFile(destinationPath, std::ios::binary);

    if (!sourceFile || !destinationFile) {
        return false; // Error in opening source or destination file
    }

    destinationFile << sourceFile.rdbuf();
    return true;
}

// Function to copy all files from a source directory to a destination directory
bool copyFilesInDirectory(const std::string& sourceDir, const std::string& destDir) {
    DIR* dir = opendir(sourceDir.c_str());

    if (dir == nullptr) {
        return false; // Unable to open the source directory
    }

    struct dirent* entry;

    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_type == DT_REG) { // Check if it's a regular file
            std::string sourcePath = sourceDir + "/" + entry->d_name;
            std::string destPath = destDir + "/" + entry->d_name;

            if (!copyFile(sourcePath, destPath)) {
                closedir(dir);
                return false; // Error while copying the file
            }
        }
    }

    closedir(dir);
    return true;
}

int main(int argc, char **argv) {
    char *mode = NULL;
    uint64_t max_node_size = DEFAULT_TEST_MAX_NODE_SIZE; // the max node size of betree is setted here
    // uint64_t min_flush_size = DEFAULT_TEST_MIN_FLUSH_SIZE;
    uint64_t max_node_to_min_flush_ratio = 32;
    uint64_t min_flush_size = max_node_size / 2;
    uint64_t min_node_size = max_node_size / max_node_to_min_flush_ratio;
    uint64_t cache_size = DEFAULT_TEST_CACHE_SIZE;
    char *backing_store_dir = NULL;
    uint64_t number_of_distinct_keys = DEFAULT_TEST_NDISTINCT_KEYS;
    uint64_t nops = DEFAULT_TEST_NOPS;
    char *script_infile = NULL;
    char *script_outfile = NULL;
    char *log_file = NULL;
    unsigned int random_seed = time(NULL) * getpid();
    double epsilon = 0.5;
    int betree_state = 0;
    double write_heavy_epsilon = 0.5;
    double read_heavy_epsilon = 0.6;
    bool shorten_betree = false;

    // REQUIRED PARAMETERS FOR PERSISTENCE AND CHECKPOINTING GRANULARITY
    uint64_t persistence_granularity = UINT64_MAX;
    uint64_t checkpoint_granularity = UINT64_MAX;

    int opt;
    char *term;

    //////////////////////
    // Argument parsing //
    //////////////////////

    while ((opt = getopt(argc, argv, "m:d:N:f:C:o:k:t:s:i:p:c:l:e:a:z:w:r:S:")) != -1) {
        switch (opt) {
            case 'm':
                mode = optarg;
                break;
            case 'd':
                backing_store_dir = optarg;
                break;
            case 'N':
                max_node_size = strtoull(optarg, &term, 10);
                if (*term) {
                    std::cerr << "Argument to -N must be an integer"
                              << std::endl;
                    usage(argv[0]);
                    exit(1);
                }
                break;
            case 'f':
                min_flush_size = strtoull(optarg, &term, 10);
                if (*term) {
                    std::cerr << "Argument to -f must be an integer"
                              << std::endl;
                    usage(argv[0]);
                    exit(1);
                }
                break;
            case 'C':
                cache_size = strtoull(optarg, &term, 10);
                if (*term) {
                    std::cerr << "Argument to -C must be an integer"
                              << std::endl;
                    usage(argv[0]);
                    exit(1);
                }
                break;
            case 'o':
                script_outfile = optarg;
                break;
            case 'k':
                number_of_distinct_keys = strtoull(optarg, &term, 10);
                if (*term) {
                    std::cerr << "Argument to -k must be an integer"
                              << std::endl;
                    usage(argv[0]);
                    exit(1);
                }
                break;
            case 't':
                nops = strtoull(optarg, &term, 10);
                if (*term) {
                    std::cerr << "Argument to -t must be an integer"
                              << std::endl;
                    usage(argv[0]);
                    exit(1);
                }
                break;
            case 's':
                random_seed = strtoull(optarg, &term, 10);
                if (*term) {
                    std::cerr << "Argument to -s must be an integer"
                              << std::endl;
                    usage(argv[0]);
                    exit(1);
                }
                break;
            case 'i':
                script_infile = optarg;
                break;
            case 'p':
                // persistence granularity
                persistence_granularity = strtoull(optarg, &term, 10);
                if (*term) {
                    std::cerr << "Argument to -p must be an integer"
                              << std::endl;
                    usage(argv[0]);
                    exit(1);
                }
                break;
            case 'c':
                // checkpoint granularity
                checkpoint_granularity = strtoull(optarg, &term, 10);
                if (*term) {
                    std::cerr << "Argument to -c must be an integer"
                              << std::endl;
                    usage(argv[0]);
                    exit(1);
                }
                break;
            case 'l':
                log_file = optarg;
                break;
            case 'e':
                epsilon = strtod(optarg, &term);
                if (*term) {
                    std::cerr << "Argument to -x must be a valid double"
                              << std::endl;
                    usage(argv[0]);
                    exit(1);
                }
                break;
            case 'a':
                betree_state = std::atoi(optarg);
                break;
            case 'z':
                max_node_size = strtoull(optarg, &term, 10);
                min_node_size = max_node_size / 4;
                min_flush_size = max_node_size / max_node_to_min_flush_ratio;
                
                if (*term) {
                    std::cerr << "Argument to -z must be an integer"
                              << std::endl;
                    usage(argv[0]);
                    exit(1);
                }
                break;
            case 'w':
                write_heavy_epsilon = strtod(optarg, &term);
                if (*term) {
                    std::cerr << "Argument to -w must be a valid double"
                              << std::endl;
                    usage(argv[0]);
                    exit(1);
                }
                break;
            case 'r':
                read_heavy_epsilon = strtod(optarg, &term);
                if (*term) {
                    std::cerr << "Argument to -r must be a valid double"
                              << std::endl;
                    usage(argv[0]);
                    exit(1);
                }
                break;
            case 'S': // if shorten the betree when the state changes to read heavy mode
                if (strcmp(optarg, "true") == 0) {
                    shorten_betree = true;
                } else if (strcmp(optarg, "false") == 0) {
                    shorten_betree = false;
                } else {
                    std::cerr << "Invalid argument for -S. Use 'true' or 'false'."
                              << std::endl;
                    exit(1);
                }
                break;
            
            
            default:
                std::cerr << "Unknown option '" << (char)opt << "'"
                          << std::endl;
                usage(argv[0]);
                exit(1);
        }
    }

    // CHECK REQUIRED PARAMETERS
    if (persistence_granularity == UINT64_MAX) {
        std::cerr << "ERROR: Persistence granularity was not assigned through "
                     "-p! This is a requirement!";
        usage(argv[0]);
        exit(1);
    }
    if (checkpoint_granularity == UINT64_MAX) {
        std::cerr << "ERROR: Checkpoint granularity was not assigned through "
                     "-c! This is a requirement!";
        usage(argv[0]);
        exit(1);
    }

    FILE *script_input = NULL;
    FILE *script_output = NULL;

    if (mode == NULL ||
        (strcmp(mode, "test") != 0 && strcmp(mode, "benchmark-upserts") != 0 &&
         strcmp(mode, "benchmark-queries") != 0)) {
        std::cerr << "Must specify a mode of \"test\" or \"benchmark\""
                  << std::endl;
        usage(argv[0]);
        exit(1);
    }

    if (strncmp(mode, "benchmark", strlen("benchmark")) == 0) {
        if (script_infile) {
            std::cerr << "Cannot specify an input script in benchmark mode"
                      << std::endl;
            usage(argv[0]);
            exit(1);
        }
        if (script_outfile) {
            std::cerr << "Cannot specify an output script in benchmark mode"
                      << std::endl;
            usage(argv[0]);
            exit(1);
        }
    }

    if (script_infile) {
        script_input = fopen(script_infile, "r");
        if (script_input == NULL) {
            perror("Couldn't open input file");
            exit(1);
        }
    }

    if (script_outfile) {
        script_output = fopen(script_outfile, "w");
        if (script_output == NULL) {
            perror("Couldn't open output file");
            exit(1);
        }
    }

    srand(random_seed);

    if (backing_store_dir == NULL) {
        std::cerr << "-d <backing_store_directory> is required" << std::endl;
        usage(argv[0]);
        exit(1);
    }

    ////////////////////////////////////////////////////////
    // Construct a betree and run the tests or benchmarks //
    ////////////////////////////////////////////////////////

    one_file_per_object_backing_store ofpobs(backing_store_dir); //backing_store_dir is tmpdir in this project 

    //ofpobs.reset_ids();

    swap_space sspace(&ofpobs, cache_size);
    Logs<Op<uint64_t, std::string>> logs(persistence_granularity, checkpoint_granularity, log_file, serialization_context(sspace));
    //
    betree<uint64_t, std::string> b(&sspace, logs, epsilon, betree_state, max_node_size, min_node_size, min_flush_size);
    
    b.recovery(LOGGING_FILE_STATUS, SWAPSPACE_OBJECTS_FILE);

    
    /**
     * STUDENTS: INITIALIZE YOUR CLASS HERE
     *
     * DON'T FORGET TO MODIFY THE FUNCTION DEFINITION FOR `test` ABOVE TO ACCEPT YOUR NEW
     * CLASS.
     *
     * REMINDER: persistence_granularity AND checkpoint_granularity SHOULD
     *           BE USED IN INITIALIZATION. THESE ARE EXPOSED
     *           THROUGH COMMAND LINE ARGUMENTS FOR TESTING (-p and -c
     * respectively). If your implementation does not use these, please
     *           note why.
     *
     */

    if (strcmp(mode, "test") == 0){
        double shorten_betree_time = 0;

        uint64_t timer = 0;
        timer_start(timer);
        test(b, write_heavy_epsilon, read_heavy_epsilon, shorten_betree, shorten_betree_time, nops, number_of_distinct_keys, script_input, script_output);
        timer_stop(timer);
        double timer_in_second = timer * 1.0 / 1000000;

        std::cout << "time consumption: " << timer_in_second << " second " << std::endl;
        std::cout << "test input: " << script_infile << std::endl;
        std::cout << "cache size: " << cache_size << std::endl;
        std::cout << "if shorten Betree when workload changes to read-heavy mode: " << shorten_betree << std::endl;
        std::cout << "time cost of shortening betree(in second): " << shorten_betree_time << std::endl;

        std::cout << "betree parameter: " << std::endl;
        std::cout << "betree split counter: " << b.get_split_counter() << std::endl;
        std::cout << "epsilon: " << b.get_epsilon() << std::endl;
        std::cout << "state: " << b.get_state() << std::endl;
        std::cout << "pivot_upper_bound: " << b.get_pivot_upper_bound() << std::endl;
        std::cout << "max_node_size: " << b.get_max_node_size() << std::endl;
        std::cout << "min_flush_size: " << b.get_min_flush_size() << std::endl;
        std::cout << "min_node_size: " << b.get_min_node_size() << std::endl;

        double average_nodes_height = b.calculateAverageHeight();
        std::cout << "average betree nodes height(at the end of the test): " << average_nodes_height << std::endl;
    }
    else if (strcmp(mode, "benchmark-upserts") == 0) {
        std::cerr << "benchmark-upserts is not available for this testing program!" << std::endl;
        return 0;
        // benchmark_upserts(b, nops, number_of_distinct_keys, random_seed);
    }
        
    else if (strcmp(mode, "benchmark-queries") == 0) {
        std::cerr << "benchmark-queries is not available for this testing program!" << std::endl;
        return 0;
        // benchmark_queries(b, nops, number_of_distinct_keys, random_seed);
    }

    

    if (script_input) fclose(script_input);

    if (script_output) fclose(script_output);


    // copy all the files from sourceDir to destinationDir
    std::string sourceDir = "tmpdir_backup";  
    std::string destinationDir = "tmpdir"; 

    if (copyFilesInDirectory(sourceDir, destinationDir)) {
        std::cout << "Files copied successfully." << std::endl;
    } else {
        std::cerr << "Error copying files." << std::endl;
    }

    return 0;
}
