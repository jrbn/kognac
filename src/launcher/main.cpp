/*
 * Copyright 2016 Jacopo Urbani
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
**/

#include <boost/chrono.hpp>

#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include <boost/program_options.hpp>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>

#include <kognac/kognac.h>
#include <kognac/compressor.h>


namespace timens = boost::chrono;
namespace logging = boost::log;
namespace fs = boost::filesystem;
namespace po = boost::program_options;

using namespace std;

void initLogging(logging::trivial::severity_level level) {
    logging::add_common_attributes();
    logging::add_console_log(std::cerr,
                             logging::keywords::format =
                                 (logging::expressions::stream << "["
                                  << logging::expressions::attr <
                                  boost::log::attributes::current_thread_id::value_type > (
                                      "ThreadID") << " "
                                  << logging::expressions::format_date_time <
                                  boost::posix_time::ptime > ("TimeStamp",
                                          "%H:%M:%S") << " - "
                                  << logging::trivial::severity << "] "
                                  << logging::expressions::smessage));
    boost::shared_ptr<logging::core> core = logging::core::get();
    core->set_filter(logging::trivial::severity >= level);
}

void printHelp(const char *programName, po::options_description & desc) {
    cout << "Usage: " << programName << " [parameters]" << endl << endl;
    cout << desc << endl;
}

void initParams(int argc, const char** argv, po::variables_map &vm,
                po::options_description &cmdline_options) {

    cmdline_options.add_options()
    ("logLevel,l", po::value<logging::trivial::severity_level>(), "Set the log level (accepted values: trace, debug, info, warning, error, fatal). Default is info.")
    ("input,i", po::value<string>(), "input. REQUIRED")
    ("help,h", "print help message")
    ("fp,f", po::value<bool>()->default_value(false), "Use FPTree to mine classes. Default is 'false'")
    ("minSupport,s", po::value<int>()->default_value(1000), "Sets the minimum support necessary to indentify class patterns. Default is '1000'")
    ("maxPatternLength,p", po::value<int>()->default_value(3), "Sets the maximum length of class patterns. Default is '3'")
    ("maxThreads", po::value<int>()->default_value(8), "Sets the maximum number of threads to use during the compression. Default is '8'")
    ("maxConcThreads", po::value<int>()->default_value(2), "Sets the number of concurrent threads that reads the raw input. Default is '2'")
    ("output,o", po::value<string>(), "output. REQUIRED")
    ("compressGraph,c", "Should I also compress the graph. If set, I create a compressed version of the triples.")
    ("sampleArg1", po::value<int>(), "Argument for the method to identify the popular terms. If the method is sample, then it represents the top k elements to extract. If it is hash or mgcs, then it indicates the number o  popular terms. REQUIRED.")
    ("sampleMethod", po::value<std::string>()->default_value("cm"), "Method to use to identify the popular terms. Can be either 'sample', 'cm', 'mgcs', 'cm_mgcs'. Default is 'cm'")
    ("sampleArg2", po::value<int>()->default_value(500), "This argument is used during the sampling procedure. It determines the sampling rate (x/10000). Default is 5%");

    po::store(po::command_line_parser(argc, argv)
              .options(cmdline_options).run(), vm);
    po::notify(vm);
}

int main(int argc, const char **argv) {
    po::variables_map vm;
    po::options_description desc("Parameters");
    initParams(argc, argv, vm, desc);

    //Init logging
    logging::trivial::severity_level level =
        vm.count("logLevel") ?
        vm["logLevel"].as<logging::trivial::severity_level>() :
        logging::trivial::info;
    initLogging(level);

    if (argc == 1 || vm.count("help") || !vm.count("input")
            || !vm.count("output") || !vm.count("sampleArg1")) {
        printHelp(argv[0], desc);
        return 0;
    }

    // Get parameters from command line
    const string input = vm["input"].as<string>();
    const string output = vm["output"].as<std::string>();
    const int parallelThreads = vm["maxThreads"].as<int>();
    const int maxConcurrentThreads = vm["maxConcThreads"].as<int>();
    const int sampleArg = vm["sampleArg1"].as<int>();
    const int sampleArg2 = vm["sampleArg2"].as<int>();
    const bool compressGraph = !vm["compressGraph"].empty();
    const int maxPatternLength = vm["maxPatternLength"].as<int>();
    const int minSupport = vm["minSupport"].as<int>();
    const bool useFP = vm["fp"].as<bool>();
    int sampleMethod = PARSE_COUNTMIN;
    if (vm["sampleMethod"].as<string>() == string("sample")) {
        sampleMethod = PARSE_SAMPLE;
    } else if (vm["sampleMethod"].as<string>() == string("cm_mgcs")) {
        sampleMethod = PARSE_COUNTMIN_MGCS;
    } else if (vm["sampleMethod"].as<string>() != string("cm")) {
        cerr << "Unrecognized option " << vm["sampleMethod"].as<string>() << endl;
        return 1;
    }

    Kognac kognac(input, output, maxPatternLength);
    BOOST_LOG_TRIVIAL(info) << "Sampling the graph ...";
    kognac.sample(sampleMethod, sampleArg, sampleArg2, parallelThreads,
                  maxConcurrentThreads);
    BOOST_LOG_TRIVIAL(info) << "Creating the dictionary mapping ...";
    kognac.compress(parallelThreads, maxConcurrentThreads, useFP, minSupport);

    if (compressGraph) {
        BOOST_LOG_TRIVIAL(info) << "Compressing the triples ...";
        kognac.compressGraph(parallelThreads, maxConcurrentThreads);
    }
    BOOST_LOG_TRIVIAL(info) << "Done.";


    return 0;
}
