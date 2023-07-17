# MapReduceFramework

The MapReduceFramework is a multi-threaded programming library that implements the MapReduce model. It provides a framework for parallelizing tasks and achieving high-performance computations using multiple processors.

## Overview

The MapReduceFramework allows you to split a large task into smaller parts that can run in parallel. It handles the synchronization and communication between threads, minimizing overhead and reducing the total runtime of the computation.

The framework consists of two main components:

1. **Client**: The client code contains the implementation of the `map` and `reduce` functions specific to your task. You can customize these functions based on the task you want to perform. The client also includes key/value classes and the `emit2` and `emit3` functions to produce intermediate and output pairs.

2. **Framework**: The framework handles the partitioning of phases, distribution of work between threads, and synchronization. It provides functions such as `startMapReduceJob`, `waitForJob`, `getJobState`, and `closeJobHandle` to manage the MapReduce jobs.

## Getting Started

To use the MapReduceFramework library, follow these steps:

1. Clone the repository: `git clone https://github.com/IsraelBenDavid/MapReduce-Framework.git`

2. Build the library: Use the provided Makefile to compile the library into a static library (`libMapReduceFramework.a`).

3. Include the library: Link the library (`libMapReduceFramework.a`) in your project and include the header files `MapReduceClient.h` and `MapReduceFramework.h`.

4. Implement your client code: Create your own client code by implementing the `map` and `reduce` functions according to your task. You can use the provided `SampleClient.cpp` as a starting point.

5. Compile your client code: Build your client code and link it with the MapReduceFramework library.

6. Run your program: Execute your program and observe the parallel execution of the MapReduce job.

## Usage

To use the MapReduceFramework, you need to follow the interface defined in the `MapReduceClient.h` and `MapReduceFramework.h` header files. These files provide the necessary function prototypes and data structures for the client and framework components.

1. Define your client code: Implement the `map` and `reduce` functions in your client code based on the task you want to perform. Use the provided key/value classes and the `emit2` and `emit3` functions to produce intermediate and output pairs.

2. Start a MapReduce job: Use the `startMapReduceJob` function to start a MapReduce job with multiple threads. Provide your client implementation, input data, an empty output vector, and the desired number of worker threads.

3. Monitor the job: You can use the `getJobState` function to get the current state of a job, including the stage and progress percentage. The `waitForJob` function allows you to wait until a job is finished.

4. Access the output: After the job is completed, the output pairs will be added to the output vector. You can access the results from the output vector for further processing or analysis.

## Examples

To help you understand the usage of the MapReduceFramework, we provide a sample client implementation (`SampleClient.cpp`) that demonstrates counting character frequency in strings. You can use this as a reference or starting point for your own client code.

## Contributing

Contributions to the MapReduceFramework project are welcome! If you find any issues or have suggestions for improvements, please feel free to open an issue or submit a pull request.

When contributing, please ensure that your code follows the existing code style and conventions. Include appropriate tests and provide clear documentation for new features or changes.

## Acknowledgments

This project was developed as part of the OS course at the Hebrew University. Special thanks to the course instructors for their guidance and support.
