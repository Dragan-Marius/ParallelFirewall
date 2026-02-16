 🛡️ Parallel Firewall (Multi-threaded Packet Filter)

A high-performance, concurrent network packet filtering system implemented in C, utilizing the Producer-Consumer architecture and POSIX Threads.

 🚀 Overview
This project is built on top of a skeleton provided by the UPB Operating Systems course. My core contributions are the concurrency logic and synchronization mechanisms, specifically implementing the thread-safe Ring Buffer (ring_buffer.c) and the consumer thread pool architecture (consumer.c, firewall.c).
This project simulates a backend firewall that analyzes incoming network packets, validates them against allowed source IPs, and logs the `PASS` or `DROP` decisions. To maximize CPU throughput and handle large volumes of data, the system processes packets concurrently using a robust multi-threaded engine built from scratch using `pthreads`.

 🏗️ Technical Architecture

 1. Producer-Consumer Thread Pool
 Producer: A single thread that reads raw packets from an input file and enqueues them into a shared memory space.
 Consumers (Thread Pool): A dynamic pool of threads (1-32) that dequeue packets, compute their cryptographic hashes, and evaluate filtering rules simultaneously.

 2. Thread-Safe Ring Buffer
To facilitate lock-free-like data transfer between the producer and consumers, a custom Circular Buffer (`ring_buffer.c`) was engineered:
 Atomic State: Used `pthread_mutex_t` to protect read/write pointers and buffer length.
 Smart Sleep/Wake: Utilized `pthread_cond_t` (`gol` / `plin`) to gracefully suspend threads when the buffer is empty or full, preventing CPU-heavy spin-locking.

 3. Critical Section Optimization
A major focus was placed on multi-core performance in `consumer.c`. Heavy operations like `packet_hash()` and `process_packet()` are executed outside the critical section (in parallel). The mutex (`log_mutex`) is strictly locked for a fraction of a millisecond only during the `fprintf` system call, ensuring thread-safe atomic writes to the output log without creating a concurrency bottleneck.

 🧠 Implementation Scope
This project was developed upon a university-provided Operating Systems skeleton framework. My core technical contributions reside in building the concurrency engine:
 `ring_buffer.c`: Implemented the thread-safe circular queue, memory wrapping, and condition variables.
 `consumer.c`: Engineered the thread pool logic, packet extraction, and atomic file I/O operations.
 (Note: The sequential packet parsing logic, file descriptors, and `main` entry point were provided as part of the course skeleton).

 📊 Evaluation & Known Limits
 The concurrency engine successfully launches, synchronizes, and cleans up threads without memory leaks or race conditions.
 Limitation: While packet processing is parallelized, strictly ordered global timestamp logging for massive volumes of heavily out-of-order packets (requiring an intermediate Min-Heap structure before file writing) is a known limitation in this current iteration.

 💻 Compilation & Execution

 Build
```bash
make ./firewall <input-file> <output-file> <num-consumers:1-32>