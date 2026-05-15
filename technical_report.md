# Technical Report: MPCS-FaaS Implementation

## Overview
This report describes the implementation of a Function-as-a-Service (FaaS) system that supports three execution modes: local, pull, and push. The system is designed to handle function registration, execution, and result retrieval through a REST API interface.

## Architecture

### Core Components and Implementation Files

1. **Web Service Layer**
   - `service/main.py`
     - FastAPI-based REST API implementation
     - Endpoints for function registration and execution
     - Task status and result retrieval
     - Base64 payload validation
     - Automatic API documentation
     - Redirect to API docs at root endpoint
   - `service/models.py`
     - Pydantic models for request/response validation
     - Function registration models
     - Task execution models
     - Status and result response models
     - Type safety and validation

2. **Task Dispatcher (`task_dispatcher.py`)**
   - Core task distribution logic
   - Three dispatch modes:
     - Local: Direct execution using multiprocessing
     - Pull: Worker-initiated task requests
     - Push: Dispatcher-initiated task distribution
   - Worker health monitoring
   - Task status management
   - ZMQ socket management
   - Task queue management
   - Worker failure handling
   - Task reassignment logic

3. **Worker Pool Implementation**
   - `worker_pool/pull_worker.py`
     - Pull-mode worker implementation
     - REQ-REP ZMQ pattern
     - Active task polling
     - Process pool for task execution
     - Worker registration
     - Result reporting
     - Error handling
   - `worker_pool/push_worker.py`
     - Push-mode worker implementation
     - DEALER-ROUTER ZMQ pattern
     - Passive task reception
     - Process pool for task execution
     - Worker registration
     - Result reporting
     - Error handling
   - `worker_pool/executor.py`
     - Task execution logic
     - Function deserialization
     - Parameter handling
     - Result serialization
     - Safe function execution
     - Error handling
     - Result formatting

4. **Infrastructure Layer**
   - `service/redis_client.py`
     - Redis interaction layer
     - Task status management (QUEUED, RUNNING, COMPLETED, FAILED)
     - Function storage and retrieval
     - Task creation and management
     - Worker heartbeat tracking
     - JSON serialization for data storage
     - Task status transitions
     - Worker health monitoring
     - Redis pub/sub for task distribution
   - `service/serialize.py`
     - Serialization utilities
     - Function serialization
     - Parameter serialization
     - Result serialization
     - Pickle-based serialization
     - Base64 encoding/decoding
     - Error handling

## Implementation Details

### Function Registration
- Functions are serialized using Python's pickle
- Stored in Redis with unique UUID
- Base64 validation for payload security

### Task Execution
1. **Local Mode**
   - Direct multiprocessing pool
   - No network overhead
   - Limited to single machine

2. **Pull Mode**
   - Worker-initiated task requests
   - REQ-REP pattern for reliability
   - Active polling with timeout

3. **Push Mode**
   - Dispatcher-initiated task distribution
   - DEALER-ROUTER pattern for scalability
   - Passive task reception

### Worker Management
- Heartbeat mechanism for worker health
- Automatic task reassignment on worker failure
- Process pool for parallel execution

### Fault Tolerance Techniques
- **Heartbeat Mechanism**:
  - Regular worker status updates
  - Configurable timeout period
  - Automatic worker health tracking
- **Task Reassignment**:
  - Automatic detection of failed workers
  - Task redistribution to healthy workers
  - State recovery for interrupted tasks

## Limitations and Future Improvements

### 1. Scalability and Distribution
- **Current Limitations**:
  - Process pool size is fixed at worker startup
  - Workers cannot be added/removed dynamically
- **Future Improvements**:
  - Dynamic worker pool resizing based on queue length
  - Automatic worker provisioning based on load

### 2. Security and Resource Management
- **Current Limitations**:
  - Only basic base64 validation for function payloads
  - No memory or CPU limits for function execution
  - No isolation between function executions
- **Future Improvements**:
  - Memory and CPU quotas per function
  - Rate limiting for API endpoints
  - Input validation for function parameters

### 3. Performance Optimization
- **Current Limitations**:
  - Pickle serialization adds significant overhead
  - No caching of frequently used functions
- **Future Improvements**:
  - MessagePack or Protocol Buffers for faster serialization
  - In-memory function cache for frequently used functions
  - Batch processing for multiple tasks
