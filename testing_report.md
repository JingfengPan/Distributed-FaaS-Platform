# MPCS-FaaS Testing Report

## Overview
This report outlines the testing strategy and results for the MPCS-FaaS system. The testing approach covers unit testing, integration testing, and end-to-end testing across different execution modes (local, pull, and push). The task dispatcher component is thoroughly tested, including its task queue management, worker health monitoring, and fault tolerance mechanisms in both pull and push modes.

## Test Categories

### 1. Web Service Tests
#### Function Registration Tests
- **Invalid Function Registration**
  - Tests rejection of non-serialized payloads
  - Verifies proper error handling (400/500 status codes)
  - Validates error message format

- **Valid Function Registration**
  - Tests successful registration of serialized functions
  - Verifies function ID generation (UUID format)
  - Confirms function storage in Redis
  - Validates function retrieval

#### Task Execution Tests
- **Basic Task Execution**
  - Tests task creation with valid function ID
  - Verifies task ID generation
  - Validates initial task state (QUEUED)
  - Confirms task parameters storage

- **Task Status Tracking**
  - Monitors task state transitions
  - Verifies status endpoint responses
  - Validates error handling for invalid task IDs
  - Tests status update timing

- **Result Retrieval**
  - Tests result endpoint for completed tasks
  - Verifies result deserialization
  - Validates error handling for pending tasks
  - Confirms result format consistency

### 2. Dispatcher and Worker Tests

#### Pull Mode Tests
- **End-to-End Task Execution**
  - Test Setup:
    - Starts pull mode dispatcher
    - Launches single worker process
    - Registers test function (double)
    - Creates task with test parameters
  - Execution Flow:
    - Worker requests task from dispatcher
    - Dispatcher assigns task to worker
    - Worker executes task and returns result
    - Dispatcher updates task status
  - Validation:
    - Task state transitions (QUEUED → RUNNING → COMPLETE)
    - Result correctness (input * 2)
    - Execution timing (within 2 seconds)
    - Resource cleanup

- **Fault Tolerance**
  - Test Setup:
    - Starts pull mode dispatcher
    - Launches two worker processes
    - Configures long-running task (0.5s sleep)
  - Execution Flow:
    1. First worker receives task
    2. Worker process terminated
    3. Dispatcher detects worker failure
    4. Task reassigned to second worker
  - Validation:
    - Worker heartbeat monitoring
    - Task state persistence
    - Automatic task reassignment
    - Task completion by second worker
    - Resource cleanup

#### Push Mode Tests
- **End-to-End Task Execution**
  - Test Setup:
    - Starts push mode dispatcher
    - Launches single worker process
    - Registers test function
    - Creates task with parameters
  - Execution Flow:
    - Worker registers with dispatcher
    - Dispatcher pushes task to worker
    - Worker executes and reports result
    - Dispatcher updates task status
  - Validation:
    - Worker registration
    - Task delivery
    - Result reporting
    - Status updates
    - Resource cleanup

#### Task State Management Tests
- **State Transition Verification**
  - Test Setup:
    - Starts pull mode dispatcher
    - Launches single worker
    - Creates long-running task
  - Execution Flow:
    1. Initial QUEUED state
    2. Transition to RUNNING
    3. Final COMPLETE state
  - Validation:
    - State transition timing
    - State persistence
    - Result storage
    - Error handling

#### Error Handling Tests
- **Task Execution Errors**
  - Test Setup:
    - Starts pull mode dispatcher
    - Launches single worker
    - Registers error-throwing function
  - Execution Flow:
    1. Task creation
    2. Worker execution
    3. Error propagation
    4. Status update
  - Validation:
    - Error capture
    - Status update to FAILED
    - Error message preservation
    - Resource cleanup

#### Concurrent Execution Tests
- **Multiple Task Processing**
  - Test Setup:
    - Starts pull mode dispatcher
    - Launches worker with 2 processes
    - Creates 5 concurrent tasks
  - Execution Flow:
    1. Task creation and queuing
    2. Worker process assignment
    3. Parallel execution
    4. Result collection
  - Validation:
    - Concurrent execution
    - Result correctness
    - Execution timing
    - Resource utilization

## Test Infrastructure

### Test Setup and Execution

To run the tests, you need to first:

1. Start Redis:
   ```bash
   redis-server
   ```

2. Start the MPCS-FaaS service in the `service` folder:
   ```bash
   uvicorn main:app --reload
   ```

3. In the root folder, run:
   ```bash
   pytest tests
   ```

### Test Coverage

#### Critical Paths
1. Function Registration
   - Valid function registration
   - Invalid payload handling
   - Function storage and retrieval

2. Task Execution
   - Task creation and queuing
   - Worker task assignment
   - Task state management
   - Result collection and validation

3. Worker Management
   - Worker registration
   - Heartbeat monitoring
   - Worker failure detection
   - Task reassignment

4. Fault Tolerance
   - Worker failure handling
   - Task recovery
   - State consistency
   - Error propagation

5. Task State Management
   - State transitions (QUEUED → RUNNING → COMPLETE/FAILED)
   - Concurrent task handling
   - Error state handling
   - Result validation

## Test Results

### Success Metrics
- All critical paths verified
- Fault tolerance mechanisms validated
- State transitions confirmed
- Error handling verified
- Concurrent execution tested
