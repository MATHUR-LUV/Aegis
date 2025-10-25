
#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define the local proto path inside the container
PROTO_DIR="./proto"

# Define the output directory for generated Python files
OUT_DIR="./generated"

# Create the output directory
mkdir -p $OUT_DIR

# Run the Python gRPC tools
python -m grpc_tools.protoc \
    --proto_path=$PROTO_DIR \
    --python_out=$OUT_DIR \
    --grpc_python_out=$OUT_DIR \
    $PROTO_DIR/agent.proto

# Create an __init__.py in the generated directory so Python treats it as a package
touch $OUT_DIR/__init__.py

