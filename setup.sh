#!/bin/bash
python3 get_pip.py

LOCAL_BIN_DIR="$HOME/.local/bin"

# List contents of the current user's local bin directory
ls "$LOCAL_BIN_DIR/"

# Add directory to PATH
export PATH="$PATH:$LOCAL_BIN_DIR"

# Persist the PATH change in ~/.bashrc
echo 'export PATH=$PATH:$HOME/.local/bin' >> ~/.bashrc

# Reload ~/.bashrc
source ~/.bashrc

# Install virtualenv
pip install virtualenv

# Create a virtual environment
virtualenv venv

# Activate the virtual environment
source venv/bin/activate

echo "Environment setup is complete. Virtual environment 'venv' is activated."