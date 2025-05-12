#!/bin/bash
# bootstrap.sh
#
# This script bootstraps the MDIO installation.
# It clones the mdio-cpp-installer repository and installs MDIO
# into the current directory (in the "inst" folder).
#
# Instruction from the installer:
#   $ ./install.sh [install_directory] [mdio_tag]
# (the mdio_tag is ignored)
# The hidden --curl flag is also used.

# Define installation directory as the "inst" folder in the current directory
INST_DIR="$(pwd)/inst"

echo "Installing MDIO in: $INST_DIR"

# Clone the installer repo if it does not already exist
if [ ! -d "mdio-cpp-installer" ]; then
    echo "Cloning mdio-cpp-installer repository..."
    git clone https://github.com/BrianMichell/mdio-cpp-installer.git || { 
        echo "Failed to clone repository."; exit 1; 
    }
fi

# Change into the installer directory
cd mdio-cpp-installer || { 
    echo "Failed to change directory into mdio-cpp-installer."; exit 1; 
}

# Make sure the installer script is executable
chmod +x install.sh

# Run the installer: Pass the installation directory, a dummy tag,
# and the hidden "--curl" flag.
echo "Running MDIO installer..."
./install.sh "$INST_DIR" dummy_tag --curl || { 
    echo "MDIO installation failed."; exit 1; 
}

echo "MDIO installation completed successfully."

# Return to the original directory
cd ..

mkdir build
