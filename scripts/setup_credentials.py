#!/usr/bin/env python3
"""
Script to set up AACT database credentials.

AACT provides free access to ClinicalTrials.gov data via PostgreSQL.
Register at: https://aact.ctti-clinicaltrials.org/users/sign_up

Usage:
    python scripts/setup_credentials.py
"""

import os
import sys
from pathlib import Path

def main():
    """Set up AACT credentials interactively."""
    print("=" * 60)
    print("AACT Database Credentials Setup")
    print("=" * 60)
    print()
    print("The AACT database provides free access to ClinicalTrials.gov data.")
    print("To get credentials:")
    print("  1. Visit: https://aact.ctti-clinicaltrials.org/users/sign_up")
    print("  2. Create a free account")
    print("  3. Use your username and password below")
    print()
    
    project_root = Path(__file__).parent.parent
    env_file = project_root / ".env"
    env_example = project_root / ".env.example"
    
    # Check if .env already exists
    if env_file.exists():
        print(f"Found existing .env file at: {env_file}")
        response = input("Overwrite? [y/N]: ").strip().lower()
        if response != 'y':
            print("Keeping existing .env file")
            return
    
    # Read the example file
    if env_example.exists():
        with open(env_example, 'r') as f:
            content = f.read()
    else:
        content = """# AACT Database Connection
AACT_HOST=aact-db.ctti-clinicaltrials.org
AACT_PORT=5432
AACT_DATABASE=aact
AACT_USER=your_username
AACT_PASSWORD=your_password

# Neo4j Connection
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# Data paths
DATA_RAW_PATH=./data/raw
DATA_STAGED_PATH=./data/staged
DATA_GRAPH_PATH=./data/graph

# Extraction settings
EXTRACTION_LIMIT=1000

# Logging
LOG_LEVEL=INFO
"""
    
    # Get credentials from user
    print("-" * 40)
    username = input("Enter AACT username: ").strip()
    password = input("Enter AACT password: ").strip()
    
    if not username or not password:
        print("Error: Username and password are required")
        sys.exit(1)
    
    # Replace placeholders
    content = content.replace("your_username", username)
    content = content.replace("your_password", password)
    
    # Write .env file
    with open(env_file, 'w') as f:
        f.write(content)
    
    print()
    print(f"✓ Created .env file at: {env_file}")
    print()
    print("You can now run the extraction:")
    print("  python scripts/extract_data.py --limit 1000")
    print()


if __name__ == "__main__":
    main()

