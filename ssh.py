#!/usr/bin/env python3
# filepath: /home/jims/work/ssh/ssh.py

import argparse
import os
import sys
import shlex
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from fabric import Connection

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Execute commands on multiple hosts in parallel via SSH')
    parser.add_argument('--hostfile', required=True, help='File containing list of hosts (one per line)')
    parser.add_argument('--password', required=True, help='SSH password')
    parser.add_argument('--outdir', help='Base directory for output (will create out/ and err/ subdirectories)')
    parser.add_argument('--stdout-dir', help='Directory to store stdout')
    parser.add_argument('--stderr-dir', help='Directory to store stderr')
    parser.add_argument('--parallelism', type=int, default=10, help='Number of parallel connections')
    parser.add_argument('--username', default=os.environ.get('USER'), help='SSH username')
    
    # Find the position of -- in arguments
    try:
        delimiter_index = sys.argv.index('--')
        # Extract arguments before --
        args = parser.parse_args(sys.argv[1:delimiter_index])
        # Extract the command after --
        args.command = ' '.join(sys.argv[delimiter_index+1:])
        if not args.command:
            parser.error("No command specified after --")
    except ValueError:
        # No -- found, show help
        parser.error("Command delimiter -- not found. Usage: script.py [options] -- command")
    
    # Handle outdir setting - if specified, derive stdout and stderr dirs
    if args.outdir:
        args.stdout_dir = args.stdout_dir or os.path.join(args.outdir, "out")
        args.stderr_dir = args.stderr_dir or os.path.join(args.outdir, "err")
    
    # Validate that we now have both output directories
    if not args.stdout_dir or not args.stderr_dir:
        parser.error("You must specify either --outdir or both --stdout-dir and --stderr-dir")
    
    return args

def read_hosts(hostfile):
    """Read hosts from a file, one per line and deduplicate while preserving order."""
    try:
        with open(hostfile, 'r') as f:
            # Use dict.fromkeys() to deduplicate while preserving order of first appearance
            hosts = list(dict.fromkeys([
                line.strip() for line in f 
                if line.strip() and not line.startswith('#')
            ]))
            
            # Print info about deduplication if duplicates were found
            original_count = len([
                line.strip() for line in open(hostfile, 'r') 
                if line.strip() and not line.startswith('#')
            ])
            if original_count > len(hosts):
                print(f"Removed {original_count - len(hosts)} duplicate hosts.")
                
            return hosts
    except FileNotFoundError:
        print(f"Error: Hostfile '{hostfile}' not found.")
        sys.exit(1)

def execute_on_host(params):
    """Execute command on a single host and save output."""
    host, username, password, command, stdout_dir, stderr_dir, timestamp = params
    
    print(f"Connecting to {host}...")
    try:
        # Establish SSH connection
        conn = Connection(
            host=host,
            user=username,
            connect_kwargs={"password": password}
        )
        
        # Execute command
        result = conn.run(command, warn=True, hide=True)
        
        # Save stdout
        stdout_file = os.path.join(stdout_dir, f"{host}_{timestamp}.out")
        with open(stdout_file, 'w') as f:
            f.write(result.stdout)
        
        # Save stderr
        stderr_file = os.path.join(stderr_dir, f"{host}_{timestamp}.err")
        with open(stderr_file, 'w') as f:
            f.write(result.stderr)
        
        success = result.return_code == 0
        status = "✓" if success else "✗"
        print(f"[{status}] {host} (exit code: {result.return_code})")
        return host, success
        
    except Exception as e:
        # Handle connection errors
        print(f"[✗] {host} - Connection error: {str(e)}")
        
        # Save error to stderr file
        stderr_file = os.path.join(stderr_dir, f"{host}_{timestamp}.err")
        with open(stderr_file, 'w') as f:
            f.write(f"Connection error: {str(e)}")
        
        return host, False

def main():
    """Main function."""
    args = parse_arguments()
    
    # Ensure output directories exist
    os.makedirs(args.stdout_dir, exist_ok=True)
    os.makedirs(args.stderr_dir, exist_ok=True)
    
    # Read hosts from file
    hosts = read_hosts(args.hostfile)
    if not hosts:
        print("No hosts found in the hostfile.")
        sys.exit(1)
    
    # Generate timestamp for output files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print(f"Starting parallel SSH execution:")
    print(f"- Hosts: {len(hosts)}")
    print(f"- Parallelism: {args.parallelism}")
    print(f"- Command: {args.command}")
    print("-" * 50)
    
    # Prepare parameters for each host
    params = [
        (host, args.username, args.password, args.command, 
         args.stdout_dir, args.stderr_dir, timestamp)
        for host in hosts
    ]
    
    # Execute in parallel
    with ThreadPoolExecutor(max_workers=args.parallelism) as executor:
        results = list(executor.map(execute_on_host, params))
    
    # Print summary
    successful = sum(1 for _, success in results if success)
    failed = len(hosts) - successful
    
    print("\nExecution Summary:")
    print(f"- Total hosts: {len(hosts)}")
    print(f"- Successful: {successful}")
    print(f"- Failed: {failed}")
    print(f"- Output directories:")
    print(f"  - STDOUT: {os.path.abspath(args.stdout_dir)}")
    print(f"  - STDERR: {os.path.abspath(args.stderr_dir)}")

if __name__ == "__main__":
    main()