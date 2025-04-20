#!/usr/bin/env python3
# filepath: /home/jims/work/ssh/ssh.py

import argparse
import os
import sys
import asyncio
import asyncssh
from datetime import datetime

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
    parser.add_argument('--timestamp', action='store_true', help='Add timestamp to output filenames (default: off)')
    parser.add_argument('--add-suffixes', action='store_true', help='Add .out and .err suffixes to output files (default: off)')
    parser.add_argument('--timeout', type=int, help='Timeout in seconds for SSH operations (default: PSSHTIMEOUT env var or 10)')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug output (default: off)')
    
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
    
    # Process timeout setting with the following precedence:
    # 1. Command line argument (--timeout)
    # 2. Environment variable (PSSHTIMEOUT)
    # 3. Default value (10 seconds)
    if args.timeout is None:
        if 'PSSHTIMEOUT' in os.environ:
            try:
                args.timeout = int(os.environ['PSSHTIMEOUT'])
            except ValueError:
                parser.error(f"Environment variable PSSHTIMEOUT must be an integer, got '{os.environ['PSSHTIMEOUT']}'")
        else:
            args.timeout = 10  # Default timeout of 10 seconds
    
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

async def execute_on_host(params):
    """Execute command on a single host and save output."""
    host, username, password, command, stdout_dir, stderr_dir, timestamp, use_timestamp, use_suffixes, timeout, debug, host_num, total_hosts = params
    
    if debug:
        print(f"Connecting to {host}...")
    
    try:
        # Establish SSH connection using asyncssh
        async with asyncssh.connect(
            host=host,
            username=username,
            password=password,
            known_hosts=None,  # Don't verify host keys for simplicity
            connect_timeout=timeout
        ) as conn:
            
            # Execute command
            result = await conn.run(command, check=False, timeout=timeout)
            
            # Construct base filename
            if use_timestamp:
                base_stdout = f"{host}_{timestamp}"
                base_stderr = f"{host}_{timestamp}"
            else:
                base_stdout = f"{host}"
                base_stderr = f"{host}"
            
            # Add suffixes if needed
            if use_suffixes:
                stdout_file = os.path.join(stdout_dir, f"{base_stdout}.out")
                stderr_file = os.path.join(stderr_dir, f"{base_stderr}.err")
            else:
                stdout_file = os.path.join(stdout_dir, base_stdout)
                stderr_file = os.path.join(stderr_dir, base_stderr)
            
            # Save stdout
            with open(stdout_file, 'w') as f:
                f.write(result.stdout)
            
            # Save stderr
            with open(stderr_file, 'w') as f:
                f.write(result.stderr)
            
            success = result.exit_status == 0
            status = "✓" if success else "✗"
            print(f"[{status}] [{host_num}/{total_hosts}] {host} (exit code: {result.exit_status})")
            return host, success, False  # host, success, unreachable
            
    except (asyncssh.Error, asyncio.TimeoutError, OSError) as e:
        error_message = str(e)
        
        # Check if this is an unreachable error
        unreachable = (
            "Connection refused" in error_message or
            "timed out" in error_message.lower() or 
            "no route to host" in error_message.lower() or
            "network unreachable" in error_message.lower() or
            "could not resolve" in error_message.lower() or
            "connection failed" in error_message.lower() or
            isinstance(e, asyncio.TimeoutError)
        )
        
        # Use ? for unreachable hosts, ✗ for other errors
        status = "?" if unreachable else "✗"
        print(f"[{status}] [{host_num}/{total_hosts}] {host} - {'Unreachable' if unreachable else 'Error'}: {error_message}")
        
        # Save error to stderr file
        if use_timestamp:
            base_stderr = f"{host}_{timestamp}"
        else:
            base_stderr = f"{host}"
            
        if use_suffixes:
            stderr_file = os.path.join(stderr_dir, f"{base_stderr}.err")
        else:
            stderr_file = os.path.join(stderr_dir, base_stderr)
            
        with open(stderr_file, 'w') as f:
            f.write(f"Connection error: {str(e)}")
        
        return host, False, unreachable

async def run_parallel(hosts, args, timestamp):
    """Run commands on multiple hosts in parallel with a concurrency limit."""
    # Prepare parameters for each host
    params = [
        (host, args.username, args.password, args.command, 
         args.stdout_dir, args.stderr_dir, timestamp, args.timestamp, 
         args.add_suffixes, args.timeout, args.debug, i+1, len(hosts))
        for i, host in enumerate(hosts)
    ]
    
    # Create a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(args.parallelism)
    
    async def run_with_limit(param):
        async with semaphore:
            return await execute_on_host(param)
    
    # Create tasks for all hosts
    tasks = [run_with_limit(param) for param in params]
    
    # Wait for all tasks to complete
    return await asyncio.gather(*tasks, return_exceptions=True)

async def main_async():
    """Main async function."""
    start_time = datetime.now()
    
    args = parse_arguments()
    
    # Ensure output directories exist
    os.makedirs(args.stdout_dir, exist_ok=True)
    os.makedirs(args.stderr_dir, exist_ok=True)
    
    # Read hosts from file
    hosts = read_hosts(args.hostfile)
    if not hosts:
        print("No hosts found in the hostfile.")
        sys.exit(1)
    
    # Generate timestamp for output files (will only be used if --timestamp is set)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print(f"Starting parallel SSH execution:")
    print(f"- Hosts: {len(hosts)}")
    print(f"- Parallelism: {args.parallelism}")
    print(f"- Command: {args.command}")
    print(f"- Using timestamps: {'Yes' if args.timestamp else 'No'}")
    print(f"- Adding file suffixes: {'Yes' if args.add_suffixes else 'No'}")
    print(f"- Timeout: {args.timeout or 'None'} seconds")
    print(f"- Debug mode: {'Enabled' if args.debug else 'Disabled'}")
    print("-" * 50)
    
    # Execute in parallel
    results = await run_parallel(hosts, args, timestamp)
    
    # Process results
    valid_results = []
    for result in results:
        if isinstance(result, Exception):
            print(f"Unexpected error: {result}")
            valid_results.append((None, False, False))
        else:
            valid_results.append(result)
    
    # Collect unreachable hosts
    unreachable_hosts = [host for host, _, is_unreachable in valid_results if host and is_unreachable]
    
    # Write unreachable hosts to file if any exist
    if unreachable_hosts:
        unreachable_file = os.path.join(args.outdir, "unreachable.hosts")
        with open(unreachable_file, 'w') as f:
            for host in unreachable_hosts:
                f.write(f"{host}\n")
    
    # Calculate elapsed time
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    elapsed_seconds = elapsed_time.total_seconds()
    
    # Format elapsed time nicely
    if elapsed_seconds < 60:
        time_str = f"{elapsed_seconds:.2f} seconds"
    elif elapsed_seconds < 3600:
        minutes = int(elapsed_seconds // 60)
        seconds = elapsed_seconds % 60
        time_str = f"{minutes} minute{'s' if minutes != 1 else ''}, {seconds:.2f} seconds"
    else:
        hours = int(elapsed_seconds // 3600)
        minutes = int((elapsed_seconds % 3600) // 60)
        seconds = elapsed_seconds % 60
        time_str = f"{hours} hour{'s' if hours != 1 else ''}, {minutes} minute{'s' if minutes != 1 else ''}, {seconds:.2f} seconds"
    
    # Print summary
    successful = sum(1 for _, success, _ in valid_results if success)
    failed = sum(1 for _, success, is_unreachable in valid_results if not success and not is_unreachable)
    unreachable = len(unreachable_hosts)
    
    print("\nExecution Summary:")
    print(f"- Total hosts: {len(hosts)}")
    print(f"- Successful: {successful}")
    print(f"- Failed: {failed}")
    print(f"- Unreachable: {unreachable}")
    if unreachable > 0:
        print(f"- Unreachable hosts saved to: {os.path.abspath(os.path.join(args.stdout_dir, 'unreachable.hosts'))}")
    print(f"- Output directories:")
    print(f"  - STDOUT: {os.path.abspath(args.stdout_dir)}")
    print(f"  - STDERR: {os.path.abspath(args.stderr_dir)}")
    print(f"- Total elapsed time: {time_str}")

def main():
    """Entry point to run the async main function."""
    asyncio.run(main_async())

if __name__ == "__main__":
    main()