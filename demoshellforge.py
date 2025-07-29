
import os
import sys
import subprocess
import threading
import time
import signal
from collections import deque
from typing import Dict, List, Optional, Tuple

class Job:
    """Represents a background job"""
    def __init__(self, job_id: int, command: str, process: subprocess.Popen):
        self.job_id = job_id
        self.command = command
        self.process = process
        self.status = "running"
        self.start_time = time.time()
        
    def is_running(self) -> bool:
        return self.process.poll() is None
        
    def get_status(self) -> str:
        if self.is_running():
            return "running"
        else:
            return f"done ({self.process.returncode})"

class UnixShell:
    """Custom Unix Shell with job scheduling and process management"""
    
    def __init__(self):
        self.jobs: Dict[int, Job] = {}
        self.job_counter = 1
        self.history = deque(maxlen=1000)
        self.aliases = {
            "ll": "ls -la",
            "la": "ls -a",
            "l": "ls -CF"
        }
        self.environment = os.environ.copy()
        self.current_dir = os.getcwd()
        
        # Handle Ctrl+C gracefully
        signal.signal(signal.SIGINT, self.signal_handler)
        
    def signal_handler(self, signum, frame):
        """Handle interrupt signals"""
        print("\n^C")
        self.prompt()
        
    def prompt(self):
        """Display shell prompt"""
        user = os.getenv("USER", "user")
        hostname = os.getenv("HOSTNAME", "localhost")
        cwd = os.getcwd().replace(os.path.expanduser("~"), "~")
        return f"{user}@{hostname}:{cwd}$ "
        
    def parse_command(self, command_line: str) -> Tuple[List[str], bool, str]:
        """Parse command line for pipes, redirection, and background execution"""
        command_line = command_line.strip()
        
        # Check for background execution
        background = command_line.endswith("&")
        if background:
            command_line = command_line[:-1].strip()
            
        # Handle output redirection
        redirect_file = None
        if ">" in command_line:
            parts = command_line.split(">")
            command_line = parts[0].strip()
            redirect_file = parts[1].strip()
            
        # Split by pipes
        commands = [cmd.strip().split() for cmd in command_line.split("|")]
        
        return commands, background, redirect_file
        
    def expand_aliases(self, command: List[str]) -> List[str]:
        """Expand shell aliases"""
        if command and command[0] in self.aliases:
            expanded = self.aliases[command[0]].split()
            return expanded + command[1:]
        return command
        
    def execute_builtin(self, command: List[str]) -> bool:
        """Execute built-in shell commands"""
        if not command:
            return True
            
        cmd = command[0]
        args = command[1:]
        
        if cmd == "cd":
            try:
                path = args[0] if args else os.path.expanduser("~")
                os.chdir(path)
                self.current_dir = os.getcwd()
                self.environment["PWD"] = self.current_dir
            except (IndexError, FileNotFoundError):
                print(f"cd: {args[0] if args else '~'}: No such file or directory")
            return True
            
        elif cmd == "pwd":
            print(os.getcwd())
            return True
            
        elif cmd == "jobs":
            self.show_jobs()
            return True
            
        elif cmd == "fg":
            self.foreground_job(args)
            return True
            
        elif cmd == "bg":
            self.background_job(args)
            return True
            
        elif cmd == "kill":
            self.kill_job(args)
            return True
            
        elif cmd == "history":
            self.show_history()
            return True
            
        elif cmd == "alias":
            self.handle_alias(args)
            return True
            
        elif cmd == "export":
            self.handle_export(args)
            return True
            
        elif cmd == "exit":
            print("Goodbye!")
            sys.exit(0)
            
        return False
        
    def execute_pipeline(self, commands: List[List[str]], background: bool = False, redirect_file: str = None):
        """Execute a pipeline of commands with pipes"""
        if len(commands) == 1:
            # Single command
            command = self.expand_aliases(commands[0])
            
            if self.execute_builtin(command):
                return
                
            try:
                # Handle redirection
                stdout = None
                if redirect_file:
                    stdout = open(redirect_file, "w")
                    
                process = subprocess.Popen(
                    command,
                    stdout=stdout or subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=self.current_dir,
                    env=self.environment
                )
                
                if background:
                    # Add to job list
                    job = Job(self.job_counter, " ".join(command), process)
                    self.jobs[self.job_counter] = job
                    print(f"[{self.job_counter}] {process.pid}")
                    self.job_counter += 1
                else:
                    # Wait for completion
                    stdout_data, stderr_data = process.communicate()
                    if stdout_data and not redirect_file:
                        print(stdout_data.decode(), end="")
                    if stderr_data:
                        print(stderr_data.decode(), end="", file=sys.stderr)
                        
                if stdout and redirect_file:
                    stdout.close()
                    
            except FileNotFoundError:
                print(f"{command[0]}: command not found")
            except Exception as e:
                print(f"Error executing command: {e}")
        else:
            # Pipeline with multiple commands
            self.execute_pipeline_chain(commands, background, redirect_file)
            
    def execute_pipeline_chain(self, commands: List[List[str]], background: bool, redirect_file: str):
        """Execute a chain of piped commands"""
        processes = []
        
        try:
            for i, command in enumerate(commands):
                command = self.expand_aliases(command)
                
                stdin = processes[i-1].stdout if i > 0 else None
                stdout = subprocess.PIPE if i < len(commands) - 1 else None
                
                # Handle final output redirection
                if i == len(commands) - 1 and redirect_file:
                    stdout = open(redirect_file, "w")
                    
                process = subprocess.Popen(
                    command,
                    stdin=stdin,
                    stdout=stdout,
                    stderr=subprocess.PIPE,
                    cwd=self.current_dir,
                    env=self.environment
                )
                processes.append(process)
                
                # Close previous stdout
                if i > 0:
                    processes[i-1].stdout.close()
                    
            if background:
                # Add pipeline as a job
                job = Job(self.job_counter, " | ".join([" ".join(cmd) for cmd in commands]), processes[-1])
                self.jobs[self.job_counter] = job
                print(f"[{self.job_counter}] {processes[-1].pid}")
                self.job_counter += 1
            else:
                # Wait for all processes
                for process in processes:
                    process.wait()
                    
                # Get final output
                if not redirect_file:
                    stdout_data, stderr_data = processes[-1].communicate()
                    if stdout_data:
                        print(stdout_data.decode(), end="")
                    if stderr_data:
                        print(stderr_data.decode(), end="", file=sys.stderr)
                        
        except Exception as e:
            print(f"Pipeline error: {e}")
            
    def show_jobs(self):
        """Display active jobs"""
        if not self.jobs:
            return
            
        for job_id, job in self.jobs.items():
            status = job.get_status()
            print(f"[{job_id}]  {status}    {job.command}")
            
        # Clean up completed jobs
        completed = [jid for jid, job in self.jobs.items() if not job.is_running()]
        for jid in completed:
            del self.jobs[jid]
            
    def foreground_job(self, args: List[str]):
        """Bring background job to foreground"""
        if not args:
            if self.jobs:
                job_id = max(self.jobs.keys())
            else:
                print("fg: no current job")
                return
        else:
            try:
                job_id = int(args[0])
            except ValueError:
                print("fg: invalid job number")
                return
                
        if job_id not in self.jobs:
            print(f"fg: job {job_id} not found")
            return
            
        job = self.jobs[job_id]
        if job.is_running():
            try:
                job.process.wait()
                del self.jobs[job_id]
            except KeyboardInterrupt:
                print("^C")
                
    def background_job(self, args: List[str]):
        """Continue stopped job in background"""
        print("bg: job control not fully implemented")
        
    def kill_job(self, args: List[str]):
        """Kill a job by job ID or PID"""
        if not args:
            print("kill: usage: kill job_id")
            return
            
        try:
            job_id = int(args[0])
            if job_id in self.jobs:
                job = self.jobs[job_id]
                job.process.terminate()
                print(f"[{job_id}] Terminated: {job.command}")
                del self.jobs[job_id]
            else:
                # Try to kill by PID
                os.kill(job_id, signal.SIGTERM)
                print(f"Process {job_id} terminated")
        except (ValueError, ProcessLookupError, PermissionError) as e:
            print(f"kill: {e}")
            
    def show_history(self):
        """Show command history"""
        for i, cmd in enumerate(self.history, 1):
            print(f"{i:4d}  {cmd}")
            
    def handle_alias(self, args: List[str]):
        """Handle alias command"""
        if not args:
            for name, command in self.aliases.items():
                print(f"alias {name}='{command}'")
        else:
            if "=" in args[0]:
                name, command = args[0].split("=", 1)
                self.aliases[name] = command.strip("'\"")
            else:
                name = args[0]
                if name in self.aliases:
                    print(f"alias {name}='{self.aliases[name]}'")
                    
    def handle_export(self, args: List[str]):
        """Handle export command"""
        for arg in args:
            if "=" in arg:
                name, value = arg.split("=", 1)
                self.environment[name] = value
                os.environ[name] = value
            else:
                if arg in self.environment:
                    print(f"{arg}={self.environment[arg]}")
                    
    def run(self):
        """Main shell loop"""
        print("Unix Shell with Job Scheduling - Type 'exit' to quit")
        
        while True:
            try:
                # Clean up completed background jobs
                completed_jobs = []
                for job_id, job in self.jobs.items():
                    if not job.is_running():
                        print(f"[{job_id}] Done: {job.command}")
                        completed_jobs.append(job_id)
                        
                for job_id in completed_jobs:
                    del self.jobs[job_id]
                    
                # Get user input
                command_line = input(self.prompt())
                
                if not command_line.strip():
                    continue
                    
                # Add to history
                self.history.append(command_line)
                
                # Parse and execute
                commands, background, redirect_file = self.parse_command(command_line)
                self.execute_pipeline(commands, background, redirect_file)
                
            except EOFError:
                print("\nGoodbye!")
                break
            except KeyboardInterrupt:
                print("")
                continue
                
if __name__ == "__main__":
    shell = UnixShell()
    shell.run()
