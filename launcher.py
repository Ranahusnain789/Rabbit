import os
import subprocess
import time
import signal


def start_microservice(name, service_path):
    print(f"Starting {name} microservice...")
    process = subprocess.Popen(['python', service_path],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    print(f"{name} microservice started with PID {process.pid}")
    return process


def launch_microservices():
    # List all microservices to start
    microservices = {
        "GlobalTurnClock": "global_turn_clock.py",
        "IntersectionsService": "intersections_service.py",
        "MapBuilderService": "mapbuilder.py",
        "ReportService": "report_service.py",
        "MovementService": "movement_service.py"
    }

    processes = {}

    # Start each service and store the process in a dictionary
    for name, path in microservices.items():
        if os.path.isfile(path):
            processes[name] = start_microservice(name, path)
        else:
            print(
                f"Error: {path} does not exist. {name} microservice not started."
            )

    try:
        # Monitor each process and log output; restart if it exits
        while True:
            for name, process in list(processes.items()):
                # Check process status
                retcode = process.poll()
                if retcode is not None:  # Process has stopped
                    print(
                        f"{name} microservice stopped with return code {retcode}. "
                        f"Restarting...")

                    # Capture and print stdout and stderr for troubleshooting
                    stdout, stderr = process.communicate()
                    if stdout:
                        print(f"{name} stdout:\n{stdout.decode()}")
                    if stderr:
                        print(f"{name} stderr:\n{stderr.decode()}")

                    # Restart the process
                    processes[name] = start_microservice(
                        name, microservices[name])

            # Wait before the next check
            time.sleep(5)
    except KeyboardInterrupt:
        print("Shutting down all microservices...")
        for name, process in processes.items():
            process.send_signal(
                signal.SIGINT)  # Use SIGINT for graceful shutdown
            process.wait()
        print("All microservices have been stopped.")


if __name__ == "__main__":
    launch_microservices()
