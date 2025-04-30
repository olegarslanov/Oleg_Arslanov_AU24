import subprocess

# Function to run pytest with specific markers
def run_pytest(marker):
    # Define the command to run pytest with the given marker
    command = [
        "pytest",
        "-m", marker,  # specify the marker (smoke, critical)
    ]

    # Run the pytest command
    subprocess.run(command)


# Main function to execute the test runs for different markers
def main():
    print("Running smoke tests...")
    run_pytest("smoke")  # Run pytest with 'smoke' marker

    print("Running critical tests...")
    run_pytest("critical")  # Run pytest with 'critical' marker

    print("Running all tests...")
    run_pytest("smoke or critical")  # Run pytest with both 'smoke' and 'critical' markers


# If the script is executed directly, call the main function
if __name__ == "__main__":
    main()

