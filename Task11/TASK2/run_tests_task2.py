import subprocess  # import module for run system commands directly from Python


def run_tests(test_mark):
    """Func for run pytest with marker"""
    f"pytest -m {test_mark}"
    result = subprocess.run(["pytest", "-m", test_mark, "task2.py"], capture_output=True, text=True)
    print(f"Output for {test_mark} tests:\n{result.stdout}")
    print(f"Errors for {test_mark} tests:\n{result.stderr}")


def main():
    # Run only smoke tests
    print("Running smoke tests...")
    run_tests("smoke")

    # Run only critical tests
    print("Running critical tests...")
    run_tests("critical")

    # Run both types of test
    print("Running smoke and critical tests...")
    run_tests("smoke or critical")


if __name__ == "__main__":
    main()
