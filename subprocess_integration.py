import subprocess

def process_with_cpp(raw_key: str) -> str:
    cpp_exe = r"C:\Users\ronen\CLionProjects\data_lake_cpp\cmake-build-debug\data_lake_cpp.exe"
    
    try:
        result = subprocess.run(
            [cpp_exe, raw_key],
            capture_output=True,
            text=True,
            check=True
        )

        for line in result.stdout.splitlines():
            if line.startswith("Processed key uploaded:"):
                return line.replace("Processed key uploaded: ", "").strip()
        return None
    except subprocess.CalledProcessError as e:
        print("C++ processing failed:", e.stderr)
        return None
