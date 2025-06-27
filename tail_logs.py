import time
import glob

def follow(files):
    files = [open(f) for f in files]
    for f in files:
        f.seek(0, 2)  # Move to end of each file
    try:
        while True:
            for f in files:
                line = f.readline()
                if line:
                    print(line, end='')  # no filename prefix
            time.sleep(0.2)
    except KeyboardInterrupt:
        for f in files:
            f.close()

if __name__ == "__main__":
    log_files = glob.glob("logs/*.log")
    follow(log_files)
