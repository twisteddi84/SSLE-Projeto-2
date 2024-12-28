import subprocess

# Directory and Python script to execute
directory = "/home/diogo/Desktop/SSLE/Projeto-2/Paxos"
script = "Banking_Node.py"

for _ in range(5):
    subprocess.run([
        "gnome-terminal", "--", "bash", "-c", 
        f"cd {directory} && python3 {script}; exec bash"
    ])